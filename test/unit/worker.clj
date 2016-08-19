(ns unit.worker
  (:require
    [byplay.core :as b]
    [clojure.test :refer :all]
    [clj-fakes.core :as f]
    [unit.fixtures.jobs :as j]
    [unit.utils :refer :all]
    [jdbc.core :as jdbc]
    [jdbc.proto]))

(use-fixtures :once with-database)

(defn schedule-jobs
  [jobs-num job & args]
  (debug-time
    (str "Schedule " jobs-num " " job "'s")
    (with-open [conn (jdbc/connection ds)]
      (jdbc/atomic
        conn
        (dotimes [_ jobs-num]
          (apply b/schedule (jdbc.proto/connection conn) job args)))))

  ; seems that otherwise tests can sometimes take much longer
  (with-open [conn (jdbc/connection ds)]
    (jdbc/execute conn "VACUUM FULL")))

(defn start-worker
  "Starts working on several threads until queue is empty.
  On timeout worker will be asked to stop.
  Returns a tuple: worker, vector of acks, elapsed time in msec."
  ([dbspec config] (start-worker dbspec config nil))
  ([dbspec config timeout]
   (let [acks (atom [])
         worker (b/new-worker dbspec
                              (merge config {:on-fail (fn on-fail [_worker _exc _job]
                                                        (assert nil "job fail is not expected"))
                                             :on-ack  (fn on-ack [_worker ack]
                                                        (if (nil? ack)
                                                          (.interrupt (Thread/currentThread)) ; stop thread on queue exhaustion
                                                          (swap! acks conj ack)))}))
         [_ elapsed] (debug-time
                       (str "Work on " (:threads-num config) " threads")
                       (elapse #(do
                                 (when timeout
                                   (future
                                     (Thread/sleep timeout)
                                     (b/interrupt worker)))

                                 (b/start worker)
                                 (b/join worker))))]
     [worker @acks elapsed])))

(defdbtest
  "worker does not automatically start"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'j/good-job))

  (b/new-worker ds {:on-ack (fn on-ack [_worker _ack]
                              (assert nil "job must not be processed until worker is started"))})

  (sleep-politely 50))

(defdbtest
  "worker with several threads on several queues: each good job is performed strictly once"
  (let [recorded-calls (atom [])]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [_ctx n]
                               (swap! recorded-calls conj n)))

      (let [jobs-num 100
            threads-num 2
            polling-interval 1
            queues [:queue-a :queue-b :queue-c]]
        (debug-time
          (str "Schedule " jobs-num " jobs")
          (with-open [conn (jdbc/connection ds)]
            (jdbc/atomic
              conn
              (dotimes [n jobs-num]
                (b/schedule-to (jdbc.proto/connection conn) (rand-nth queues) #'j/good-job n)))))

        ; act
        (let [[_ acks _] (start-worker ds {:queues           queues
                                           :threads-num      threads-num
                                           :polling-interval polling-interval})]
          ; assert
          (is (every? ack-done? acks) "no job should fail")
          (is (= jobs-num (count acks)) "each job must be ack'ed strictly once")
          (is (= (into #{} (range jobs-num))
                 (into #{} @recorded-calls))
              "each job must be called strictly once"))))))

(defdbtest
  "worker respects the order of the specified queues"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule-to jdbc-conn :queue-a #'j/good-job 1)

    (b/schedule-to jdbc-conn :queue-b #'j/good-job 2)
    (b/schedule-to jdbc-conn :queue-b #'j/good-job 3)

    (b/schedule-to jdbc-conn :queue-c #'j/good-job 4)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 5)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 6))

  ; act & assert
  (let [[_ acks _] (start-worker ds {:queues           [:queue-c :queue-a :queue-b]
                                     :threads-num      1
                                     :polling-interval 0})]
    (is (= [{:id 4 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(4)" :state b/job-state-done}
            {:id 5 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(5)" :state b/job-state-done}
            {:id 6 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(6)" :state b/job-state-done}
            {:id 1 :queue :queue-a :job "unit.fixtures.jobs/good-job" :args "(1)" :state b/job-state-done}
            {:id 2 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(2)" :state b/job-state-done}
            {:id 3 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(3)" :state b/job-state-done}]
           acks))))

(defn test-worker-with-several-threads-finishes-faster-that-a-single-threaded-worker
  [queues]
  (let [jobs-num 200
        job-delay 10
        threads-num 4
        polling-interval 10]
    (schedule-jobs jobs-num #'j/heavy-job job-delay)

    ; act
    (let [[worker _acks elapsed] (start-worker ds {:queues           queues
                                                   :threads-num      threads-num
                                                   :polling-interval polling-interval})

          ; assert
          ideal-elapsed (/ (* jobs-num (+ job-delay polling-interval)) threads-num)
          actual-error (Math/abs (- (/ elapsed ideal-elapsed) 1)) ; percent
          max-expected-error 0.3]
      (with-open [jdbc-conn (.getConnection ds)]
        (is (nil? (b/work-once jdbc-conn)) "just in case: there must be no jobs left after stopping a worker"))
      (is (= :terminated (b/state worker)) "just in case")
      (is (< actual-error max-expected-error))
      (println "ideal-elapsed =" ideal-elapsed "elapsed =" elapsed)
      (println "Expected error =" max-expected-error "actual =" actual-error))))

(defdbtest
  "worker with several threads finishes faster than a single threaded worker"
  (test-worker-with-several-threads-finishes-faster-that-a-single-threaded-worker [:test-queue]))

(defdbtest
  "worker with several threads on all queues finishes faster than a single threaded worker"
  (test-worker-with-several-threads-finishes-faster-that-a-single-threaded-worker nil))

(defdbtest
  "worker can be gracefully stopped"
  (let [jobs-num 100
        job-delay 10
        threads-num 5
        polling-interval 10
        max-possible-elapsed (/ (* jobs-num (+ job-delay polling-interval)) threads-num)
        expected-elapsed 235]
    (assert (< expected-elapsed max-possible-elapsed) "self test")

    (schedule-jobs jobs-num #'j/heavy-job job-delay)

    ; act
    (let [worker-stopped? (atom false)
          worker (b/new-worker ds
                               {:queues           [:test-queue]
                                :threads-num      threads-num
                                :polling-interval polling-interval
                                :on-fail          (fn on-fail [_worker _exc job]
                                                    ; using (assert ...) because (is ...) may not work in a thread
                                                    (assert nil (str "stopping a worker should not fail any job but got: " (pr-str job))))
                                :on-ack           (fn on-ack [worker ack]
                                                    (assert (not (nil? ack)) "test scenario should not allow queue exhaustion")
                                                    (assert (= :running (b/state worker)) "worker must correctly report its state on ack")
                                                    (assert (not @worker-stopped?) "child thread must not work after worker is stopped"))})
          _stopper (future
                     (Thread/sleep expected-elapsed)
                     (assert (= :running (b/state worker)) "self test")
                     (b/interrupt worker))
          [_ elapsed] (debug-time
                        (str "Work on " threads-num " threads")
                        (elapse #(doto worker b/start b/join)))

          ; assert
          actual-error (- (/ elapsed expected-elapsed) 1)   ; percent
          max-expected-error 0.1]
      (is (= :terminated (b/state worker)) "worker must correctly report its state")
      (reset! worker-stopped? true)

      ; double check: wait some more time to make sure background threads really weren't working on jobs after worker had stopped
      (sleep-politely max-possible-elapsed)
      (with-open [jdbc-conn (.getConnection ds)]
        (is (ack-done? (b/work-once jdbc-conn)) "there still must be jobs left in queue"))

      (is (<= 0 actual-error max-expected-error)
          (str "worker must be stopped in time; expected-elapsed = " expected-elapsed "msec, elapsed = " elapsed " msec"))
      ;(println "expected-elapsed =" expected-elapsed "max-possible-elapsed =" max-possible-elapsed)
      (println "Expected error =" max-expected-error "actual =" actual-error))))

(defdbtest
  "worker can notify on failed jobs"
  (let [jobs-num 10
        threads-num 2
        polling-interval 0
        expected-exc-msg "expected bad-job exception"]
    (schedule-jobs jobs-num #'j/bad-job expected-exc-msg)

    ; act
    (let [fails (atom [])                                   ; vector of [exc job] pairs
          acks (atom [])
          worker-atom (atom nil)]
      (reset! worker-atom (b/new-worker ds
                                        {:queues           [:test-queue]
                                         :threads-num      threads-num
                                         :polling-interval polling-interval
                                         :on-fail          (fn on-fail [worker exc job]
                                                             (assert (= worker @worker-atom) "worker must be passed into a callback`")
                                                             (swap! fails conj [exc job]))
                                         :on-ack           (fn on-ack [_worker ack]
                                                             (if (nil? ack)
                                                               (.interrupt (Thread/currentThread)) ; stop thread on queue exhaustion
                                                               (swap! acks conj ack)))}))
      (debug-time
        (str "Work on " threads-num " threads")
        (doto @worker-atom b/start b/join))

      ; assert
      (is (= :terminated (b/state @worker-atom)) "worker must correctly report its state")

      (is (= jobs-num (count @fails)))
      (is (every? (fn expected-fail?
                    [[exc job]]
                    (and (= expected-exc-msg (.getMessage exc))
                         (ack-failed? [exc job])))
                  @fails))

      (is (= (into #{} @fails) (into #{} @acks)) "all acks must still be delivered too"))))

(defdbtest
  "worker: on-ack will be called after on-fail"
  (f/with-fakes
    (let [on-fail (f/recorded-fake)
          on-ack (f/recorded-fake [[f/any? f/any?] (fn [_worker _ack]
                                                     (.interrupt (Thread/currentThread)))])]
      (with-open [jdbc-conn (.getConnection ds)]
        (b/schedule jdbc-conn #'j/bad-job "expected exception"))

      ; act
      (doto (b/new-worker ds {:queues  [:test-queue]
                              :on-fail on-fail
                              :on-ack  on-ack})
        b/start
        b/join)

      ; assert
      (is (f/were-called-in-order on-fail f/any?
                                  on-ack f/any?))
      (is (= 2 (count (f/calls)))))))

(defdbtest
  "default worker exception handler prints to stderr"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'j/bad-job "expected exception"))

  ; act
  (let [worker (b/new-worker ds {:queues [:test-queue]})
        err-str (with-err-str (b/start worker)
                              (sleep-politely 100)
                              (b/interrupt worker))]
    ; assert
    (is (re-find #"^Job failed: \{:id 1, :job \"unit\.fixtures\.jobs/bad-job\".*\nException"
                 err-str))))

(defn test-worker-processes-all-jobs-in-order-of-arrival
  [config]
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule-to jdbc-conn :queue-a #'j/good-job 1)

    (b/schedule-to jdbc-conn :queue-b #'j/good-job 2)
    (b/schedule-to jdbc-conn :queue-b #'j/good-job 3)

    (b/schedule-to jdbc-conn :queue-c #'j/good-job 4)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 5)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 6))

  ; act & assert
  (let [[_ acks _] (start-worker ds config)]
    (is (= [{:id 1 :queue :queue-a :job "unit.fixtures.jobs/good-job" :args "(1)" :state b/job-state-done}
            {:id 2 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(2)" :state b/job-state-done}
            {:id 3 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(3)" :state b/job-state-done}
            {:id 4 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(4)" :state b/job-state-done}
            {:id 5 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(5)" :state b/job-state-done}
            {:id 6 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(6)" :state b/job-state-done}]
           acks))))

(defdbtest
  "with no queues specified worker processes all jobs in the order of their arrival"
  (test-worker-processes-all-jobs-in-order-of-arrival {:threads-num      1
                                                       :polling-interval 0}))

(defdbtest
  "with nil queues specified worker processes all jobs in the order of their arrival"
  (test-worker-processes-all-jobs-in-order-of-arrival {:queues           nil
                                                       :threads-num      1
                                                       :polling-interval 0}))

(defdbtest
  "worker can accept a db-spec map"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'j/good-job))

  ; act
  (let [[_ acks _] (start-worker {:vendor "postgresql"      ; as in https://funcool.github.io/clojure.jdbc/latest/#connection-parameters
                                  :name   db-name}
                                 {:polling-interval 0})]
    (is (= [{:id 1 :queue :test-queue :job "unit.fixtures.jobs/good-job" :args "nil" :state b/job-state-done}]
           acks))))