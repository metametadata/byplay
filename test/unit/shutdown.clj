(ns unit.shutdown
  (:require
    [byplay.core :as b]
    [clojure.test :refer :all]
    [unit.utils :refer :all]
    [unit.fixtures.jobs :as j]))

(use-fixtures :once with-database)

(defn ^{:byplay.core/queue :test-queue} long-job
  "Acts similarly to Thread/sleep but keeps the core really busy and is not interruptible."
  [_ctx steps step-msec]
  (println "  job started")
  (dotimes [i steps]
    (let [nanosec (* step-msec 1000000)
          deadline (+ nanosec (System/nanoTime))]
      (while (> deadline (System/nanoTime))))

    (println "  " (str (inc i) "/" steps))))

(defdbtest
  [{:shutdown-test true}
   "graceful shutdown: it's possible to gracefully stop the worker on app shutdown"]
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'long-job 5 500)
    (b/schedule jdbc-conn #'j/good-job))

  (let [worker (b/new-worker ds {:threads-num      1
                                 :polling-interval 1000
                                 :on-fail          (fn [_worker exc _job]
                                                     (assert nil (str "job unexpectedly failed with exception:\n" (pr-str exc))))
                                 :on-ack           (fn [worker ack]
                                                     (println "ACK:" (pr-str ack))
                                                     (when (nil? ack)
                                                       (println "  queue is empty! stopping the worker...")
                                                       (b/interrupt worker)))})]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(do
                                 (println "Interrupting a worker...")
                                 (doto worker b/interrupt b/join)

                                 ; assert
                                 (with-open [jdbc-conn (.getConnection ds)]
                                   (is (ack-done? (b/work-once jdbc-conn)) "the second job was left in the database")
                                   (is (nil? (b/work-once jdbc-conn)) "queue must be empty")))))

    (b/start worker)

    ; act
    (Thread/sleep 1000)
    (println "Exit!")
    (System/exit 0)

    (is nil "App wasn't shutdown in time.")))