(ns unit.core
  (:require
    [byplay.core :as b]
    [clojure.test :refer :all]
    [clj-fakes.core :as f]
    [unit.fixtures.jobs :as j]
    [unit.utils :refer :all]
    [jdbc.core :as jdbc]
    [jdbc.proto]
    [jdbc.types]))

(use-fixtures :once with-database)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; migrations ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defdbtest
  "it's OK to migrate more than once"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/migrate jdbc-conn)
    (b/migrate jdbc-conn)))

(defdbtest
  "it's OK to rollback more than once"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/rollback jdbc-conn)
    (b/rollback jdbc-conn)))

(defdbtest
  "rollback removes migrations table too"
  (with-open [jdbc-conn (.getConnection ds)]
    (let [migrations-table-exists? (fn []
                                     (-> (jdbc.types/->connection jdbc-conn)
                                         (jdbc/fetch "SELECT to_regclass('byplay_migrations')")
                                         first :to_regclass nil? not))]
      (is (migrations-table-exists?) "self test")

      ; act
      (b/rollback jdbc-conn)

      ; assert
      (is (not (migrations-table-exists?))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; single queue ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defdbtest
  "work-once on the specified queue returns the done job"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (b/schedule jdbc-conn #'j/good-job 1 2)

      ; act
      (is (= {:state b/job-state-done :id 1 :job "unit.fixtures.jobs/good-job" :args "(1 2)" :queue :test-queue}
             (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is (f/was-called-once j/good-job [(f/arg job-context?) 1 2])))))

(defdbtest
  "work-once on the empty queue returns nil"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (b/schedule jdbc-conn #'j/good-job 1 2)

      ; act & assert
      (is (nil? (b/work-once jdbc-conn {:queues [:empty-queue]})))
      (is (f/was-not-called j/good-job)))))

(defdbtest
  "scheduled job can be processed only once"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (b/schedule jdbc-conn #'j/good-job 1 2)
      (b/work-once jdbc-conn {:queues [:test-queue]})

      ; act & assert
      (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})) "there should be no jobs left")
      (is (f/was-called-once j/good-job [(f/arg job-context?) 1 2])))))

(defdbtest
  "schedule and process three jobs off a single queue"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (f/patch! #'j/another-job (f/recorded-fake))

      (b/schedule jdbc-conn #'j/good-job 1 2)
      (b/schedule jdbc-conn #'j/good-job 3 4)
      (b/schedule jdbc-conn #'j/another-job 5 6 "7")

      ; act
      (is (= {:id 1 :job "unit.fixtures.jobs/good-job" :args "(1 2)" :state b/job-state-done :queue :test-queue}
             (b/work-once jdbc-conn {:queues [:test-queue]})))
      (is (= {:id 2 :job "unit.fixtures.jobs/good-job" :args "(3 4)" :state b/job-state-done :queue :test-queue}
             (b/work-once jdbc-conn {:queues [:test-queue]})))
      (is (= {:id 3 :job "unit.fixtures.jobs/another-job" :args "(5 6 \"7\")" :state b/job-state-done :queue :test-queue}
             (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is (f/were-called-in-order
            j/good-job [(f/arg job-context?) 1 2]
            j/good-job [(f/arg job-context?) 3 4]
            j/another-job [(f/arg job-context?) 5 6 "7"]))
      (is (= 3 (count (f/calls)))))))

(defdbtest
  "on success job's transaction is committed"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [ctx data]
                               (insert-aux-data! (:jdbc-conn ctx) data)))
      (b/schedule jdbc-conn #'j/good-job "expected data")

      ; act
      (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is-aux-data-committed jdbc-conn "expected data"))))

(defdbtest
  "on exception job is marked failed and is returned in a pair with the exception"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (let [expected-exception (Exception. "expected exception")]
        (f/patch! #'j/another-job (fn [_ _]
                                    (throw expected-exception)))
        (b/schedule jdbc-conn #'j/another-job :_data)

        ; act and assert
        (is (= [expected-exception {:state b/job-state-failed
                                    :id    1
                                    :job   "unit.fixtures.jobs/another-job"
                                    :args  "(:_data)"
                                    :queue :test-queue}]
               (b/work-once jdbc-conn {:queues [:test-queue]})))))))

(defdbtest
  "on exception job's transaction is rolled back"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/another-job (fn [ctx data]
                                  (insert-aux-data! (:jdbc-conn ctx) data)
                                  (throw (ex-info "expected" {}))))
      (b/schedule jdbc-conn #'j/another-job "some data")

      ; act
      (is (ack-failed? (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is-aux-data-empty jdbc-conn))))

(defdbtest
  "failed job is not executed again"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/another-job (fn [_ _]
                                  (throw (ex-info "expected" {}))))
      (b/schedule jdbc-conn #'j/another-job :_data)
      (b/work-once jdbc-conn {:queues [:test-queue]})

      ; act and assert
      (f/patch! #'j/another-job (f/recorded-fake))
      (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})))
      (is (f/was-not-called j/another-job)))))

(defdbtest
  "on assertion error job's transaction is rolled back"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/another-job (fn [ctx data]
                                  (insert-aux-data! (:jdbc-conn ctx) data)
                                  (assert nil "expected")))
      (b/schedule jdbc-conn #'j/another-job "some data")

      ; act
      (is (ack-failed? (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is-aux-data-empty jdbc-conn))))

(defdbtest
  "job can be scheduled from another job"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (f/patch! #'j/another-job (fn [ctx _data]
                                  (b/schedule (:jdbc-conn ctx) #'j/good-job 1 2)))

      (b/schedule jdbc-conn #'j/another-job :_data)
      (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})) "self test")

      ; act & assert
      (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})))
      (is (f/was-called-once j/good-job [(f/arg job-context?) 1 2])))))

(defdbtest
  "job scheduling from another job is rolled back on job error"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (f/patch! #'j/another-job (fn [ctx _data]
                                  (b/schedule (:jdbc-conn ctx) #'j/good-job 1 2)
                                  (throw (Exception. "expected exception"))))

      (b/schedule jdbc-conn #'j/another-job :_data)
      (is (ack-failed? (b/work-once jdbc-conn {:queues [:test-queue]})) "self test")

      ; act & assert
      (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})))
      (is (f/was-not-called j/good-job)))))

(defdbtest
  "job can schedule itself"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake [f/any?
                                               (fn [ctx _data]
                                                 (b/schedule (:jdbc-conn ctx) #'j/good-job :_rescheduled-data))]))

      (b/schedule jdbc-conn #'j/good-job :_first-data)
      (b/work-once jdbc-conn {:queues [:test-queue]})

      ; act
      (b/work-once jdbc-conn {:queues [:test-queue]})

      ; assert
      (is (f/were-called-in-order
            j/good-job [(f/arg job-context?) :_first-data]
            j/good-job [(f/arg job-context?) :_rescheduled-data])))))

(defdbtest
  "job cannot be scheduled to a namespaced queue"
  (with-open [jdbc-conn (.getConnection ds)]
    (is (thrown-with-msg?
          java.lang.AssertionError
          #"Assert failed: \(nil\? \(namespace"
          (b/schedule-to jdbc-conn :some-ns/queue #'j/good-job)))))

(defdbtest
  "job with a namespaced queue cannot be scheduled"
  (with-open [jdbc-conn (.getConnection ds)]
    (is (thrown-with-msg?
          java.lang.AssertionError
          #"Assert failed: \(nil\? \(namespace"
          (b/schedule jdbc-conn #'j/job-with-namespaced-queue)))))

(def ^{::b/queue :test-queue} ifn-job
  (reify clojure.lang.IFn
    (applyTo
      [_ [ctx data]]
      (insert-aux-data! (:jdbc-conn ctx) data))))

(defdbtest
  "reified IFn can be used as a job"
  (with-open [jdbc-conn (.getConnection ds)]
    (let [expected-aux-data "ifn-job data"]
      ; act
      (b/schedule jdbc-conn #'ifn-job expected-aux-data)

      ; assert
      (is (ack-done? (b/work-once jdbc-conn)))
      (is-aux-data-committed jdbc-conn expected-aux-data))))

(defdbtest
  "funcool/clojure.jdbc connection :conn can also be used inside the job"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [ctx data]
                               (is (= (:jdbc-conn ctx) (jdbc.proto/connection (:conn ctx)))
                                   "raw connection must be wrapped by the high-level connection instance")
                               (insert-aux-data! (jdbc.proto/connection (:conn ctx)) data)))
      (b/schedule jdbc-conn #'j/good-job "expected data")

      ; act
      (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})))

      ; assert
      (is-aux-data-committed jdbc-conn "expected data"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; several queues ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defdbtest
  "schedule and independently process three jobs off different queues"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (f/recorded-fake))
      (f/patch! #'j/another-job (f/recorded-fake))

      (b/schedule-to jdbc-conn :queue1 #'j/good-job 1 2)
      (b/schedule-to jdbc-conn :queue2 #'j/good-job 3 4)
      (b/schedule-to jdbc-conn :queue3 #'j/another-job 5 6 "7")

      ; act & assert
      (is (= {:id 1 :job "unit.fixtures.jobs/good-job" :args "(1 2)" :state b/job-state-done :queue :queue1}
             (b/work-once jdbc-conn {:queues [:queue1]})))
      (is (f/was-called-once j/good-job [(f/arg job-context?) 1 2]))

      (is (= {:id 2 :job "unit.fixtures.jobs/good-job" :args "(3 4)" :state b/job-state-done :queue :queue2}
             (b/work-once jdbc-conn {:queues [:queue2]})))
      (is (f/was-called j/good-job [(f/arg job-context?) 3 4]))

      (is (= {:id 3 :job "unit.fixtures.jobs/another-job" :args "(5 6 \"7\")" :state b/job-state-done :queue :queue3}
             (b/work-once jdbc-conn {:queues [:queue3]})))
      (is (f/was-called-once j/another-job [(f/arg job-context?) 5 6 "7"])))))

(defdbtest
  "work-once can work on several queues, order of queues matters"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule-to jdbc-conn :queue-a #'j/good-job 1)

    (b/schedule-to jdbc-conn :queue-b #'j/good-job 2)
    (b/schedule-to jdbc-conn :queue-b #'j/good-job 3)

    (b/schedule-to jdbc-conn :queue-c #'j/good-job 4)
    (b/schedule-to jdbc-conn :queue-c #'j/bad-job 5)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 6)

    ; act & assert
    (let [work #(b/work-once jdbc-conn {:queues [:queue-c :queue-a :queue-b]})]
      (is (= {:id 4 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(4)" :state b/job-state-done}
             (work)))
      (is (= {:id 5 :queue :queue-c :job "unit.fixtures.jobs/bad-job" :args "(5)" :state b/job-state-failed}
             (second (work))))
      (is (= {:id 6 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(6)" :state b/job-state-done}
             (work)))

      (is (= {:id 1 :queue :queue-a :job "unit.fixtures.jobs/good-job" :args "(1)" :state b/job-state-done}
             (work)))

      (is (= {:id 2 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(2)" :state b/job-state-done}
             (work)))
      (is (= {:id 3 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(3)" :state b/job-state-done}
             (work)))

      (is (= nil (work))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; default queues ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn test-work-once-works-on-all-queues
  [& work-once-args]
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule-to jdbc-conn :queue-a #'j/good-job 1)

    (b/schedule-to jdbc-conn :queue-b #'j/good-job 2)
    (b/schedule-to jdbc-conn :queue-b #'j/good-job 3)

    (b/schedule-to jdbc-conn :queue-c #'j/good-job 4)
    (b/schedule-to jdbc-conn :queue-c #'j/bad-job 5)
    (b/schedule-to jdbc-conn :queue-c #'j/good-job 6)

    ; act & assert
    (let [work #(apply b/work-once jdbc-conn work-once-args)]
      (is (= {:id 1 :queue :queue-a :job "unit.fixtures.jobs/good-job" :args "(1)" :state b/job-state-done}
             (work)))

      (is (= {:id 2 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(2)" :state b/job-state-done}
             (work)))

      (is (= {:id 3 :queue :queue-b :job "unit.fixtures.jobs/good-job" :args "(3)" :state b/job-state-done}
             (work)))

      (is (= {:id 4 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(4)" :state b/job-state-done}
             (work)))
      (is (= {:id 5 :queue :queue-c :job "unit.fixtures.jobs/bad-job" :args "(5)" :state b/job-state-failed}
             (second (work))))
      (is (= {:id 6 :queue :queue-c :job "unit.fixtures.jobs/good-job" :args "(6)" :state b/job-state-done}
             (work)))

      (is (= nil (work))))))

(defdbtest
  "work-once without queues specified works on all queues"
  (test-work-once-works-on-all-queues))

(defdbtest
  "work-once with nil queues works on all queues"
  (test-work-once-works-on-all-queues nil))

(defdbtest
  "work-once with [] queues works on all queues"
  (test-work-once-works-on-all-queues []))

(defdbtest
  "job is scheduled to :default queue by default"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'j/job-without-queue)

    (is (= {:state b/job-state-done :id 1 :job "unit.fixtures.jobs/job-without-queue" :args "nil" :queue :default}
           (b/work-once jdbc-conn)))))

(defdbtest
  "scheduling to nil queue is the same as scheduling to a default queue"
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule-to jdbc-conn nil #'j/job-without-queue)

    (is (= {:state b/job-state-done :id 1 :job "unit.fixtures.jobs/job-without-queue" :args "nil" :queue :default}
           (b/work-once jdbc-conn)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; transactions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defdbtest
  "scheduling can be rolled back by an outer transaction"
  ; act
  (with-open [jdbc-conn (.getConnection ds)]
    (let [conn (jdbc.types/->connection jdbc-conn)]
      (jdbc/atomic
        conn
        (b/schedule jdbc-conn #'j/good-job)
        (jdbc/set-rollback! conn)))

    ; assert
    (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})))))

(defdbtest
  "successful work will prematurely commit an \"outer transaction\" (using jdbc.core/atomic)"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (b/schedule jdbc-conn #'j/good-job)

      (let [conn (jdbc.types/->connection jdbc-conn)]
        (jdbc/atomic
          conn
          (insert-aux-data! jdbc-conn "expected data")

          (with-open [jdbc-conn (.getConnection ds)]
            (is-aux-data-empty jdbc-conn))

          (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})) "self test")

          (with-open [jdbc-conn (.getConnection ds)]
            (is-aux-data-committed jdbc-conn "expected data")))))))

(defdbtest
  "successful work will prematurely commit an \"outer transaction\" (using raw BEGIN..ROLLBACK)"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (b/schedule jdbc-conn #'j/good-job)

      ; act
      (let [conn (jdbc.types/->connection jdbc-conn)]
        (try
          (.setAutoCommit jdbc-conn false)                  ; see https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#disable_auto_commit
          (jdbc/execute conn "BEGIN")

          (insert-aux-data! jdbc-conn "expected data")

          (with-open [jdbc-conn (.getConnection ds)]
            (is-aux-data-empty jdbc-conn))

          (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})) "self test")

          (with-open [jdbc-conn (.getConnection ds)]
            (is-aux-data-committed jdbc-conn "expected data")))))))

(defdbtest
  "successful work cannot be rolled back by an \"outer transaction\" (using jdbc.core/atomic)"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [ctx data]
                               (insert-aux-data! (:jdbc-conn ctx) data)))
      (b/schedule jdbc-conn #'j/good-job "expected data")

      ; act
      (let [conn (jdbc.types/->connection jdbc-conn)]
        (jdbc/atomic
          conn
          (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})))

          ; assert
          (is-aux-data-committed jdbc-conn "expected data")
          (jdbc/set-rollback! conn)))))

  ; just in case create a new connection
  (with-open [jdbc-conn (.getConnection ds)]
    (is-aux-data-committed jdbc-conn "expected data")
    (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})))))

(defdbtest
  "successful work cannot be rolled back by an \"outer transaction\" (using raw BEGIN..ROLLBACK)"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [ctx data]
                               (insert-aux-data! (:jdbc-conn ctx) data)))
      (b/schedule jdbc-conn #'j/good-job "expected data")

      ; act
      (let [conn (jdbc.types/->connection jdbc-conn)]
        (try
          (.setAutoCommit jdbc-conn false)                  ; see https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html#disable_auto_commit
          (jdbc/execute conn "BEGIN")
          (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]})))
          (insert-aux-data! jdbc-conn "unexpected data")

          (finally
            (jdbc/execute conn "ROLLBACK"))))))

  ; assert
  ; just in case create a new connection
  (with-open [jdbc-conn (.getConnection ds)]
    (is-aux-data-committed jdbc-conn "expected data")
    (is (nil? (b/work-once jdbc-conn {:queues [:test-queue]})))))

(defdbtest
  "connection in job context is not autocommitable"
  (with-open [jdbc-conn (.getConnection ds)]
    (f/with-fakes
      (f/patch! #'j/good-job (fn [ctx]
                               ; assert
                               (is (false? (.getAutoCommit (jdbc.proto/connection (:jdbc-conn ctx)))))))
      (b/schedule jdbc-conn #'j/good-job)

      ; act
      (is (ack-done? (b/work-once jdbc-conn {:queues [:test-queue]}))))))