; Benchmarking tests which should detect incorrectly created indexes
(ns unit.indexes
  (:require
    [unit.worker]
    [byplay.core :as b]
    [clojure.test :refer :all]
    [unit.fixtures.jobs :as j]
    [unit.utils :refer :all]))

(use-fixtures :once with-database)

(defn test-worker-finishes-in-reasonable-time
  [queues]
  (let [jobs-num 25000
        threads-num 4
        ideal-elapsed (/ jobs-num threads-num)
        max-expected-error 0.3                              ; percent
        early-fail-timeout (* 2 ideal-elapsed)]
    (unit.worker/schedule-jobs jobs-num #'j/good-job)

    ; act
    (let [[_worker _acks elapsed] (unit.worker/start-worker ds {:queues           queues
                                                                :threads-num      threads-num
                                                                :polling-interval 0}
                                                            early-fail-timeout)

          ; assert
          actual-error (Math/abs (- (/ elapsed ideal-elapsed) 1))]
      (with-open [jdbc-conn (.getConnection ds)]
        (is (nil? (b/work-once jdbc-conn)) "there must be no jobs left after stopping a worker"))

      (is (< actual-error max-expected-error))
      (println "ideal-elapsed =" ideal-elapsed "elapsed =" elapsed)
      (println "Expected error =" max-expected-error "actual =" actual-error))))

(defdbtest
  "benchmark working on a specific queue"
  (test-worker-finishes-in-reasonable-time [:test-queue]))

(defdbtest
  "benchmark working on all queues"
  (test-worker-finishes-in-reasonable-time nil))