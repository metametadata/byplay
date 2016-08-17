(ns unit.shutdown
  (:require
    [byplay.core :as b]
    [clojure.test :refer :all]
    [unit.utils :refer :all]))

(use-fixtures :once with-database)

(defn ^{:byplay.core/queue :test-queue} long-job
  "Acts similarly to Thread/sleep but keeps the core really busy and is not interruptible."
  [_ctx]
  (println "  job started, press Ctrl+C!")
  (let [steps 5
        step-msec 500]
    (dotimes [i steps]
      (let [nanosec (* step-msec 1000000)
            deadline (+ nanosec (System/nanoTime))]
        (while (> deadline (System/nanoTime))))

      (println "  " (str (inc i) "/" steps)))))

(defdbtest
  [{:shutdown-test true}
   "graceful shutdown: manually press Ctrl+C while job is processed and check that it's finished before app exits (test must be run with `lein trampoline`!)"]
  (with-open [jdbc-conn (.getConnection ds)]
    (b/schedule jdbc-conn #'long-job))

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
                                 (doto worker b/interrupt b/join))))

    (doto worker b/start b/join)

    (is nil "You must press Ctrl+C while job is in progress.")))