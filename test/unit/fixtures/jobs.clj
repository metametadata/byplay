(ns unit.fixtures.jobs)

(defn ^{:byplay.core/queue :test-queue} good-job
  [_ctx & _args])

(defn ^{:byplay.core/queue :test-queue} bad-job
  [_ctx exc-msg]
  (throw (Exception. exc-msg)))

(defn ^{:byplay.core/queue :test-queue} another-job
  [_ctx & _args])

(defn ^{:byplay.core/queue :test-queue} heavy-job
  "Acts similarly to Thread/sleep but keeps the core really busy and is not interruptible."
  [_ctx msec]
  (let [nanosec (* msec 1000000)
        deadline (+ nanosec (System/nanoTime))]
    (while (> deadline (System/nanoTime)))))

(defn job-without-queue
  [_ctx])

(defn ^{:byplay.core/queue :some-ns/queue} job-with-namespaced-queue
  [_ctx])