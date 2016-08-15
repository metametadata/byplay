(ns unit.fixtures.jobs)

(defn ^{:byplay.core/queue :test-queue} good-job
  [_ctx & _args])

(defn ^{:byplay.core/queue :test-queue} bad-job
  [_ctx exc-msg]
  (throw (Exception. exc-msg)))

(defn ^{:byplay.core/queue :test-queue} another-job
  [_ctx & _args])

(defn block
  "Similar to Thread/sleep but keeps the core really busy."
  [msec]
  (let [nanosec (* msec 1000000)
        deadline (+ nanosec (System/nanoTime))]
    (while (> deadline (System/nanoTime)))))

(defn ^{:byplay.core/queue :test-queue} blocking-job
  [_ctx msec]
  (block msec))

(defn job-without-queue
  [_ctx])

(defn ^{:byplay.core/queue :some-ns/queue} job-with-namespaced-queue
  [_ctx])