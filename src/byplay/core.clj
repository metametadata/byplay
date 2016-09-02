(ns byplay.core
  (:require [jdbc.core :as jdbc]
            [jdbc.proto]
            [jdbc.types]
            [ragtime.jdbc]
            [ragtime.repl]
            [ragtime.protocols :as p]))

; Database stuff
(def ^:no-doc -migrations-table "byplay_migrations")
(def ^:no-doc -migrations-dir "byplay-migrations")

(defn migrate
  "Creates Byplay's table and other stuff in the database (if it doesn't already exist)."
  [jdbc-conn]
  (ragtime.repl/migrate {:datastore  (ragtime.jdbc/sql-database {:connection jdbc-conn} {:migrations-table -migrations-table})
                         :migrations (ragtime.jdbc/load-resources -migrations-dir)
                         :reporter   (constantly nil)}))

(defn rollback
  "Removes all Byplay data from the database (if there's any)."
  [jdbc-conn]
  (let [datastore (ragtime.jdbc/sql-database {:connection jdbc-conn} {:migrations-table -migrations-table})
        migrations-num (count (p/applied-migration-ids datastore))]
    (ragtime.repl/rollback {:datastore  datastore
                            :migrations (ragtime.jdbc/load-resources -migrations-dir)
                            :reporter   (constantly nil)}
                           migrations-num))

  (jdbc/execute (jdbc.types/->connection jdbc-conn) (str "DROP TABLE IF EXISTS " -migrations-table)))

(def ^{:doc "Constant for a new job state."} job-state-new 0)
(def ^{:doc "Constant for a done job state."} job-state-done 1)
(def ^{:doc "Constant for a failed job state."} job-state-failed 2)

(defn queue-sql->clj
  "Converts the queue name from a string into a Clojure keyword."
  [q-str]
  (keyword q-str))

(defn queue-clj->sql
  "Converts the queue name from a Clojure keyword into a string for SQL. The keyword must not have a namespace."
  [q-keyword]
  {:pre [(keyword? q-keyword)
         (nil? (namespace q-keyword))]}
  (name q-keyword))

; Jobs
(defn ^:no-doc -fully-qualified-name
  [v]
  (str (ns-name (:ns (meta v))) "/" (:name (meta v))))

(defn schedule-to
  "Puts the job into the specified queue. If `nil` queue is specified then job will be scheduled to `:default` queue."
  [jdbc-conn queue job-var & args]
  (jdbc/execute (jdbc.types/->connection jdbc-conn)
                ["INSERT INTO byplay (job, args, state, queue) values (?, ?, ?, ?)"
                 (-fully-qualified-name job-var)
                 (pr-str args)
                 job-state-new
                 (queue-clj->sql (or queue :default))]))

(defn schedule
  "Puts the job into the queue specified in the job's metadata at `::queue`.
  If no queue is specified then job will be scheduled to `:default` queue."
  [jdbc-conn job-var & args]
  (let [queue (::queue (meta job-var))]
    (apply schedule-to jdbc-conn queue job-var args)))

(defn ^:no-doc -reserve-job-from-any-queue
  [conn]
  (first (jdbc/fetch conn
                     ["SELECT id, job, args
                     FROM byplay
                     WHERE state = ?
                     ORDER BY id
                     FOR UPDATE
                     SKIP LOCKED
                     LIMIT 1"
                      job-state-new])))

(defn ^:no-doc -reserve-job-from-queue
  [conn queue]
  (first (jdbc/fetch conn
                     ["SELECT id, job, args
                     FROM byplay
                     WHERE state = ? AND queue = ?
                     ORDER BY id
                     FOR UPDATE
                     SKIP LOCKED
                     LIMIT 1"
                      job-state-new
                      (queue-clj->sql queue)])))

(defn ^:no-doc -reserve-job
  [conn queues]
  (if (seq queues)
    (reduce (fn [_ q]
              (when-let [job (-reserve-job-from-queue conn q)]
                (reduced job)))
            nil
            queues)

    (-reserve-job-from-any-queue conn)))

(defn ^:no-doc -mark-job
  "Changes job state, returns updated row map."
  [conn job state]
  (-> (jdbc/fetch conn
                  ["UPDATE byplay SET state = ? WHERE id = ? RETURNING *" state (:id job)])
      first
      (update :queue queue-sql->clj)))

; Work
(defn ^:no-doc -try-with-savepoint
  "Runs the specified function passing it conn and args.
  On Throwable exception: rolls transaction to original state and returns the exception. Otherwise: returns nil."
  [f conn & args]
  (jdbc/execute conn "SAVEPOINT before")
  (try
    (apply f conn args)
    nil

    (catch Throwable e
      (jdbc/execute conn "ROLLBACK TO before")
      e)))

(defn ^:no-doc -perform-job
  [conn job]
  (let [job-var (find-var (symbol (:job job)))
        args-list (read-string (:args job))
        ctx {:jdbc-conn (jdbc.proto/connection conn)}]
    (assert job-var "job var not found")
    (apply @job-var ctx args-list)))

(defn work-once
  "Low-level API. Processes a single job from one of the queues. The order of the queues matters.
  If no queues are specified than a job from any queue will be taken.

  Returns an *ack*:

  - `nil` - if job was not found
  - job row map - if job is done, e.g. `{:id 123 :state job-state-done :job \"app.jobs/some-job\" :args \"(1 2)\" :queue :some-queue}`
  - `[exception, job row map]` - if job has failed

  Job exception is not rethrown but the job is marked *failed* instead of *done*.

  Do not call this function inside an already created transaction because `work-once` will create a new transaction itself.
  Otherwise, your outer transaction will be prematurely committed because nested transactions are not supported by PostgreSQL."
  ([jdbc-conn] (work-once jdbc-conn {:queues nil}))
  ([jdbc-conn {:keys [queues] :as _config}]
   (let [conn (jdbc.types/->connection jdbc-conn)]
     (jdbc/atomic
       conn
       (when-let [job (-reserve-job conn queues)]
         (if-let [exception (-try-with-savepoint -perform-job conn job)]
           [exception (-mark-job conn job job-state-failed)]
           (-mark-job conn job job-state-done)))))))

; Worker
(defprotocol WorkerProtocol
  (start [_] "Starts working in background threads.")
  (interrupt [_]
    "Asks the worker to gracefully finish working.
    Worker will wait for finishing of all currently executed jobs.
    You can also call `join` after `interrupt` to block your thread until worker is stopped.
    You cannot start the worker again after interruption.")
  (state [_]
    "Returns the current worker state:

     - `:new`
     - `:running`
     - `:terminated`")
  (join [_] "Blocks the current thread until the worker is stopped."))

(defrecord Worker [master-thread]
  WorkerProtocol
  (start [_] (.start master-thread))
  (interrupt [_] (.interrupt master-thread))
  (state [_]
    (condp = (.getState master-thread)
      Thread$State/NEW :new
      Thread$State/TERMINATED :terminated
      :running))
  (join [_] (.join master-thread)))

(alter-meta! #'->Worker assoc :no-doc true)
(alter-meta! #'map->Worker assoc :no-doc true)

(defn ^:no-doc -polling-thread
  "Creates a thread which will infinitely call the function with the specified interval."
  [on-poll interval-msec]
  (Thread.
    #(while (not (Thread/interrupted))
      (on-poll)

      (try
        (Thread/sleep interval-msec)
        (catch InterruptedException _
          (.interrupt (Thread/currentThread)))))))

(defn ^:no-doc -work-threads
  "Creates several working threads."
  [dbspec queues threads-num polling-interval-msec on-ack]
  (letfn [(on-poll []
            (with-open [conn (jdbc/connection dbspec)]
              (-> conn
                  jdbc.proto/connection
                  (work-once {:queues queues})
                  on-ack)))]
    (doall
      (for [_ (range threads-num)]
        (-polling-thread on-poll polling-interval-msec)))))

(defn ^:no-doc -println-err
  "As in http://yellerapp.com/posts/2014-12-11-14-race-condition-in-clojure-println.html"
  [& more]
  (.write *err* (str (clojure.string/join " " more) "\n")))

(defn new-worker
  "Creates several threads for infinitely calling [[work-once]] in them with the specified polling interval.
  Returns a [[WorkerProtocol]] instance which can be started when needed.

  `dbspec` specifies the database connection parameters and
  has the same format as in [funcool/clojure.jdbc](https://funcool.github.io/clojure.jdbc/latest/#connection-parameters).

  Config keys:

  - `:queues` - vector of queues to poll (order matters), default is `nil` which means that worker can take a job from any queue
  - `:threads-num` - number of worker threads, default is 1
  - `:polling-interval` - in msec, default is 5000
  - `:on-fail` - a function with signature `(on-fail worker exception job-row-map)` executed on exception inside a job,
   by default it prints a message into stderr
  - `:on-ack` - a function `(on-ack worker ack)` will be executed in child threads with the result of every
  [[work-once]] call. By default does nothing and is mostly useful for testing.
  If `on-fail` is specified then in case of a failed job `on-ack` will be executed after `on-fail`.
  You can `(.interrupt (Thread/currentThread))` in `on-ack` to stop working in the particular child thread."
  [dbspec config]
  (let [default-config {:queues           nil
                        :threads-num      1
                        :polling-interval 5000
                        :on-fail          (fn on-fail-default [_worker exc job]
                                            (-println-err "Job failed:" (pr-str job) "\nException:" (pr-str exc)))
                        :on-ack           (fn on-ack-default [_ _])}
        {:keys [queues threads-num polling-interval on-fail on-ack]} (merge default-config config)
        worker-atom (atom nil)
        on-ack-wrapper (fn on-ack-wrapper [ack]
                         ; notify about a failure if there's any
                         (when (vector? ack)
                           (let [[exception failed-job] ack]
                             (on-fail @worker-atom exception failed-job)))

                         ; and ack anyway
                         (on-ack @worker-atom ack))
        threads (-work-threads dbspec
                               queues
                               threads-num
                               polling-interval
                               on-ack-wrapper)
        master-thread (Thread. #(try
                                 ; wait for all threads to finish
                                 (doseq [t threads] (.start t))
                                 (doseq [t threads] (.join t))

                                 (catch InterruptedException _
                                   ; ask all child threads to stop and wait for them
                                   (doseq [t threads] (.interrupt t))
                                   (doseq [t threads] (.join t))

                                   ; simply allow master thread to exit, there's no need to re-interrupt the thread here because
                                   ; there's no code higher up on the stack that needs to know about this thread's interruption
                                   )))]
    (reset! worker-atom (->Worker master-thread))))