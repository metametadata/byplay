(ns unit.utils
  (:require
    [byplay.core :as b]
    [jdbc.core :as jdbc]
    [jdbc.types]
    [jdbc.proto]
    [hikari-cp.core :as h]
    [clojure.string :as string]
    [clojure.test :refer [is]])
  (:import (java.io StringWriter)
           (java.sql Connection)))

; Timing
(defmacro debug-time
  "Evaluates expr and prints the time it took. Returns the value of expr."
  [description expr]
  `(do
     (print "⧗" (str ~description "... "))
     (flush)
     (let [start# (. System (nanoTime))
           ret# ~expr]
       (println (Math/ceil (/ (- (. System (nanoTime)) start#) 1000000)) "msec")
       ret#)))

(defn elapse
  "Executes the specified function and returns [function-return-value elapsed-msec]."
  [f]
  (let [start (. System (nanoTime))
        result (f)
        elapsed-msec (Math/ceil (/ (- (. System (nanoTime)) start) 1000000))]
    [result elapsed-msec]))

; Fixture setup helpers
(defn -exec-db-command
  [command]
  ; "postgres" is a default existing database
  (with-open [conn (jdbc/connection "postgresql://localhost:5432/postgres")]
    (with-open [s (.createStatement (jdbc.proto/connection conn))]
      (.executeUpdate s command))))

(def db-name "byplay_test")
(def -datasource-options {:pool-name                "test-pool"
                          :adapter                  "postgresql"
                          :leak-detection-threshold 2000
                          :database-name            db-name
                          :server-name              "localhost"
                          :port-number              5432})

(def ds nil)

(defn -reset-database
  []
  (debug-time
    "Reset test database..."
    (do
      (-exec-db-command (str "DROP DATABASE IF EXISTS " db-name))
      (-exec-db-command (str "CREATE DATABASE " db-name))))

  (println "Create connection pool...")
  (alter-var-root #'ds (fn [_] (h/make-datasource -datasource-options))))

(defn -close-pool
  []
  (println "Close connection pool...")
  (h/close-datasource ds))

(defn with-database
  "Fixture."
  [f]
  (-reset-database)
  (newline)
  (f)
  (-close-pool))

(defn -reset-aux-table
  "Create a simple empty table for testing needs."
  []
  (with-open [conn (jdbc/connection ds)]
    (jdbc/execute conn "DROP TABLE IF EXISTS aux")
    (jdbc/execute conn "CREATE TABLE aux (data TEXT NOT NULL)")))

(defn -reset-tables
  []
  (with-open [jdbc-conn (.getConnection ds)]
    (b/rollback jdbc-conn)
    (b/migrate jdbc-conn))

  (-reset-aux-table)

  ; just in case?
  (-exec-db-command (str "VACUUM ANALYZE")))

; Test macros
(defmacro -deftest-verbose
  "The same as deftest but name is defined using a string.
  Name is also added to the test metadata at :verbose-name.
  Instead of just a name you can also provide a pair: [test-metadata name].

  Inspired by: https://gist.github.com/mybuddymichael/4425558"
  [name-or-meta-name-pair & body]
  (let [meta-name-pair? (sequential? name-or-meta-name-pair)
        metadata (when meta-name-pair?
                   (first name-or-meta-name-pair))
        name (if meta-name-pair?
               (second name-or-meta-name-pair)
               name-or-meta-name-pair)
        name-symbol (-> name
                        string/lower-case
                        (string/replace #"\W" "-")
                        (string/replace #"-+" "-")
                        (string/replace #"-$" "")
                        symbol
                        (with-meta metadata))]
    `(alter-meta!
       (clojure.test/deftest ~name-symbol ~@body)
       assoc :verbose-name ~name)))

(defn -test-with-thread-exception-detection
  [f]
  (let [thread-exception-detected? (atom false)]
    (Thread/setDefaultUncaughtExceptionHandler
      (reify Thread$UncaughtExceptionHandler
        (uncaughtException [_ thread ex]
          (reset! thread-exception-detected? true)
          (locking *out* (println "\n⚡ Uncaught exception on" (.getName thread) ":" ex)))))

    (f)

    (when @thread-exception-detected?
      (is nil "Uncaught exception was detected in background thread, read logs for more info."))))

(defmacro -deftest
  "The same as -deftest-verbose but will fail the test if there was an exception in non-main thread."
  [name-string & body]
  `(-deftest-verbose ~name-string
                     (-test-with-thread-exception-detection #(do ~@body))))

(defmacro defdbtest
  "The same as -deftest but will reset `db` test database before execution."
  [name-string & body]
  `(-deftest ~name-string
     (-reset-tables)
     ~@body))

; Arg matchers
(defn -submap?
  "Checks whether m contains all entries in sub."
  [^java.util.Map m ^java.util.Map sub]
  (.containsAll (.entrySet m) (.entrySet sub)))

(defn job-context?
  [ctx]
  (instance? Connection (:jdbc-conn ctx)))

; Aux table helpers
(defn insert-aux-data!
  [jdbc-conn data]
  (jdbc/execute (jdbc.types/->connection jdbc-conn)
                ["INSERT INTO aux (data) values (?)"
                 data]))

(defn is-aux-data-empty
  [jdbc-conn]
  (is (= [] (jdbc/fetch (jdbc.types/->connection jdbc-conn)
                        "SELECT * FROM aux"))))

(defn is-aux-data-committed
  [jdbc-conn data]
  (is (= [{:data data}]
         (jdbc/fetch (jdbc.types/->connection jdbc-conn)
                     "SELECT * FROM aux"))))

; Assertion helpers
(defn ack-done?
  [ack]
  (= (:state ack) b/job-state-done))

(defn ack-failed?
  [[_exception failed-job :as _ack]]
  (= (:state failed-job) b/job-state-failed))

(defn sleep-politely
  [msec]
  (println "Please wait for" msec "msec...")
  (Thread/sleep msec))

(defmacro with-err-str
  "Catches *err* from all threads and returns it as a string."
  [& body]
  `(let [s# (new StringWriter)]
     (with-redefs [*err* s#]
       ~@body
       (str s#))))