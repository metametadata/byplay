# Byplay

Clojure background job queue on top of PostgreSQL 9.5.

It allows creating background jobs, placing those jobs on multiple queues, and processing them later.
Background jobs can be any named Clojure function.

The project is mostly inspired by [Que](https://github.com/chanks/que),
[Resque](https://github.com/resque/resque/) and [Celery](https://github.com/celery/celery).

[![Clojars Project](https://img.shields.io/clojars/v/byplay.svg)](https://clojars.org/byplay)

## Features
- **Durability**: queue can survive app restarts because it is stored inside a PostgreSQL table.
All done and failed jobs are left in a table
so that at any time user is able inspect, retry or purge them manually.
- **Embedment**: queue consumption worker can be easily started in a background thread.
- **Parallelism**: queue can be consumed by several threads on different machines to better utilize multiple CPU cores.
The parallel queue consumption is based on a new
[FOR UPDATE/SKIP LOCKED](http://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/) feature from PostgreSQL 9.5.
- **Transactional guarantees**:  
    - Every job is executed inside its own database transaction.
    - If a job is marked *done* than all its database statements have been committed.
    - In case of exception inside a job all job's database statements are rolled back 
    and a job is marked *failed*. Thus if a job is marked *new* or *failed* then 
    none of its database statements have been committed yet.
    - Scheduling can be executed in a transaction where a data needed for a job is committed.
    So that a worker will not pick up the job before the database commits.
- **Multiple queues**: jobs can be scheduled to different queues/tags.
E.g. you can schedule heavy jobs into a separate "slow" queue/worker 
in order to not block an execution of more important jobs from a "light" queue.
- **Fewer dependencies**: if you already use PostgreSQL, a separate queue (Redis, RabbitMQ, etc.) is another moving part that can break.
- **Small**: the implementation with docstrings is less than 300 LOC.

## Anti-Features
It hasn't been proven yet, but Byplay can experience the problem described in Que docs:
> Que's job table undergoes a lot of churn when it is under high load, and like any heavily-written table, 
is susceptible to bloat and slowness if Postgres isn't able to clean it up. The most common cause of this is 
long-running transactions, so it's recommended to try to keep all transactions against the database housing 
Que's job table as short as possible. This is good advice to remember for any high-activity database, 
but bears emphasizing when using tables that undergo a lot of writes.

This PostgreSQL issue is explained in more detail in the article 
["Postgres Job Queues & Failure By MVCC"](https://brandur.org/postgres-queues).

## Installation

Add dependency to your project:

```clj
[byplay "0.3.0"]
```

Require a namespace:

```clj
(ns my-app.core
  (:require
    [byplay.core :as b]
    ,,,))
```

## Quickstart

### Initialization

On your app start setup Byplay table and the accompanying indexes in the database (it's safe to call this function more than once):

```clj
(b/migrate jdbc-conn)
```

Here `jdbc-conn` is a "raw" [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) connection.
There are different ways to obtain such instance:

1) Via [funcool/clojure.jdbc](https://funcool.github.io/clojure.jdbc/latest/#creating-a-connection) JDBC wrapper:

```clj
(with-open [conn (jdbc.core/connection dbspec)]
  (let [jdbc-conn (jdbc.proto/connection conn)]
    ,,,))
```

2) Via [clojure/java.jdbc](http://clojure-doc.org/articles/ecosystem/java_jdbc/home.html#reusing-connections) JDBC wrapper:

```clj
(clojure.java.jdbc/with-db-connection [conn db-spec]
  (let [jdbc-conn (clojure.java.jdbc/db-connection conn)]
    ,,,))
```

3) Via JDBC datasource (e.g. [HikariCP](https://github.com/tomekw/hikari-cp#postgresql-example)):
 
```clj
(with-open [jdbc-conn (.getConnection datasource)]
    ,,,)
```

### Job Definition

Define a job function:

```clj
(defn my-job
  [ctx x y z]
  (do-something-in-job-transaction (:jdbc-conn ctx))
  ,,,)
```

Here `(:jdbc-conn ctx)` is a JDBC connection with the current transaction in progress.

### Scheduling

Put the job into `:default` queue:

```clj
(b/schedule jdbc-conn #'my-job 1 2 3)
```

Explicitly specify another queue using `schedule-to`:

```clj
(b/schedule-to jdbc-conn :my-queue #'my-job 1 2 3)
```

Or specify the queue in the job metadata at `:byplay.core/queue` key:

```clj
(defn ^{::b/queue :my-queue} my-job
  [ctx x y z]
  ,,,)

(b/schedule jdbc-conn #'my-job 1 2 3)
```

### Working

Define an instance of [funcool/clojure.jdbc](https://funcool.github.io/clojure.jdbc/latest/#connection-parameters) 
database specification, e.g.:

```clj
(def dbspec {:classname   "org.postgresql.Driver"
             :subprotocol "postgresql"
             :subname     "//localhost:5432/myapp"})
```

Start a background worker with 2 concurrent work threads, each polling the specified queue for a new job every 5 seconds: 

```clj
(b/start (b/new-worker dbspec {:threads-num      2
                               :queues           [:my-queue]
                               :polling-interval 5000
                               :on-fail          (fn on-fail
                                                   [worker exc {:keys [id job args queue state] :as _job}]
                                                   ,,,)}))
```

`on-fail` function will be called if exception is thrown from the job.

### Shutdown

You can ask a worker to finish all currently running jobs and stop polling a database with `interrupt` method.
For example this is how a worker can be gracefully stopped in 
[the application shutdown hook](https://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html#addShutdownHook\(java.lang.Thread\)):

```clj
(.addShutdownHook (Runtime/getRuntime)
                      (Thread. #(do
                                 ; stop the worker before other services (to not break jobs in progress)
                                 (doto worker b/interrupt b/join)
                                 
                                 ; stop other services
                                 ,,,)))
```

## Tips

### Jobs should be idempotent whenever possible.
Because in rare cases a job may be started more than once.
E.g. a worker may die in the middle of a job execution leaving this job in *new* state.

Thanks to transactional guarantees, if job only updates the database then you don't have to worry about this problem.
Just don't forget to use a connection from the job context. 

### Use database connection pool to speed things up.
See [funcool/clojure.jdbc docs](https://funcool.github.io/clojure.jdbc/latest/#connection-pool).
Otherwise Byplay will create a new connection to the database on every poll.
 
### Job signatures are important.
If you schedule a job and than rename its namespace/function than worker won't find the job var and will fail the task.
Also be careful with changing job args.

### Worker can throw exceptions in background threads.
It is possible that an exception can occur in the worker thread outside of a job function.
By default such exceptions silently kill a background thread. So it's a good practice to be ready to explicitly detect them with
[Thread/setDefaultUncaughtExceptionHandler](https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions).

## Documentation

More information can be found at [the project site](http://metametadata.github.io/byplay/):

* [API Reference](http://metametadata.github.io/byplay/api/)
* [Developer Guide](http://metametadata.github.io/byplay/dev-guide/)

## License
Copyright © 2016 Yuri Govorushchenko.

Released under an MIT license.