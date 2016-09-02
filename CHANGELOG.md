# Changelog

## 0.4.0

- Added `:conn` into job context. It's the current `funcool/clojure.jdbc` connection.
So that `(:jdbc-conn ctx) == (jdbc.proto/connection (:conn ctx))`.
This feature can simplify your job code if you also use `funcool/clojure.jdbc` JDBC wrapper.

## 0.3.0

- Public release.