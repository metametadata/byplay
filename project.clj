(defproject
  byplay "0.3.0"
  :description "Clojure background job queue on top of PostgreSQL 9.5."
  :url "https://github.com/metametadata/byplay"
  :license {:name "MIT" :url "http://opensource.org/licenses/MIT"}

  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]

                 [funcool/clojure.jdbc "0.9.0"]
                 [org.postgresql/postgresql "9.4.1209"]

                 [ragtime "0.6.3"]]

  :profiles {:test {:dependencies [[clj-fakes "0.7.0"]

                                   [hikari-cp "1.7.3" :exclusions [org.slf4j/slf4j-api]]

                                   [org.clojure/tools.logging "0.3.1"]
                                   [ch.qos.logback/logback-classic "1.1.7"]]}}

  :plugins [[com.jakemccrary/lein-test-refresh "0.15.0"]
            [lein-codox "0.9.5"]]

  :pedantic? :abort

  :source-paths ["src" "test"]

  :repositories {"clojars" {:sign-releases false}}

  :test-refresh {:notify-command ["terminal-notifier" "-title" "Tests" "-message"]}

  :test-selectors {:default       (complement :shutdown-test)
                   :shutdown-test :shutdown-test}

  :codox {:source-uri   "https://github.com/metametadata/byplay/blob/master/{filepath}#L{line}"
          :language     :clojure
          :source-paths ["src"]
          :output-path  "site/api"
          :metadata     {:doc/format :markdown}})