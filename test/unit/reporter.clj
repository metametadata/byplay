; Prettify test reports
(ns unit.reporter
  (:require
    [clojure.test :refer [report with-test-out inc-report-counter *testing-contexts* *testing-vars*
                          testing-contexts-str testing-vars-str *stack-trace-depth*]]
    [clojure.stacktrace :as stack]))

; Change the report multimethod to ignore namespaces that don't contain any tests.
; taken from: http://blog.jayfields.com/2010/08/clojuretest-introduction.html
(defmethod report :begin-test-ns [m]
  (with-test-out
    (when (some #(:test (meta %)) (vals (ns-interns (:ns m))))
      (println "\n-------====== Testing" (ns-name (:ns m)) "======-------"))))

; Report test names
(defmethod report :begin-test-var [m]
  (with-test-out
    (println "\r\uD83C\uDF00 " (:verbose-name (meta (:var m))))))

(def ansi-reset "\u001B[0m")
(def ansi-bold "\u001B[1m")
(def ansi-red "\u001B[31m")
(def ansi-yellow "\u001B[33m")

; Error reporting with color
(def failed-tests (atom []))

(defn testing-vars-short-str
  "Returns a string representation of the current test."
  []
  (assert (= (count *testing-vars*) 1) "reporter doesn't know what to do in this case :/")
  (str
    (ns-name (:ns (meta (first *testing-vars*)))) "/"
    (:name (meta (first *testing-vars*)))))

(defn add-failed-test!
  []
  (swap! failed-tests conj (testing-vars-short-str)))

(defmethod report :fail [m]
  (add-failed-test!)

  (try
    (print ansi-red)
    (with-test-out
      (inc-report-counter :fail)
      (println "\nFAIL in" (testing-vars-str m))
      (print ansi-yellow)
      (when (seq *testing-contexts*) (println (testing-contexts-str)))
      (when-let [message (:message m)] (println message))
      (println "expected:" (pr-str (:expected m)))
      (println "  actual:" (pr-str (:actual m))))

    (finally
      (print ansi-reset))))

(defmethod report :error [m]
  (add-failed-test!)

  (try
    (print ansi-red)
    (with-test-out
      (inc-report-counter :error)
      (println "\nERROR in" (testing-vars-str m))
      (print ansi-yellow)
      (when (seq *testing-contexts*) (println (testing-contexts-str)))
      (when-let [message (:message m)] (println message))
      (println "expected:" (pr-str (:expected m)))
      (print "  actual: ")
      (let [actual (:actual m)]
        (if (instance? Throwable actual)
          (stack/print-cause-trace actual *stack-trace-depth*)
          (prn actual))))

    (finally
      (print ansi-reset))))

; Summary reporting with color
(defmethod report :summary [m]
  (try
    (print ansi-bold)
    (when (not (every? zero? [(:fail m) (:error m)]))
      (print ansi-red))

    (with-test-out
      (println "\nRan" (:test m) "tests containing"
               (+ (:pass m) (:fail m) (:error m)) "assertions.")
      (println (:fail m) "failures," (:error m) "errors.")

      ; print names of failed tests
      (print ansi-reset)
      (print ansi-red)
      (println (clojure.string/join "\n" (distinct @failed-tests)))

      ; clear a list of failed tests (important if tests are automatically rerun)
      (reset! failed-tests []))

    (finally
      (print ansi-reset))))
