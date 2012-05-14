(ns clj-sql-mapper.test.mapper
  (:require [clj-sql-mapper.mapper :as mpr])
  (:use clojure.test))

(deftest mappings
  (let [mappings [(mpr/make-mappings :a inc dec)
                  (mpr/make-mappings [:b :c] (partial + 2) (partial - 2))]]
    (is (= {:a 2 :b 4 :c 5}) (mpr/apply-mappings (map first mappings) {:a 1 :b 2 :c 3}))
    (is (= {:a 0 :b 0 :c 1}) (mpr/apply-mappings (map second mappings) {:a 1 :b 2 :c 3}))
    (is (= '({:a 2 :b 4 :c 5} {:a 12 :b 24 :c 35})) (mpr/apply-mappings (map first mappings) [{:a 1 :b 2 :c 3} {:a 11 :b 22 :c 33}]))
    (is (= '({:a 0 :b 0 :c 1} {:a 10 :b 20 :c 31})) (mpr/apply-mappings (map second mappings) [{:a 1 :b 2 :c 3} {:a 11 :b 22 :c 33}]))))

; (run-tests)