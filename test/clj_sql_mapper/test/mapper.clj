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

(deftest reduce-rows
  (is (=  {:as [{:a 1}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :b 1}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a :b]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 1}]}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                                                        :children [{:row-key :bs :match-val-fn :b :ks [:b]}]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 1} {:b 2}]} {:a 2 :bs [{:b 1}] :cs [{:c 3}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                       {:row-key :cs :match-val-fn :c :ks [:c]}]}
                           [{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 1} {:a 2 :c 3}]))))

; (run-tests)