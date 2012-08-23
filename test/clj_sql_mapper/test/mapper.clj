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

(deftest partial-mappings
  (let [mappings-1 [(mpr/make-mapping inc :a)]
        mappings-2 [(mpr/make-mapping (partial + 2) :b)
                    (mpr/make-mapping (partial + 3) :c)]]
    (is (= {:a 2 :b 3 :c 4} (mpr/apply-mappings mappings-1 mappings-2 {:a 1 :b 1 :c 1})))
    (is (= {:a 2 :b 1 :c 1} ((partial mpr/apply-mappings mappings-1) {:a 1 :b 1 :c 1})))
    (is (= {:a 2 :b 3 :c 4} ((partial (partial mpr/apply-mappings mappings-1) mappings-2) {:a 1 :b 1 :c 1})))))

(deftest reduce-rows
  (is (=  {:as [{:a 1}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :b 1}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a :b]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 1}]}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                                                        :children [{:row-key :bs :match-val-fn :b :ks [:b]}]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 1} {:b 2}]} {:a 2 :bs [{:b 1}] :cs [{:c 3}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                       {:row-key :cs :match-val-fn :c :ks [:c]}]}
                           [{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 1} {:a 2 :c 3}])))
  (is (=  {:as [{:a 1 :cs [{:c 1 :ds [{:d 1}]}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                       {:row-key :cs :match-val-fn :c :ks [:c]
                                        :children [{:row-key :ds :match-val-fn :d :ks [:d]}]}]}
                           [{:a 1 :c 1 :d 1}])))
  (is (=  {:as [{:a 1 :cs [{:c 1 :ds [{:d 1} {:d 2}]}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                       {:row-key :cs :match-val-fn :c :ks [:c]
                                        :children [{:row-key :ds :match-val-fn :d :ks [:d]}]}]}
                           [{:a 1 :c 1 :d 1} {:a 1 :c 1 :d 2}])))
  (is (=  {:as [{:a 1 :bs [{:b 1}] :cs [{:c 1 :ds [{:d 1} {:d 2}]}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                       {:row-key :cs :match-val-fn :c :ks [:c]
                                        :children [{:row-key :ds :match-val-fn :d :ks [:d]}]}]}
                           [{:a 1 :c 1 :d 1} {:a 1 :c 1 :d 2} {:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 2 :cs [{:c 3}]}]} {:a 2 :bs [{:b 2 :cs [{:c 3}]}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]
                                        :children [{:row-key :cs :match-val-fn :c :ks [:c]}]}]}
                           [ {:a 1 :b 2 :c 3} {:a 2 :b 2 :c 3} ]))))

(deftest reduce-rows-with-mappings
  (is (=  {:as [{:a 2 :b 1}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a :b] :mappings [(mpr/make-mapping inc :a)]} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 2 :b 2}]} (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a :b] :mappings (mpr/make-mappings [:a :b] inc)} [{:a 1 :b 1}])))
  (is (=  {:as [{:a 1 :bs [{:b 2 :cs [{:c 4}]}]} {:a 2 :bs [{:b 2 :cs [{:c 4}]}]}]}
          (mpr/reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                            :children [{:row-key :bs :match-val-fn :b :ks [:b]
                                        :children [{:row-key :cs :match-val-fn :c :ks [:c] :mappings [(mpr/make-mapping inc :c)]}]}]}
                           [ {:a 1 :b 2 :c 3} {:a 2 :b 2 :c 3} ]))))

(run-tests)