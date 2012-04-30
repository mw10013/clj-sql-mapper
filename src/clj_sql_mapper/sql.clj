(ns clj-sql-mapper.sql
  "TODO: spacing, sql fragments, where, cond, set, substitution
"
  (:refer-clojure :rename {when core-when})
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn prepare [m sql]
  (println "prepare: " sql)
  (reduce (fn [[sql-str sql-params] x]
            (println "prepare x: " x (type x))
            (cond
             (string? x) [(str sql-str x) sql-params]
             (keyword? x) [(str sql-str \?) (conj sql-params (m x))]
             :else (let [[s params] (x m)]
                     [(str sql-str s) (into sql-params params)])))
          [nil []] sql))

(defn- parse [s]
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-]+" #(str \" % \")) read-string eval))

(defn- compile-sql [sql]
  (cond
   (string? sql) (parse sql)
   :else (-> sql eval list)))

(defn sql* [& args]
  (->> args (mapcat compile-sql) (remove #(and (string? %) (str/blank? %)))))

(defmacro sql [& args]
  (let [sql (apply sql* args)]
    `(list  ~@sql)))

(defmacro when [& [predicate & coll]]
  (let [predicate (if (seq? predicate) predicate (list predicate))
        coll (apply sql* coll)]
    `(fn [param-map#] (core-when (~@predicate param-map#) (prepare param-map# (list ~@coll))))))

(defn prepare-where [m coll]
  (println "prepare-where")
  (println "prepare-where: " coll (type coll))
  (when-let [coll (->> coll (map (partial prepare m)) (remove (comp str/blank? first)) seq)]
    (let [[ sql-str params] (reduce (fn [[sql-str params] [s p]]
                                      (let [s (if (str/blank? sql-str) (str/replace s #"^\s*(?i:and|or)\s+" "") s)]
                                        [(str sql-str " " s) (into params p)]))
                                    [nil []] coll)]
      [(str "where " sql-str) params])))

(defmacro where [& coll]
  (let [coll (map #(list 'list (sql*) %) coll)]
    (prn coll)
    `(fn [param-map#] (prepare-where param-map# (list ~@coll)))))

; (sql (where "title = :title"))
; (prepare {:title "the-title"} (sql (where "title = :title")))

