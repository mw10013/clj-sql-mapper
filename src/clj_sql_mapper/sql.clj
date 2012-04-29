(ns clj-sql-mapper.sql
  "TODO: spacing, sql fragments, where, cond, set, substitution
"
  (:refer-clojure :rename {when core-when})
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn prepare [m sql]
  (reduce (fn [[sql-str sql-params] x]
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

