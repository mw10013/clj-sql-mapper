(ns clj-sql-mapper.sql
  "TODO: spacing, cond, set, substitution, foreach, parent.child
"
  (:refer-clojure :rename {when core-when})
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn prepare [m sql]
  (reduce (fn [[sql-str sql-params] x]
            (cond
             (string? x) [(str sql-str x) sql-params]
             (keyword? x) [(str sql-str \?) (conj sql-params (m x))]
             (var? x) (let [[s p] (prepare m @x)] [(str sql-str s) (into sql-params p)])
             :else (let [[s params] (x m)]
                     [(str sql-str s) (into sql-params params)])))
          [nil []] sql))

(defn- parse [s]
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-]+|#'[a-z0-9\-]+" #(str \" % \")) read-string eval))

(defn- compile-sql [sql]
  (cond
   (string? sql) (parse sql)
   (var? sql) sql
   :else (-> sql eval list)))

(defn sql* [& args]
  (->> args (mapcat compile-sql) (remove #(and (string? %) (str/blank? %)))))

(defmacro sql [& args]
  (let [sql (apply sql* args)]
    `(list  ~@sql)))

(defmacro when [& [predicate & sqls]]
  (let [predicate (if (seq? predicate) predicate (list predicate))
        sqls (apply sql* sqls)]
    `(fn [param-map#] (core-when (~@predicate param-map#) (prepare param-map# (list ~@sqls))))))

(defn prepare-where [m sqls]
  (when-let [sqls (->> sqls (map (partial prepare m)) (remove (comp str/blank? first)) seq)]
    (butlast (reduce (fn [[sql-str params first?] [s p]]
                       (let [s (if first? (str/replace s #"^\s*(?i:and|or)\s+" "") s)]
                         [(str sql-str " " s) (into params p) false]))
                     ["where" [] true] sqls))))

(defmacro where [& sqls] `(fn [param-map#] (prepare-where param-map# '~(map sql* sqls))))
