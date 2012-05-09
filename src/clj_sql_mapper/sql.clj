(ns clj-sql-mapper.sql
  "Dynamic sql for Clojure with apologies to mybatis.

   Use the sql macro to compile SQL represented as strings, vars,
   and functions taking a param map.

   prepare takes a param map and turns compiled SQL into vector
   containing the corresponding SQL string and params.
   The vector is suitable for clojure.java.jdbc/with-query-results."
  (:refer-clojure :rename {when core-when set core-set cond core-cond})
  (:require [clojure.string :as str]))

(defn- parse-str [s]
  "Parses s to return a collection of strings, keywords, and vars."
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-]+" #(str \" % \")) read-string eval))

(defn- compile-sql [sql]
  "Compiles sql into a list of strings, keywords, vars, and functions
   taking a parameter map."
  (core-cond
   (string? sql) (parse-str sql)
   (var? sql) (list sql)
   :else (-> sql eval list)))

(defn sql [& args]
  "Takes args of string, var, and code evaluating into a function taking
   a parameter map argument.

   Strings are parsed into substrings and keywords
   Eg. \"select * from table where title = :title\" parses into
   \"select \" * \" from table where title = \" :title

   Returns collection of 'compiled' sql as strings, keywords, vars, and
   functions taking a parameter map argument. Run the collection through
   prepare with a param map to get the corresponding SQL string with bind
   vars."
  (->> args (mapcat compile-sql) (remove #(and (string? %) (str/blank? %)))))

(defn prepare
  "Takes a param map and compiled sql.
   Returns a vector of SQL string with any bind vars and parameters.
   This vector is suitable for clojure.java.jdbc/with-query-results."
  ([sql] (prepare {} sql))
  ([m sql]
     (reduce (fn [prep-sql x]
               (core-cond
                (string? x) (update-in prep-sql [0] str x)
                (keyword? x) (-> prep-sql (update-in [0] str \?) (conj (m x)))
                (var? x) (let [[s & rest] (prepare m @x)] (-> prep-sql (update-in [0] str s) (into rest)))
                (fn? x) (do (def f x) (let [[s & rest] (x m)] (-> prep-sql (update-in [0] str s) (into rest))))
                (coll? x) (let [[s & rest] (prepare m x)] (-> prep-sql (update-in [0] str s) (into rest)))
                :else (throw (IllegalArgumentException. (str "sql/prepare: can't handle " x " (" (type x) ") in " sql)))))
             [""] sql)))

(defmacro when [& [predicate & sqls]]
  "Compiles to sql that returns prepared sqls if predicate
   returns truthy against a param map."
  (let [predicate (if (seq? predicate) predicate (list predicate))]
    `(fn [param-map#] (core-when (~@predicate param-map#) (prepare param-map# (sql ~@sqls))))))

(defn prepare-where [param-map sqls]
  (when-let [sqls (->> sqls (map (partial prepare param-map)) (remove (comp str/blank? first)) seq)]
    (first (reduce (fn [[prep-sql first?] [s & rest]]
                     (let [s (if first? (str/replace s #"^\s*(?i:and|or)\s+" "") s)]
                       [(-> prep-sql (update-in [0] str " " s) (into rest)) false]))
                   [[" where"] true] sqls))))

(defmacro where
  "Compiles to sql that returns a where clause trimming and/or's as necessary."
  [& sqls]
  `(fn [param-map#] (prepare-where param-map# (map sql (list ~@sqls)))))

(defn prepare-set [m sqls]
  (let [sqls (->> sqls (map (partial prepare m)) (remove (comp str/blank? first)))]
    (-> (reduce (fn [[sql-str :as prep-sql] [s & rest]]
                  (-> prep-sql (update-in [0] str " " s) (into rest)))
                [" set"] sqls)
        (update-in [0] str/replace #"\s*,\s*$" ""))))

(defmacro set
  "Compiles to sql that returns a set clause trimming comma's as necssary."
  [& sqls]
  `(fn [param-map#] (prepare-set param-map# (map sql (list ~@sqls)))))

(defn prepare-cond [m sqls]
  (or (->> sqls (map (partial prepare m)) (drop-while (comp str/blank? first)) first) [""]))

(defmacro cond
  "Compiles to sql that returns the first non-blank sql."
  [& sqls]
  `(fn [param-map#] (prepare-cond param-map# (map sql (list ~@sqls)))))

(defn prepare-coll [m k]
  (let [coll (->> k m (map #(if (or (string? %) (keyword? %)) (str \' (name %) \') %)) (interpose ", ") (apply str))]
    [(str " (" coll \))]))

(defmacro coll
  "Compiles to sql that returns a sql collection for k in a param map.
   If k corresponding to [:a :b :c] the sql collection would be
   ('a', 'b', 'c')."
  [k]
  `(fn [param-map#] (prepare-coll param-map# ~k)))
