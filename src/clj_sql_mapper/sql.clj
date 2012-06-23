(ns clj-sql-mapper.sql
  "Dynamic sql for Clojure with apologies to mybatis.

   Use the sql fn to compile SQL represented as strings, vars,
   and functions taking a param map.

   prepare takes a param map and turns compiled SQL into vector
   containing the corresponding SQL string and params.
   The vector is suitable for clojure.java.jdbc/with-query-results."
  (:refer-clojure :rename {when core-when set core-set cond core-cond})
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(def ^{:dynamic true :doc ":sql - bind vars, :keywords - return keywords,
  :keywords! - substitute values."}
  *keyword-mode* :sql)

(defn- parse-str [s]
  "Parses s to return a collection of strings and keywords."
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-!]+" #(str \" % \")) read-string eval))

; (parse-str "xmlelement(\"title\", :title)")
; (parse-str "xmlelement(\\\"title\\\", :title)")

(defn- compile-sql [sql]
  "Compiles sql into a list of strings, keywords, vars, colls,
   and functions taking a parameter map.
   Always returns a coll so sql can mapcat."
  (core-cond
   (string? sql) (parse-str sql)
   (or (fn? sql) (var? sql) (keyword? sql)) (list sql)
   (coll? sql) sql
   :else (throw (IllegalArgumentException. (str "sql/compile-sql: " sql)))))

(defn sql
  "Takes args of string, var, coll, and fns taking
   a parameter map argument.

   Strings are parsed into substrings and keywords
   Eg. \"select * from table where title = :title\" parses into
   \"select * from table where title = \" :title

   Returns collection of 'compiled' sql as strings, keywords, vars, and
   functions taking a parameter map argument. Run the collection through
   prepare with a param map to get the corresponding SQL string with bind
   vars."
  [& args]
  (->> args (mapcat compile-sql) (remove #(and (string? %) (str/blank? %)))))

(defn str-space [x y]
  "(str x y) with a space in between if the last char
   of x is a letter or digit and the first char of y is not ).
   If y is blank, returns x."
  (if (str/blank? y)
    x
    (let [ch (last x)]
      (if (and ch (Character/isLetterOrDigit ch) (not= (first y) \)))
        (str x \space y)
        (str x y)))))

(defn- prepare-keyword
  "If keyword k ends with ! then substitute the corresponding value of k without the !
   into prep-sql. Otherwise use *keyword-mode*,
   :sql - bind var
   :keywords - keyword as bind var
   :keywords! - value as bind var"
  [prep-sql m k]
  (if (= (-> k name last) \!)
    (-> prep-sql (update-in [0] str-space (m (->> k name butlast (apply str) keyword))))
    (condp = *keyword-mode*
      :sql (-> prep-sql (update-in [0] str \?) (conj (m k)))
      :keywords (-> prep-sql (update-in [0] str \?) (conj k))
      :keywords! (let [v (m k)
                       v (if (string? v) (str \' v \') v)]
                   (update-in prep-sql [0] str v)))))

(defn prepare
  "Takes a param map and compiled sql.
   Returns a vector of SQL string with any bind vars and parameters.
   This vector is suitable for clojure.java.jdbc/with-query-results."
  ([sql] (prepare {} sql))
  ([m sql]
     (reduce (fn [prep-sql x]
               (core-cond
                (string? x) (update-in prep-sql [0] str-space x)
                (keyword? x) (prepare-keyword prep-sql m x )
                (var? x) (let [[s & rest] (prepare m @x)] (-> prep-sql (update-in [0] str-space s) (into rest)))
                (fn? x) (let [[s & rest] (x m)] (-> prep-sql (update-in [0] str-space s) (into rest)))
                (coll? x) (let [[s & rest] (prepare m x)] (-> prep-sql (update-in [0] str-space s) (into rest)))
                :else (throw (IllegalArgumentException. (str "sql/prepare: can't handle " x " (" (type x) ") in " sql)))))
             [""] sql)))

(defmacro when [& [predicate & sqls]]
  "Compiles to sql that returns prepared sqls if predicate
   returns truthy against a param map."
  (let [predicate (if (seq? predicate) predicate (list predicate))]
    `(fn [param-map#] (core-when (~@predicate param-map#) (prepare param-map# (sql ~@sqls))))))

(defmacro when [& [predicate & sqls]]
  "Compiles to sql that returns prepared sqls if predicate
   returns truthy against a param map."
  `(fn [param-map#] (core-when (~predicate param-map#) (prepare param-map# (sql ~@sqls)))))

(defn prepare-where [param-map sqls]
  (when-let [sqls (->> sqls (map (partial prepare param-map)) (remove (comp str/blank? first)) seq)]
    (first (reduce (fn [[prep-sql first?] [s & rest]]
                     (let [s (if first? (str/replace s #"^\s*(?i:and|or)\s+" "") s)]
                       [(-> prep-sql (update-in [0] str " " s) (into rest)) false]))
                   [["where"] true] sqls))))

(defmacro where
  "Compiles to sql that returns a where clause trimming and/or's as necessary."
  [& sqls]
  `(fn [param-map#] (prepare-where param-map# (map sql (list ~@sqls)))))

(defn prepare-set [m sqls]
  (let [sqls (->> sqls (map (partial prepare m)) (remove (comp str/blank? first)))]
    (-> (reduce (fn [[sql-str :as prep-sql] [s & rest]]
                  (-> prep-sql (update-in [0] str " " s) (into rest)))
                ["set"] sqls)
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
  (let [coll (->> k m (map #(if (or (string? %) (keyword? %)) (str \' (name %) \') %)) (interpose \,) (apply str))]
    [(str "(" coll \))]))

(defmacro coll
  "Compiles to sql that returns a sql collection for k in a param map.
   If k corresponding to [:a :b :c] the sql collection would be
   ('a','b','c')."
  [k]
  `(fn [param-map#] (prepare-coll param-map# ~k)))

(defn param-keys [m]
  [(apply str (interpose \, (map jdbc/as-identifier (keys m))))])

(defn param-vals [m]
  (reduce (fn [prep-sql x]
            (if (keyword? x)
              (prepare-keyword prep-sql m x)
              (update-in prep-sql [0] str x)))
          [""] (->> m keys (interpose \,))))

