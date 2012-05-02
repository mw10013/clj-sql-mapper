(ns clj-sql-mapper.sql
  "TODO: spacing, cond, substitution, foreach, parent.child, refer warnings
"
  (:refer-clojure :rename {when core-when set core-set cond core-cond})
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn prepare [m sql]
  (reduce (fn [[sql-str sql-params] x]
            (core-cond
             (string? x) [(str sql-str x) sql-params]
             (keyword? x) [(str sql-str \?) (conj sql-params (m x))]
             (var? x) (let [[s p] (prepare m @x)] [(str sql-str s) (into sql-params p)])
             :else (let [[s p] (x m)]
                     [(str sql-str s) (into sql-params p)])))
          [nil []] sql))

(defn- parse [s]
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-]+|#'[a-z0-9\-]+" #(str \" % \")) read-string eval))

(defn- compile-sql [sql]
  (core-cond
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
                     [" where" [] true] sqls))))

(defmacro where [& sqls] `(fn [param-map#] (prepare-where param-map# '~(map sql* sqls))))

(defn prepare-set [m sqls]
  (let [sqls (->> sqls (map (partial prepare m)) (remove (comp str/blank? first)))
        [sql-str params] (reduce (fn [[sql-str params] [s p]]
                                   [(str sql-str " " s) (into params p)])
                                 [" set" []] sqls)
        sql-str (str/replace sql-str #"\s*,\s*$" "")]
    [sql-str params]))

(defmacro set [& sqls] `(fn [param-map#] (prepare-set param-map# '~(map sql* sqls))))

(defn prepare-cond [m sqls]
  (or (->> sqls (map (partial prepare m)) (drop-while (comp str/blank? first)) first) ["" []]))

(defmacro cond [& sqls] `(fn [param-map#] (prepare-cond param-map# '~(map sql* sqls))))

; (prepare {} (sql (cond (when :title "where title = :title"))))
