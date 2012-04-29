(ns clj-sql-mapper.sql
  (:refer-clojure :rename {when core-when})
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]))

(defn- parse [s]
  #_(log/info "parse: " s)
  (-> (str "'(\"" s "\")")  (str/replace  #":[a-z0-9\-]+" #(str \" % \")) read-string eval))

(defn- compile [sql]
  (cond
   (string? sql) (parse sql)
   :else (-> sql eval list)))

(defn sql* [& args]
  (->> args (mapcat compile) (remove #(and (string? %) (str/blank? %)))))

(defmacro sql [& args]
  (let [sql (apply sql* args)]
    `(list  ~@sql)))

; (sql "select * from table where title = :title")
; (sql "select * from table" (when :title "where title = :title"))
; (sql (when identity "where title = :titleeee"))
; (sql (when :title "where title = :title"))

(defmacro when [& [predicate & coll]]
  (let [predicate (if (seq? predicate) predicate (list predicate))
        coll (apply sql* coll)]
    `(fn [param-map#] (core-when (~@predicate param-map#) (list ~@coll)))))

; (when :title "where title = :title")
; ((when :title "where title = :title") {:title "title"})
; (macroexpand-1 '(when :title "where title = :title"))
; (when #(:title %) "where title = :title")
; ((when #(:title %) "where title = :title") {:title "title"})
; (when identity "where title = :title")
; ((when identity "where title = :title") {:title "title"})
; (macroexpand-1 '(when identity "where title = :title"))

(comment
  (parse "select * from table where id = :id")
  (parse "insert into table (id, col1, col2) values (seq.nextval, :col1, :col2")
  (-> "[\"select * from table where id = :id\"]" parse read-string eval)
  (re-seq #"(:[a-z0-9\-]+)" "1abc e :the-col :id")

  (sql/sql "select * from blog where state = 'ACTIVE'"
           (sql/when :title "where title like :title")
           (sql/when predicate "where title like :title")
           (sql/when #(or (% :title) (% :description)) "where title like :title"))

  (defsql "select * from blog where state = 'ACTIVE'"
    (when (:title *ctx*)
      "AND title like :title")
    (when (:author *ctx)
      "AND author_name like :author"))

  (defsql "select * from blog where state = 'ACTIVE'"
    (where 
     (when (:title *ctx*)
       "AND title like :title")
     (when (:author *ctx)
       "AND author_name like :author")))
  )
