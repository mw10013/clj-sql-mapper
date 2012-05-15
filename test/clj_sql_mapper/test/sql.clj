(ns clj-sql-mapper.test.sql
  (:use clojure.test)
  (:require [clj-sql-mapper [sql :as sql]]))

(def cols (sql/sql "col1, col2, col3"))
(def title (sql/sql "and title = :title"))
(def where-title (sql/sql "where title = :title"))

(deftest sql
  (is (= '("select * from table") (sql/sql "select * from table")))
  (is (= '("select * from table") (let [s "select * from table"] (sql/sql s))))
  (is (= '("select * from table where title = " :title) (sql/sql "select * from table where title = :title")))
  (is (= (list "select " #'cols " from table") (sql/sql "select " #'cols " from table")))
  (is (= #'where-title) (sql/sql "#'where-title")))

(deftest sql-when
  (is (= ["where title = ?" "the-title"] ((sql/when :title "where title = :title") {:title "the-title"})))
  (is (= ["where title = ?" "the-title"] (let [s "where title = :title"] ((sql/when :title s) {:title "the-title"}))))
  (is (= ["where title = ?" "the-title"] ((sql/when :title #'where-title) {:title "the-title"})))
  (is (= ["where title = ?" "the-title"] ((sql/when #(:title %) "where title = :title") {:title "the-title"})))
  (is (= ["where title = ?" "the-title"] ((sql/when identity "where title = :title") {:title "the-title"}))))

(deftest prepare
  (is (= ["select * from table"] (sql/prepare {} (sql/sql "select * from table"))))
  (is (= ["select * from table where title = ?" "the-title"]
         (sql/prepare {:title "the-title"} (sql/sql "select * from table where title = :title"))))
  (is (= ["select * from blog where state = 'ACTIVE' and author like ? and title like ?" "the-author" "the-title"]
         (sql/prepare {:author "the-author" :title "the-title"}
                      (sql/sql "select * from blog where state = 'ACTIVE' "
                               (sql/when #(and (% :author) (% :title)) "and author like :author and title like :title")))))
  (is (= ["select * from table where title = ?" "the-title"]
         (let [select-table (sql/sql "select * from table")
               where-title (sql/sql "where title = :title")]
           (sql/prepare {:title "the-title"} [select-table where-title])))))

(deftest prepare-keyword
  (is (= ["select * from table order by col1"] (sql/prepare {:col1 "col1"} (sql/sql "select * from table order by :col1!"))))
  (is (= ["select * from table order by col1"]
         (sql/prepare {:order "col1"} (sql/sql "select * from table" (sql/when :order "order by :order!")))))
  (is (= ["select * from table order by col1, col2"]
         (sql/prepare {:order "col1, col2"} (sql/sql "select * from table" (sql/when :order "order by :order!")))))
  (is (= ["select * from table"]
         (sql/prepare {} (sql/sql "select * from table" (sql/when :order "order by :order!"))))))

(deftest vars
  (is (= ["select col1, col2, col3 from table"] (sql/prepare {} (sql/sql "select" #'cols "from table"))))
  (is (= ["select col1, col2, col3 from table where title = ?" "the-title"]
         (sql/prepare {:title "the-title"} (sql/sql "select" #'cols "from table" (sql/where #'title))))))

(deftest sql-where
  #_(is (= ["" []] (sql/prepare {} (sql/sql (sql/where)))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where "title = :title")))))
  (is (= ["where title = ?" "the-title"] (let [s "title = :title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where s))))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where "or title = :title")))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where "and title = :title")))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where " or title = :title")))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where " and title = :title")))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where "OR title = :title")))))
  (is (= ["where title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/where "AND title = :title")))))
  (is (= ["where title = ?" "the-title"]
         (sql/prepare {:title "the-title"} (sql/sql (sql/where (sql/when :author "and author = :author")
                                                               (sql/when :title "and title = :title"))))))
  (is (= ["where author = ?" "clinton"]
         (sql/prepare {:author "clinton"} (sql/sql (sql/where (sql/when :author "and author = :author")
                                                               (sql/when :title "and title = :title"))))))
  (is (= ["where author = ? and title = ?" "clinton" "the-title"]
         (sql/prepare {:author "clinton" :title "the-title"} (sql/sql (sql/where (sql/when :author "and author = :author")
                                                                                 (sql/when :title "and title = :title")))))) )

(deftest sql-set
  (is (= ["update table set col1 = val1"] (sql/prepare {} (sql/sql "update table" (sql/set "col1 = val1,")))))
  (is (= ["update table set col1 = val1"] (sql/prepare {} (let [s "col1 = val1,"] (sql/sql "update table" (sql/set s))))))
  (is (= ["set title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/set "title = :title,")))))
  (is (= ["set title = ?, author = ?" "the-title" "clinton"]
         (sql/prepare {:title "the-title" :author "clinton"}
                      (sql/sql (sql/set (sql/when :title "title = :title,")
                                        (sql/when :author "author = :author,")))))))

(deftest sql-cond
  (is (= ["title = ?" "the-title"] (sql/prepare {:title "the-title"} (sql/sql (sql/cond (sql/when :title "title = :title")
                                                                                          (sql/when :author "author = :author")
                                                                                          "otherwise")))))
  (is (= ["author = ?" "clinton"] (sql/prepare {:author "clinton"} (sql/sql (sql/cond (sql/when :title "title = :title")
                                                                                        (sql/when :author "author = :author")
                                                                                        "otherwise")))))
  (is (= ["otherwise"] (sql/prepare {} (sql/sql (sql/cond (sql/when :title "title = :title")
                                                             (sql/when :author "author = :author")
                                                             "otherwise")))))
  (is (= ["otherwise"] (sql/prepare (let [s "otherwise"] (sql/sql (sql/cond (sql/when :title "title = :title")
                                                                                        (sql/when :author "author = :author")
                                                                                        s)))))))

(deftest coll
  (is (= ["()"] (sql/prepare {} (sql/sql (sql/coll :coll)))))
  (is (= ["(1, 2, 3)"] (sql/prepare {:coll [1 2 3]} (sql/sql (sql/coll :coll)))))
  (is (= ["('a', 'b', 'c')"] (sql/prepare {:coll [:a :b :c]} (sql/sql (sql/coll :coll)))))
  (is (= ["('a', 'b', 'c')"] (sql/prepare {:coll ["a" "b" "c"]} (sql/sql (sql/coll :coll))))))

; (run-tests 'clj-sql-mapper.test.sql)