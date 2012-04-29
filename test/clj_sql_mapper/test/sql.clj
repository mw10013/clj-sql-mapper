(ns clj-sql-mapper.test.sql
  (:use clojure.test)
  (:require [clj-sql-mapper [sql :as sql]]))

(deftest sql
  (is (= '("select * from table") (sql/sql "select * from table")))
  (is (= '("select * from table where title = " :title) (sql/sql "select * from table where title = :title"))))

(deftest when
  (is (= ["where title = ?" ["the-title"]] ((sql/when :title "where title = :title") {:title "the-title"})))
  (is (= ["where title = ?" ["the-title"]] ((sql/when #(:title %) "where title = :title") {:title "the-title"})))
  (is (= ["where title = ?" ["the-title"]] ((sql/when identity "where title = :title") {:title "the-title"}))))

(deftest prepare
  (is (= ["select * from table" []] (sql/prepare {} (sql/sql "select * from table"))))
  (is (= ["select * from table where title = ?" ["the-title"]]
         (sql/prepare {:title "the-title"} (sql/sql "select * from table where title = :title")))))

(run-tests 'clj-sql-mapper.test.sql)