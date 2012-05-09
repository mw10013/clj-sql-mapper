(ns clj-sql-mapper.test.dbfn
  (:use clojure.test)
  (:require [clojure.java.jdbc :as jdbc]
            [clj-sql-mapper [sql :as sql] [db :as db] [dbfn :as dbfn]]))

(defonce db (db/create-db {:datasource-spec {:classname "org.hsqldb.jdbcDriver"
                                             :subprotocol "hsqldb"
                                             :subname "clj_sql_mapper_test_hsqldb"}
                           :pool-spec {:idle-time-excess-in-sec (* 15 60)
                                       :idle-time (* 30 60)}
                           :naming-strategy {:keys #(-> % clojure.string/lower-case (clojure.string/replace \_ \-))
                                             :fields #(clojure.string/replace % \- \_)}}))

(defn- db-fixture [t]
  (def db (db/create-db {:datasource-spec {:classname "org.hsqldb.jdbcDriver"
                                        :subprotocol "hsqldb"
                                        :subname "clj_sql_mapper_test_hsqldb"}
                      :pool-spec {:idle-time-excess-in-sec (* 15 60)
                                  :idle-time (* 30 60)}
                      :naming-strategy {:keys #(-> % clojure.string/lower-case (clojure.string/replace \_ \-))
                                        :fields #(clojure.string/replace % \- \_)}}))
  (t)
  (db/destroy-db db))

#_(use-fixtures :once db-fixture)

(defn- table-fixture [t]
  (db/with-db db
    (doseq [table [:fruit]]
      (try
        (jdbc/drop-table table)
        (catch Exception _
          ; ignore
          )))
    (t)))

(use-fixtures :each table-fixture)

(defn- create-test-table [table]
  (db/with-db db
    (jdbc/create-table
     :fruit
     [:id :int]
     [:name "VARCHAR(32)"]
     [:appearance "VARCHAR(32)"]
     [:cost :int]
     [:grade :real])))

(deftest inline-sql
  (create-test-table :fruit)
  (dbfn/definsert insert-fruit db (dbfn/sql "insert into fruit (id, name, appearance, cost, grade) values (:id, :name, :appearance, :cost, :grade)"))
  (is (= '(1)) (insert-fruit :id 11 :name "apple" :appearance "red" :cost 1 :grade 1.0))

  (dbfn/defselect fruit db (dbfn/sql "select * from fruit"))
  (is (= ["select * from fruit"] (dbfn/sql-only (fruit))))
  (is (= [{:id 11, :name "apple", :appearance "red", :cost 1, :grade 1.0}] (fruit)))

  (dbfn/defupdate update-fruit db (dbfn/sql "update fruit set name = :name, appearance = :appearance where id = :id"))
  (is (= '(1) (update-fruit :id 11 :name "orange" :appearance "orangy")))
  (is (= '(0) (update-fruit :id 12 :name "orange" :appearance "orangy")))

  (dbfn/defdelete delete-fruit db (dbfn/sql "delete from fruit where id = :id"))
  (is (= '(1)) (delete-fruit :id 11))
  (is (= '(0)) (delete-fruit :id 11)))

; (run-tests 'clj-sql-mapper.test.dbfn)