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
          ))))
  (t))

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

(deftest base-specs
  (declare base-spec arg-spec)
  
   (dbfn/defspec test-db-spec nil
    (#(is (empty? %))))
  
  (dbfn/defspec test-db-spec db
    ((fn [spec] (is (= db (:db spec))) spec)))
    
  (dbfn/defspec base-spec test-db-spec
    ((fn [spec] (is (= db (:db spec))) spec)))

  (dbfn/defspec arg-spec nil
    (dbfn/argkeys [:a :b :c])
    ((fn [spec] (is (= [:a :b :c] (:argkeys spec))) spec)))

  (dbfn/defspec doc-spec nil
    (dbfn/doc "a doc string")
    ((fn [spec] (is (= "a doc string" (:doc spec))) spec)))

  (dbfn/defspec db-and-args [base-spec arg-spec]
    (dbfn/doc "a doc string")
    ((fn [spec] (is (= db (:db spec))) spec))
    ((fn [spec] (is (= [:a :b :c] (:argkeys spec))) spec))))

(deftest inline-sql
  (create-test-table :fruit)
  (dbfn/definsert insert-fruit db (dbfn/sql "insert into fruit (id, name, appearance, cost, grade) values (:id, :name, :appearance, :cost, :grade)"))
  (is (= '(1) (insert-fruit :id 11 :name "apple" :appearance "red" :cost 1 :grade 1.0)))

  (dbfn/defselect fruit db (dbfn/sql "select * from fruit"))
  (is (= ["select * from fruit"] (dbfn/sql-only (fruit))))
  (is (= [{:id 11, :name "apple", :appearance "red", :cost 1, :grade 1.0}] (fruit)))

  (dbfn/defupdate update-fruit db (dbfn/sql "update fruit set name = :name, appearance = :appearance where id = :id"))
  (is (= '(1) (update-fruit :id 11 :name "orange" :appearance "orangy")))
  (is (= '(0) (update-fruit :id 12 :name "orange" :appearance "orangy")))

  (is (= '(1)) (insert-fruit :id 22 :name "banana" :appearance "yellow" :cost 22 :grade 2.0))

  (def cols (sql/sql "id, name, appearance "))
  (def by-id (sql/sql "where id = :id"))
  (dbfn/defselect select-by-id db (dbfn/sql "select" cols "from fruit" by-id))
  (is (= [{:id 22 :name "banana" :appearance "yellow"}] (select-by-id :id 22)))

  (dbfn/defspec fruit-base db (dbfn/sql "select" cols "from fruit"))
  (dbfn/defselect all-fruit fruit-base)
  (is (= 2 (count (all-fruit))))

  (dbfn/defselect by-appearance fruit-base
    (dbfn/argkeys [:appearance])
    (dbfn/sql "where appearance = :appearance"))
  (is (= ["select id, name, appearance from fruit where appearance = ?" "yellow"]
         (dbfn/sql-only (by-appearance "yellow"))))
  (is  (= 2 (-> (dbfn/spec-only (by-appearance "yellow")) :sql count)))
  (is (= 1 (count (by-appearance "yellow"))))

  (dbfn/defdelete delete-fruit db (dbfn/sql "delete from fruit where id = :id"))
  (is (= '(1)) (delete-fruit :id 11))
  (is (= '(0)) (delete-fruit :id 11)))

(deftest prepare-transform
  (create-test-table :fruit)
  (dbfn/definsert insert-fruit-1 db
    (dbfn/argkeys [:id :name :appearance])
    (dbfn/prepare (fn [m] (update-in m [:name] str "-1")))
    (dbfn/sql "insert into fruit (id, name, appearance) values (:id, :name, :appearance)"))
  (is (= '(1) (insert-fruit-1 111 "watermelon" "pink")))

  (dbfn/defselect fruit-1 db
    (dbfn/argkeys [:name])
    (dbfn/prepare (fn [m] (update-in m [:name] str "-1")))
    (dbfn/sql "select id, name, appearance from fruit where name = :name")
    (dbfn/transform (fn [rs] (update-in rs [0 :name] str "-2"))))
  (is (= '[{:id 111 :name "watermelon-1-2" :appearance "pink"}])) (fruit-1 "watermelon"))

(deftest generated-keys
  (create-test-table :fruit)
  (dbfn/definsert insert-generated-key db
    (dbfn/argkeys [:id :name :appearance])
    (dbfn/generated-keys [:ID]) ; hsqldb cap rules.
    (dbfn/sql "insert into fruit (" sql/param-keys ") values (" sql/param-vals ")"))
  (is (= [{:id 1}] (insert-generated-key 1 "apple" "red"))))

(deftest insert-multiple
  (create-test-table :fruit)
  (dbfn/definsert insert-multiple db
    (dbfn/sql "insert into fruit (" sql/param-keys ") values (" sql/param-vals ")"))
  (is (= '(1) (insert-multiple {:id 1 :name "apple" :appearance "red"})))
  (is (= '(1) (insert-multiple :id 1 :name "apple" :appearance "red")))
  (is (= '(1 1) (insert-multiple {:id 1 :name "apple" :appearance "red"} {:id 2 :name "orange" :appearance "orange"})))
  (is (= '(1 1) (insert-multiple [{:id 1 :name "apple" :appearance "red"} {:id 2 :name "orange" :appearance "orange"}]))))

(deftest exec-modes
  (dbfn/defselect fruit-modes db
    (dbfn/argkeys [:name :cost])
    (dbfn/sql "select * from fruit where name = :name and cost = :cost"))
  (is (= ["select * from fruit where name = ? and cost = ?" "kiwi" 1])
      (dbfn/sql-only (fruit-modes "kiwi" 1)))
  (is (= ["select * from fruit where name = ? and cost = ?" :name :cost])
      (dbfn/keywords-only (fruit-modes "kiwi" 1)))
  (is (= ["select * from fruit where name = 'kiwi' and cost = 1"])
      (dbfn/keywords!-only (fruit-modes "kiwi" 1)))
  (is (map? (dbfn/spec-only (fruit-modes "kiwi" 1)))))

; (run-tests 'clj-sql-mapper.test.dbfn)