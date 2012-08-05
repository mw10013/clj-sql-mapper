(ns clj-sql-mapper.test.dbfn
  (:use clojure.test)
  (:require [clojure.java.jdbc :as jdbc]
            [clj-sql-mapper [sql :as sql] [db :as db] [dbfn :as d]]))

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
  
   (d/defspec test-db-spec nil
    (#(is (empty? %))))
  
  (d/defspec test-db-spec db
    ((fn [spec] (is (= db (:db spec))) spec)))
    
  (d/defspec base-spec test-db-spec
    ((fn [spec] (is (= db (:db spec))) spec)))

  (d/defspec arg-spec nil
    (d/argkeys [:a :b :c])
    ((fn [spec] (is (= [:a :b :c] (:argkeys spec))) spec)))

  (d/defspec doc-spec nil
    (d/doc "a doc string")
    ((fn [spec] (is (= "a doc string" (:doc spec))) spec)))

  (d/defspec db-and-args [base-spec arg-spec]
    (d/doc "a doc string")
    ((fn [spec] (is (= db (:db spec))) spec))
    ((fn [spec] (is (= [:a :b :c] (:argkeys spec))) spec))))

(deftest inline-sql
  (create-test-table :fruit)
  (d/definsert insert-fruit db (d/sql "insert into fruit (id, name, appearance, cost, grade) values (:id, :name, :appearance, :cost, :grade)"))
  (is (= '(1) (insert-fruit :id 11 :name "apple" :appearance "red" :cost 1 :grade 1.0)))

  (d/defselect fruit db (d/sql "select * from fruit"))
  (is (= ["select * from fruit"] (d/sql-only (fruit))))
  (is (= [{:id 11, :name "apple", :appearance "red", :cost 1, :grade 1.0}] (fruit)))

  (d/defupdate update-fruit db (d/sql "update fruit set name = :name, appearance = :appearance where id = :id"))
  (is (= '(1) (update-fruit :id 11 :name "orange" :appearance "orangy")))
  (is (= '(0) (update-fruit :id 12 :name "orange" :appearance "orangy")))

  (is (= '(1)) (insert-fruit :id 22 :name "banana" :appearance "yellow" :cost 22 :grade 2.0))

  (def cols (sql/sql "id, name, appearance "))
  (def by-id (sql/sql "where id = :id"))
  (d/defselect select-by-id db (d/sql "select" cols "from fruit" by-id))
  (is (= [{:id 22 :name "banana" :appearance "yellow"}] (select-by-id :id 22)))

  (d/defspec fruit-base db (d/sql "select" cols "from fruit"))
  (d/defselect all-fruit fruit-base)
  (is (= 2 (count (all-fruit))))

  (d/defselect by-appearance fruit-base
    (d/argkeys [:appearance])
    (d/sql "where appearance = :appearance"))
  (is (= ["select id, name, appearance from fruit where appearance = ?" "yellow"]
         (d/sql-only (by-appearance "yellow"))))
  (is  (= 2 (-> (d/spec-only (by-appearance "yellow")) :sql count)))
  (is (= 1 (count (by-appearance "yellow"))))

  (d/defdelete delete-fruit db (d/sql "delete from fruit where id = :id"))
  (is (= '(1)) (delete-fruit :id 11))
  (is (= '(0)) (delete-fruit :id 11)))

(deftest prepare-transform
  (create-test-table :fruit)
  (d/definsert insert-fruit-1 db
    (d/argkeys [:id :name :appearance])
    (d/prepare (fn [m] (update-in m [:name] str "-1")))
    (d/sql "insert into fruit (id, name, appearance) values (:id, :name, :appearance)"))
  (is (= '(1) (insert-fruit-1 111 "watermelon" "pink")))

  (d/defselect fruit-1 db
    (d/argkeys [:name])
    (d/prepare (fn [m] (update-in m [:name] str "-1")))
    (d/sql "select id, name, appearance from fruit where name = :name")
    (d/transform (fn [rs] (update-in rs [0 :name] str "-2"))))
  (is (= '[{:id 111 :name "watermelon-1-2" :appearance "pink"}])) (fruit-1 "watermelon"))

(deftest generated-keys
  (create-test-table :fruit)
  (d/definsert insert-generated-key db
    (d/argkeys [:id :name :appearance])
    (d/generated-keys [:ID]) ; hsqldb cap rules.
    (d/sql "insert into fruit (" sql/param-keys ") values (" sql/param-vals ")"))
  (is (= [{:id 1}] (insert-generated-key 1 "apple" "red"))))

(deftest insert-multiple
  (create-test-table :fruit)
  (d/definsert insert-multiple db
    (d/sql "insert into fruit (" sql/param-keys ") values (" sql/param-vals ")"))
  (is (= '(1) (insert-multiple {:id 1 :name "apple" :appearance "red"})))
  (is (= '(1) (insert-multiple :id 1 :name "apple" :appearance "red")))
  (is (= '(1 1) (insert-multiple {:id 1 :name "apple" :appearance "red"} {:id 2 :name "orange" :appearance "orange"})))
  (is (= '(1 1) (insert-multiple [{:id 1 :name "apple" :appearance "red"} {:id 2 :name "orange" :appearance "orange"}]))))

(deftest exec-modes
  (d/defselect fruit-modes db
    (d/argkeys [:name :cost])
    (d/sql "select * from fruit where name = :name and cost = :cost"))
  (is (= ["select * from fruit where name = ? and cost = ?" "kiwi" 1])
      (d/sql-only (fruit-modes "kiwi" 1)))
  (is (= ["select * from fruit where name = ? and cost = ?" :name :cost])
      (d/keywords-only (fruit-modes "kiwi" 1)))
  (is (= ["select * from fruit where name = 'kiwi' and cost = 1"])
      (d/keywords-only! (fruit-modes "kiwi" 1)))
  (is (map? (d/spec-only (fruit-modes "kiwi" 1)))))

; (run-tests 'clj-sql-mapper.test.dbfn)