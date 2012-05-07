(ns clj-sql-mapper.dbfn
  (:require (clj-sql-mapper [sql :as sql] [db :as db])))

(defn sql [spec sql]
  (update-in spec [:sql] (fnil conj []) sql))

(defmacro defspec [name base & body]
  `(let [base# (or ~base {})
         base# (if (:connection-spec base#) {:db base#} base#)
         base# (-> base# ~@body)]
     (def ~name base#)))

#_(defmacro defquery [name spec & body]
  `(let [spec# (-> ~spec @~body)]
     (defn ~name [param-map#] (sql/prepare param-map# (:sql spec#)))))

(comment
  (defspec spec nil)
  (defspec spec {})
  (defspec spec {:connection-spec {:datasource "datasource"}})
  (defspec spec {:connection-spec {:datasource "datasource"}} (sql (sql/sql "select * from table")))
  (defspec spec {:connection-spec {:datasource "datasource"}} (sql (sql/sql "select * from table")) (sql (sql/sql " where title = :title")))
  (macroexpand-1 '(defspec spec nil))
  (macroexpand-1 '(defspec spec {}))
  (defquery query spec)
  )
