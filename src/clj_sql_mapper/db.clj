(ns clj-sql-mapper.db
  (:require [clojure.java.jdbc :as jdbc])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defn create-db [db-spec]
  (let [datasource-spec (:datasource-spec db-spec)
        pool-spec (:pool-spec db-spec)
        cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname datasource-spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol datasource-spec) ":" (:subname datasource-spec)))
               (.setUser (:user datasource-spec))
               (.setPassword (:password datasource-spec))
               (.setMaxIdleTimeExcessConnections (or (:max-idle-time-excess-in-sec pool-spec)
                                                     (* 15 60)))
               (.setMaxIdleTime (or (:max-idle-time-in-sec pool-spec) (* 30 60))))]
    {:connection-spec {:datasource cpds}
     :naming-strategy (:naming-strategy db-spec)}))

(defn with-db*
  "Evaluates f in the context of a new/existing connection to db.
   A new connection is created within the context of the db's naming strategy."
  [db f]
  (if (jdbc/find-connection)
    (f)
    (jdbc/with-naming-strategy (:naming-strategy db)
      (jdbc/with-connection (:connection-spec db)
        (f)))))

(defmacro with-db
  "Evaluates body in the context of a new/existing connection to db."
  [db & body]
  `(with-db* ~db (fn [] ~@body)))

(comment
  (def db (create-db {:datasource-spec {:classname "org.hsqldb.jdbcDriver"
                                        :subprotocol "hsqldb"
                                        :subname "clj_sql_mapper_test_hsqldb"}
                      :pool-spec {:idle-time-excess-in-sec (* 15 60)
                                  :idle-time (* 30 60)}
                      :naming-strategy {:keys #(-> % clojure.string/lower-case (clojure.string/replace \_ \-))
                                        :fields #(clojure.string/replace % \- \_)}}))

  (jdbc/with-connection (:connection-spec db)
    (jdbc/create-table
     :fruit
     [:id :int]
     [:name "VARCHAR(32)"]
     [:appearance "VARCHAR(32)"]
     [:cost :int]
     [:grade :real])
    (jdbc/with-query-results rs ["select * from fruit"] (vec rs)))

  (with-db db
    (jdbc/drop-table :fruit))

  (with-db db
    (jdbc/do-prepared "INSERT INTO fruit ( name, appearance, cost, grade ) VALUES ( 'test', 'test', 1, 1.0 )"))

  (with-db db
    (jdbc/with-query-results rs ["select * from fruit"] (vec rs)))

  )

  


