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

(defn destroy-db [db]
  (when-let [datasource (-> db :connection-spec :datasource)]
    (com.mchange.v2.c3p0.DataSources/destroy datasource)))

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
