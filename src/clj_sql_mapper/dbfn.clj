(ns clj-sql-mapper.dbfn
  (:require [clojure.java.jdbc :as jdbc]
            (clj-sql-mapper [sql :as sql] [db :as db])))

(def ^{:dynamic true} *exec-mode* false)

(defn sql [spec & sqls]
  (update-in spec [:sql] (fnil conj []) (apply sql/sql sqls)))

(defn argkeys [spec argkeys]
  (update-in spec [:argkeys] (fnil into []) argkeys))

(defn spec [base]
  (let [base (or base {})]
    (if (:connection-spec base) {:db base} base)))

(defmacro defspec [name base & body]
  `(def ~name (-> ~base spec ~@body)))

(defn- args->param-map [argkeys & args]
  (cond
   argkeys (zipmap argkeys args)
   (-> args first map?) (first args)
   :else (apply hash-map args)))

; (args->param-map [:appearance] "yellow")

(defn select [spec & args]
  (let [param-map (apply args->param-map (:argkeys spec) args)
        sql (sql/prepare param-map (:sql spec))]
    (cond
     (= *exec-mode* :sql) sql
     (= *exec-mode* :spec) spec
     :else (db/with-db (:db spec)
             (jdbc/with-query-results rs sql (vec rs))))))

(defmacro defselect [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial select spec#))))

(defn- do-prepared [spec & args]
  (let [param-map (apply args->param-map (:argkeys spec) args)
        sql (sql/prepare param-map (:sql spec))]
    (cond
     (= *exec-mode* :sql) sql
     (= *exec-mode* :spec) spec
     :else (db/with-db (:db spec)
             (jdbc/do-prepared (first sql) (rest sql))))))

(def insert do-prepared)

(defmacro definsert [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial insert spec#))))

(def update do-prepared)

(defmacro defupdate [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial update spec#))))

(def delete do-prepared)

(defmacro defdelete [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial delete spec#))))

(defmacro sql-only [& body]
  `(binding [*exec-mode* :sql] ~@body))

(defmacro spec-only [& body]
  `(binding [*exec-mode* :spec] ~@body))