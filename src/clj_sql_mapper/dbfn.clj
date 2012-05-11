(ns clj-sql-mapper.dbfn
  (:require [clojure.java.jdbc :as jdbc]
            (clj-sql-mapper [sql :as sql] [db :as db])))

(def ^{:dynamic true} *exec-mode* false)

(defn sql [spec & sqls]
  (update-in spec [:sql] (fnil conj []) (apply sql/sql sqls)))

(defn argkeys [spec argkeys]
  "Set the collection of keys to zip map against the args
   to form the param map."
  (assoc-in spec [:argkeys] argkeys))

(defn prepare
  "Set function to be applied to param map."
  [spec f]
  (assoc-in spec [:prepare-fn] f))

(defn transform
  "Set function to be applied to restul set."
  [spec f]
  (assoc-in spec [:transform-fn] f))

(defn spec [base]
  (let [base (or base {})]
    (if (:connection-spec base) {:db base} base)))

(defmacro defspec [name base & body]
  `(def ~name (-> ~base spec ~@body)))

(defn- args->param-map [{:keys [argkeys prepare-fn]} args]
  (let [param-map (cond
                   argkeys (zipmap argkeys args)
                   (-> args first map?) (first args)
                   :else (apply hash-map args))
        prepare-fn (or prepare-fn identity)]
    (prepare-fn param-map)))

(defn select [spec & args]
  (let [param-map (args->param-map spec args)
        sql (sql/prepare param-map (:sql spec))]
    (cond
     (= *exec-mode* :sql) sql
     (= *exec-mode* :spec) spec
     :else (db/with-db (:db spec)
             (jdbc/with-query-results rs sql
               (if-let [transform-fn (:transform-fn spec)] (transform-fn rs) (vec rs)))))))

(defmacro defselect [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial select spec#))))

(defn- do-prepared [spec & args]
  (let [param-map (args->param-map spec args)
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