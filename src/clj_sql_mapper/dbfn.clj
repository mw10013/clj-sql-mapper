(ns clj-sql-mapper.dbfn
  (:require [clojure.java.jdbc :as jdbc]
            (clj-sql-mapper [sql :as sql] [db :as db])))

(def ^{:dynamic true :doc ":sql - return sql, :keywords - return keywords,
  :keywords! - substitue values, otherwise exec sql"}
  *exec-mode* nil)

(defn doc [spec s]
  "Set the doc string to s."
  (assoc-in spec [:doc] s))

(defn argkeys [spec argkeys]
  "Set the collection of keys to zip map against the args
   to form the param map."
  (assoc-in spec [:argkeys] argkeys))

(defn prepare
  "Set function to be applied to param map."
  [spec f]
  (assoc-in spec [:prepare-fn] f))

(defn sql [spec & sqls]
  (update-in spec [:sql] (fnil conj []) (apply sql/sql sqls)))

(defn transform
  "Set function to be applied to restul set."
  [spec f]
  (assoc-in spec [:transform-fn] f))

(defn spec [base-specs]
  "Make spec from base-specs.
   If a base spec is a db spec, it will be put into the spec
   with :db key. base-specs can by nil or a map."
  (let [base-specs (cond
                    (map? base-specs) [base-specs]
                    (or (nil? base-specs) (empty? base-specs)) [{}]
                    :else base-specs)
        base-specs (map (fn [base]
                          (if (:connection-spec base) {:db base} base)) base-specs)]
    (apply merge base-specs)))

(defmacro defspec [name base & body]
  `(def ~name (-> ~base spec ~@body)))

(defn- args->param-map [{:keys [argkeys prepare-fn]} args]
  (let [param-map (cond
                   argkeys (zipmap argkeys args)
                   (-> args first map?) (first args)
                   :else (apply hash-map args))
        prepare-fn (or prepare-fn identity)]
    (prepare-fn param-map)))

(defn- exec [spec f args]
  (binding [sql/*keyword-mode* (if (#{:keywords :keywords!} *exec-mode*) *exec-mode* :sql)]
    (let [param-map (args->param-map spec args)
          sql (sql/prepare param-map (:sql spec))]
      (cond
       (#{:sql :keywords :keywords!} *exec-mode*) sql
       (= *exec-mode* :spec) spec
       :else (f spec sql)))))

(defn select [spec & args]
  (exec spec
        (fn [spec sql]
          (jdbc/with-query-results rs sql
            (if-let [transform-fn (:transform-fn spec)] (-> rs vec transform-fn) (vec rs))))
        args))

(defmacro defselect [name spec & body]
  `(let [spec# (-> ~spec spec ~@body)]
     (def ~name (partial select spec#))))

(defn- do-prepared [spec & args]
  (exec spec
        (fn [spec sql] (jdbc/do-prepared (first sql) (rest sql)))
        args))

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

(defmacro keywords-only [& body]
  `(binding [*exec-mode* :keywords] ~@body))

(defmacro keywords!-only [& body]
  `(binding [*exec-mode* :keywords!] ~@body))

(defmacro spec-only [& body]
  `(binding [*exec-mode* :spec] ~@body))
