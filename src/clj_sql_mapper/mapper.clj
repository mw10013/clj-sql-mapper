(ns clj-sql-mapper.mapper)

(defn make-mapping
  "Takes f, a function of one arg that returns the arg mapped,
   and a single key or collection of keys.
   Returns a function taking a map. For each k, if the
   corresponding value is not nil, passes it to f and
   puts the result in the map."
  ([f k]
     (fn [m] (if (nil? (m k)) m (update-in m [k] f))))
  ([f k & keys]
     (let [keys (conj keys k)]
          (fn [m]
            (reduce (fn [m k] (if (nil? (m k)) m (update-in m [k] f))) m keys)))))

(defn make-mappings
  "Takes a collection of keys and functions.
   For every function, makes a mapping for the collection
   of keys."
  [ks & fns]
  (let [ks (if (coll? ks) ks [ks])]
    (map #(apply make-mapping % ks) fns)))

(defn- reduce-mappings [mappings m] (reduce #(%2 %1) m mappings))

(defn apply-mappings
  "Takes collection of mappings and x, a map or collection of
   maps. Applies mappings on x."
  [mappings x]
  (if (map? x)
    (reduce-mappings mappings x)
    (map (partial reduce-mappings mappings) x)))

(comment
  (apply-mappings [(fn [m] (update-in m [:a] inc))] {:a 1})
  (apply-mappings [(fn [m] (update-in m [:a] inc))] [{:a 1} {:a 11}])

  (apply-mappings [(make-mapping inc :a)] {:a 1})
  (apply-mappings [(make-mapping inc :a :b)] [{:a 1 :b 2} {:a 11 :b 22}])

  (make-mappings [:a :b] inc dec)
)