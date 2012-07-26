(ns clj-sql-mapper.mapper
  (:use [clojure.core.incubator :only [dissoc-in]]))

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

(defn- result-path
  "Returns result path for m into the final result map.
   Path starts with :result"
  [m path]
  (let [result-path (reduce
                     (fn [p k]
                       (let [p (conj p k)
                             coll (get-in m p)]
                         (when-not (vector? coll)
                           (throw (Exception. (str "result-path: expected vector at " p " instead of " coll))))
                         (when (= (count coll) 0) (throw (Exception. (str "result-path: coll at " p " is empty."))))
                         (conj p (-> coll count dec))))
         [:result] (butlast path))]
    (conj result-path (last path))))

(defn- reduce-row
  "Reduce r into m using the :matches in m."
  [m r]
  (reduce
   (fn [m {path ::path :keys [match-val-fn ks mappings]}]
     (let [v (match-val-fn r)
           match-path (list* :matches path)]
       (if (= v (get-in m (concat match-path [::v])))
         m
         (if (nil? v)
           (dissoc-in m match-path)
           (-> m
               (update-in match-path (fnil assoc {}) ::v v)
               (update-in (result-path m path) (fnil conj [])
                          (apply-mappings mappings (select-keys r ks))))))))
   m (:matchers m)))

(defn- matchers
    "Returns collection of matchers for m assoc'ing ::path along the way.
     Depth-first traversal along :children"
    [m]
    (loop [m (assoc m ::path [(:row-key m)])
           stack [] result []]
      (let [result (conj result (dissoc m :children))
            stack (into stack (and (-> m :children seq)
                                   (->> m :children rseq
                                        (map (fn [child]
                                               (assoc child ::path (conj (::path m) (:row-key child))))))))]
        (if (empty? stack)
          result
          (recur (peek stack) (pop stack) result)))))

(defn reduce-rows
  "Reduce rows using m as a template.

  m contains nested maps with the following keys:
  :row-key
    Key to use in result map.
  :match-val-fn
    fn to match against a row.
  :ks
    coll of keys to select into the result for matching rows.
  :mappings
    mappings to apply against values of ks for result."
  [m rows]
  (:result (reduce reduce-row {:matchers (matchers m)} rows)))

(comment
  (println (reduce-rows {:row-key :as :match-val-fn :a :ks [:a]
                         :children [{:row-key :bs :match-val-fn :b :ks [:b]}
                                    {:row-key :cs :match-val-fn :c :ks [:c]}]}
                        [{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 1} {:a 2 :c 3}]))
  
  (println (reduce-rows [{:path [:as] :match-val-fn :a :row-key :as :ks [:a]}
                         {:path [:as :bs] :match-val-fn :b :row-key :bs :ks [:b]}
                         {:path [:as :cs] :match-val-fn :c :row-key :cs :ks [:c]}]
                        [{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 1} {:a 2 :c 3}]))
  
  (apply-mappings [(fn [m] (update-in m [:a] inc))] {:a 1})
  (apply-mappings nil {:a 1})
  (apply-mappings [(fn [m] (update-in m [:a] inc))] [{:a 1} {:a 11}])

  (apply-mappings [(make-mapping inc :a)] {:a 1})
  (apply-mappings [(make-mapping inc :a :b)] [{:a 1 :b 2} {:a 11 :b 22}])

  (make-mappings [:a :b] inc dec)

  (def m {:row-key :as :match-val-fn :a :ks [:a]
          :children [{:row-key :bs :match-val-fn :b :ks [:b]
                      :children [{:row-key :bbs}
                                 {:row-key :bb1s}]}
                           {:row-key :cs :match-val-fn :c :ks [:c]}]})

  

  (matchers m)
)