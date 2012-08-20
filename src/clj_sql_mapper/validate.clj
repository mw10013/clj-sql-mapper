(ns clj-sql-mapper.validate
  "Validate a map with apologies to https://github.com/weavejester/valip")

(defn- validate
  "Validates the map m against the rule.
   A rule is [key predicate error-string]. If predicate fails
   returns {key [error] otherwise nil.

   If key is :*, passes m into predicate.

   If key is a collection, the collection should contain rules
   and the first invalid rule returns an error map."
  [m [k pred? error :as rule]]
  (if (coll? k)
    (->> rule (map (partial validate m)) (remove nil?) first)
    (when-not (pred? (if (= k :*) m (k m)))
      {k [error]})))

(defn invalid?
  "If the map m is invalid against rules, return a map of errors otherwise nil.
   A rule is [key predicate error]. Error map is {key [error]"
  [m & rules]
  (->> rules
       (map (partial validate m))
       (apply merge-with into)))
