(ns clj-sql-mapper.validate
  "Validate a map with apologies to https://github.com/weavejester/valip")

(defn- validate
  "Validates the map m against the rule.
   A rule is [key predicate error-string] or
   [val-fn key predicate error-string].
   If predicate fails returns {key [error] otherwise nil.

   If key is a collection, the collection should contain rules
   and the first invalid rule returns an error map."
  ([m rule]
     (if (-> rule first coll?)
       (->> rule (map (partial validate m)) (remove nil?) first)
       (apply validate m rule)))
  ([m k pred? error] (validate m k k pred? error))
  ([m val-fn k pred? error]
     (when-not (-> m val-fn pred?)
       {k [error]})))

(defn invalid?
  "If the map m is invalid against rules, return a map of errors otherwise nil.
   A rule is [key predicate error]. Error map is {key [error]"
  [m & rules]
  (->> rules
       (map (partial validate m))
       (apply merge-with into)))
