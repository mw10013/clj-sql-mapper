(ns clj-sql-mapper.browse
  (:use [clojure.java.browse :only [browse-url]]
        [hiccup [core :only [html]] [page :only [include-css]]]
        [clojure.data.xml :only [sexp-as-element emit]])
  (:require [clojure.data.xml :as xml]))

(defn- html-row [ks m]
  [:tr (map #(vector :td (m %)) ks)])

(defn- html-table [ks rs]
  (when-let [m (first rs)]
    (let [ks (or ks (->> m keys (apply sorted-set)))]
      (html
       [:head
        (include-css "http://twitter.github.com/bootstrap/assets/css/bootstrap.css")]
       [:div.container-fluid
        [:p "&nbsp;"]
        [:table.table.table-bordered.table-striped.table-condensed
         [:thead
          [:tr
           (map #(vector :th %) ks)]]
         [:tbody
          (map (partial html-row ks) rs)]]]))))

(defn browse-resultset
  "Browse a rs of maps as an html table in a browser.
   ks specifies keys to display and may be nil, which
   will display all keys."
  ([rs] (browse-resultset nil rs))
  ([ks rs]
     (when-let [html (html-table ks rs)]
       (let [file (java.io.File/createTempFile "clj-sql-mapper-table" ".html")]
         (.deleteOnExit file)
         (spit file html)
         (-> file .toURI str browse-url)))))

(defn- as-xml-sexp [tag x]
  (cond
   (map? x) (into [tag] (map (fn [[k v]] (as-xml-sexp k v)) x))
   (coll? x) (into [tag] (map #(as-xml-sexp :item %) x))
   :else [tag x]))

(defn browse-map
  "Browse a map as an xml doc in a browser."
  [m]
  (let [xml (->> m (as-xml-sexp :root) sexp-as-element)
        file (java.io.File/createTempFile "clj-sql-mapper-map" ".xml")]
    (.deleteOnExit file)
    (with-open [writer (clojure.java.io/writer file)]
      (emit xml writer))
    (-> file .toURI str browse-url)))

(comment
  (browse-map {:a 1 :b 2})
  (browse-map {:a 1 :children [{:c 3} {:d 4}]})

  (browse-resultset [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
  (browse-resultset [:b :a] [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
  )


