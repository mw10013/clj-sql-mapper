(ns clj-sql-mapper.browse
  (:use [clojure.java.browse :only [browse-url]]
        [hiccup [core :only [html]] [page :only [include-css]]])
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
          (browse-url (str "file:///" (.getAbsolutePath file)))))))

(comment
  (browse-resultset [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
  (browse-resultset [:b :a] [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
  (xml/sexp-as-element [:root {:a 1 :b 2} [:parents "parents"]])
  (println
   (xml/indent-str
    (xml/sexp-as-element
     [:root {:a 1 :b 2}
      [:parents
       [:parent {:id 1}
        [:children [:child {:id 3}] [:child {:id 4}]]]
       [:parent {:id 2}
        [:children [:child {:id 5}] [:child {:id 6}]]]]])))
  )


