(ns clj-sql-mapper.browse
  (:use [clojure.java.browse :only [browse-url]]
        [hiccup [core :only [html]] [page :only [include-css]]])
  (:require [clojure.data.xml :as xml]))

(defn- html-row [ks m]
  [:tr (map #(vector :td (m %)) ks)])

(defn- html-table [coll]
  (when-let [m (first coll)]
    (let [ks (->> m keys (apply sorted-set))]
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
          (map (partial html-row ks) coll)]]]))))

(defn browse
  "Browse a coll of maps as an html table in a browser."
  [coll]
  (when-let [html (html-table coll)]
    (let [file (java.io.File/createTempFile "clj-sql-mapper-table" ".html")]
      (.deleteOnExit file)
      (spit file html)
      (browse-url (str "file:///" (.getAbsolutePath file))))))

; (println (xml/indent-str (xml/as-elements [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])))
; (println (xml/indent-str (xml/as-elements {:b 2 :c 3 :a 1})))

(comment
  (browse [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
  )


