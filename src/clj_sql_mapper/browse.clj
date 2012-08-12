(ns clj-sql-mapper.browse
  (:use [clojure.java.browse :only [browse-url]]
        [hiccup.core :only [html]]))

(defn- html-row [ks m]
  [:tr (map #(vector :td (m %)) ks)])

(defn- html-table [coll]
  (when-let [m (first coll)]
    (let [ks (->> m keys (apply sorted-set))]
      (html
       [:table {:border 1}
        [:tr
         (map #(vector :th %) ks)]
        (map (partial html-row ks) coll)]))))

(defn browse
  "Browse a coll of maps as an html table in a browser."
  [coll]
  (when-let [html (html-table coll)]
    (let [file (java.io.File/createTempFile "clj-sql-mapper-table" ".html")]
      (.deleteOnExit file)
      (spit file html)
      (browse-url (str "file:///" (.getAbsolutePath file))))))

; (browse [{:b 2 :c 3 :a 1} {:b 22 :c 33 :a 11}])
