(ns clj-sql-mapper.rhandler
  (:use hiccup.core hiccup.page hiccup.form
        [clojure.string :only [upper-case]]
        [bultitude.core :only [namespaces-on-classpath]])
  (:require compojure.core))

(defonce ^{:doc "Routes and handlers defined by defrh."} routes (atom {}))

(defn wrap-rhandler
  "Middleware that dispatches requests to handlers defined by defrh.
   If there is no matching handler, the handler argument is called.
   Also takes ns-syms and requires them and their children."
  [handler & ns-syms]
  (doseq [sym ns-syms
          f (namespaces-on-classpath :prefix (name sym))]
    (require f))
  (fn [req]
    (if-let [resp (some (fn [[_ h]] (h req)) @routes)]
      resp
      (handler req))))

(defn route->key [method route]
  (->> (if (string? route) [route] route) (list* method) (clojure.string/join \space)))

(defmacro defrh
  "Adds a route handler to be dispatched by wrap-rhandler.
   
   Supported forms:

   (defrh \"/foo/:id\" [id]) an unnamed route
   (defrh :post \"foo/:id\" [id]) a route that responds to POST
   (defrh foo \"/foo/:id\" [id]) a named route
   (defrh foo :post \"/foo/:id\" [id]) 

   The default method is :get"
  [& args]
  (let [[fn-name method route bindings & body] (cond
                                                (-> args second keyword?) args
                                                (-> args first symbol?) (list* (first args) :get (rest args))
                                                (-> args first keyword?) (list* nil args)
                                                :else (list* nil :get args))
        k (route->key method route)
        method (symbol "compojure.core" (-> method name upper-case))]
    (if fn-name
      `(do
         (defn ~fn-name [req#] (compojure.core/let-request [~bindings req#] ~@body))
         (swap! routes assoc ~k (~method ~route ~bindings #(~fn-name %))))
      `(swap! routes assoc ~k (~method ~route ~bindings ~@body)))))

(comment
  (reset! routes {})
  (doseq [s ["a" "b"]]
    (defrh (str "/gen-" s) [] s))
  
  (defrh "/form" [] (html (form-to [:post "/form"] (submit-button "submit"))))
  (defrh :post "/form" [] "posted-1")
  (defrh path  "/path/:id" [id] (str "path v1: id:" id))
  (defrh "/path/:id" [id] (str "path v2: id:" id))
  (defrh ["/user/:id" :id #"[0-9]+"] [id] (str "user v1: id: " id))
  (defrh ["/user/:id" :id #"[0-9]+"] [id] (str "user v2: id: " id))
  
  (route->key :get "/path/:id")
  (route->key :post ["/path/:id" :id #"[0-9]+"])
  (route->key :any "/path")
  )