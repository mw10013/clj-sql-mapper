(ns clj-sql-mapper.test.rhandler
  (:require [clj-sql-mapper.rhandler :as rh])
  (:use clojure.test
        [ring.mock.request :only [request]]))

(deftest rh
  (reset! rh/routes {})
  (let [handler (rh/wrap-rhandler identity)]
    (rh/defrh "/no-name-no-method" [] "no-name-no-method")
    (rh/defrh :post "/no-name" [] "no-name")
    (rh/defrh name-no-method "/name-no-method" [] "name-no-method")
    (rh/defrh name-and-method :post "/name-and-method" [] "name-and-method")
    (is (= "no-name-no-method" (:body (handler (request :get "/no-name-no-method")))))
    (is (= "no-name" (:body (handler (request :post "/no-name")))))
    (is (= "name-no-method" (:body (handler (request :get "/name-no-method")))))
    (is (= "name-and-method" (:body (handler (request :post "/name-and-method")))))
    (is (= "name-no-method" (name-no-method {})))
    (is (= "name-and-method" (name-and-method {})))))

(deftest redefine-rh
  (reset! rh/routes {})
  (let [handler (rh/wrap-rhandler identity)]
    (rh/defrh "/get" [] "get")
    (is (= "get" (:body (handler (request :get "/get")))))
    (rh/defrh "/get" [] "get redefined")
    (is (= "get redefined" (:body (handler (request :get "/get")))))))

(deftest post-and-named-get-rh
  (reset! rh/routes {})
  (let [handler (rh/wrap-rhandler identity)]
    (rh/defrh rh-handler "/rh-handler" [] "rh-handler")
    (rh/defrh :post "/rh-handler" {:as req} (rh-handler req))
    (is (= "rh-handler" (:body (handler (request :get "/rh-handler")))))
    (is (= "rh-handler" (:body (handler (request :post "/rh-handler")))))))

(deftest regex-rh
  (reset! rh/routes {})
  (let [handler (rh/wrap-rhandler identity)]
    (rh/defrh rh-handler ["/rh-handler/:id"  :id #"\d+"] [id] (str "rh-handler: " id))
    (is (= "rh-handler: 1" (:body (handler (request :get "/rh-handler/1")))))
    (rh/defrh rh-handler ["/rh-handler/:id"  :id #"\d+"] [id] (str "rh-handler-redefined: " id))
    (is (= "rh-handler-redefined: 1" (:body (handler (request :get "/rh-handler/1")))))))

; (run-tests 'clj-sql-mapper.test.rhandler)