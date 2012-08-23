(ns clj-sql-mapper.test.validate
  (:require [clj-sql-mapper.validate :as v])
  (:use clojure.test))

(deftest validate
  (is (not (v/invalid? {:a "val"} [:a (complement clojure.string/blank?) "Required."])))
  (is (= {:a ["Required."]} (v/invalid? {:a ""} [:a (complement clojure.string/blank?) "Required."])))
  (is (not (v/invalid? {:a "val" :b "val"}
                       [:a (complement clojure.string/blank?) "Required."]
                       [:b (complement clojure.string/blank?) "Required."])))
  (is (= {:b ["Required."]} (v/invalid? {:a "val" :b ""}
                                        [:a (complement clojure.string/blank?) "Required."]
                                        [:b (complement clojure.string/blank?) "Required."])))
  (is (= {:password ["Required."]} (v/invalid? {:password "" :confirm-password ""}
                                               [[:password (complement clojure.string/blank?) "Required."]
                                                [:confirm-password (complement clojure.string/blank?) "Required."]])))
  (is (= {:confirm-password ["Required."]} (v/invalid? {:password "secret" :confirm-password ""}
                                                       [[:password (complement clojure.string/blank?) "Required."]
                                                        [:confirm-password (complement clojure.string/blank?) "Required."]])))
  (is (= {:* ["Passwords must match."]}
         (v/invalid? {:password "secret" :confirm-password "secret-1"}
                     [[:password (complement clojure.string/blank?) "Required."]
                      [:confirm-password (complement clojure.string/blank?) "Required."]
                      [identity :* #(apply = (map % [:password :confirm-password])) "Passwords must match."]])))
  (is (not (v/invalid? {:password "secret" :confirm-password "secret"}
                       [[:password (complement clojure.string/blank?) "Required."]
                        [:confirm-password (complement clojure.string/blank?) "Required."]
                        [identity :* #(apply = (map % [:password :confirm-password])) "Passwords must match."]])))
  (is (= {:a ["Required."]}
         (v/invalid? {:password "secret" :confirm-password "secret" :a ""}
                     [[:password (complement clojure.string/blank?) "Required."]
                      [:confirm-password (complement clojure.string/blank?) "Required."]
                      [identity :* #(apply = (map % [:password :confirm-password])) "Passwords must match."]]
                     [:a (complement clojure.string/blank?) "Required."]))))