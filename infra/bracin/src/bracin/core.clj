(ns bracin.core
  (:require [ring.adapter.jetty :as jetty])
  (:gen-class))

(defn -main
  [& args]
  (jetty/run-jetty
    (fn [req]
      {:status 200,
       :body "Hello, world."})
    {:port 3000
     :join? true}))
