(ns bracin.core
  (:require [ring.adapter.jetty :as jetty])
  (:gen-class))

(defn handler
  []
  (fn [req] {:status 200, :body "Hello, world."}))

(defn -main
  [& args]
  (jetty/run-jetty (handler)
                   {:port 3000
                    :join? true}))
