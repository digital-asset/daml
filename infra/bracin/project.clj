(defproject bracin "app"
  :dependencies [[org.clojure/clojure "1.11.1"]]
  :main ^:skip-aot bracin.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
