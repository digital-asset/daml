# Mock scala/codegen example (does not send any commands just prints them to STD OUT)
$ sbt -DDA.sdkVersion=100.12.6 compile "mock-example/runMain com.digitalasset.example.ExampleMain"

# Sandbox scala/codegen example (sends commands to sandbox and receives transactions)
$ sbt -DDA.sdkVersion=100.12.6 "sandbox-example/runMain com.digitalasset.example.ExampleMain ./scala-codegen/target/repository/daml-codegen/Main.dar"
