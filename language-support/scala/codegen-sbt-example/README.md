# Mock scala/codegen example (does not send any commands just prints them to STD OUT)
$ sbt "mock-example/runMain com.digitalasset.example.ExampleMain"

# Sandbox scala/codegen example (sends commands to sandbox and receives transactions)
$ sbt "sandbox-example/runMain com.digitalasset.example.ExampleMain ./scala-codegen/target/repository/daml-codegen/Main.dar"