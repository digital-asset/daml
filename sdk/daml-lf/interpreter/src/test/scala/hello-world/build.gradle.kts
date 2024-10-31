
plugins {
    scala
    id("com.google.protobuf") version "0.9.4"
    id("org.teavm") version "0.10.2"
}

group = "com.digitalasset.daml.lf"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    teavm("com.google.protobuf:protobuf-java:4.29.0-RC1")
    teavm("org.scala-lang:scala-library:2.13.14")
    teavm("org.scala-lang:scala-reflect:2.13.14")
    teavm(teavm.libs.interop)
}

protobuf {
    protoc {
        setPath("/Users/carlpulley/Downloads/protoc-29.0-rc-1-osx-aarch_64/bin/protoc")
    }
}

sourceSets {
    main {
        proto {
            setSrcDirs(listOf("../../../../../transaction/src/main/protobuf"))
        }
        scala {
            setSrcDirs(listOf("src/main/scala"))
        }
    }
}

teavm.wasm {
    // mainClass is used by teaVM to determine code slice that will be compiled
    mainClass = "user.UserMain"

    debugInformation = true
}

tasks.named("assemble") {
    dependsOn(tasks.named("generateWasm"))
}
