plugins {
    java
    id("com.google.protobuf") version "0.9.4"
    kotlin("multiplatform") version "2.0.21"
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
    implementation("com.google.protobuf:protobuf-java:3.8.0")
}

sourceSets {
    main {
        proto {
            srcDir("../../../../../transaction/src/main/protobuf")
        }
        kotlin {
            srcDirs.set(listOf("src/main/kotlin"))
        }
    }
}

kotlin {
    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
    wasmJs {
        platformType.set(wasm)
    }
}
