
plugins {
    java
    id("com.google.protobuf") version "0.9.4"
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
    implementation("de.mirkosertic.bytecoder:bytecoder.api:2024-05-10")
}

sourceSets {
    main {
        proto {
            setSrcDirs(listOf("../../../../../transaction/src/main/protobuf"))
        }
        java {
            setSrcDirs(listOf("src/main/java"))
        }
    }
}

tasks.jar {
    manifest.attributes["Main-Class"] = "user.UserMain"
    val dependencies = configurations
            .runtimeClasspath
            .get()
            .map(::zipTree) // OR .map { zipTree(it) }
    from(dependencies)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
