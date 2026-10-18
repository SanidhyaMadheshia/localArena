// Kafka Streams processors for LocalArena (Java 21). Only stream processing
// that needs the Kafka Streams JVM library lives here; everything else is Go.
plugins {
    application
    id("com.google.protobuf") version "0.10.0"
}

repositories { mavenCentral() }

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

val kafkaVersion = "4.4.0"
val protobufVersion = "4.36.2"

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.20")
    // Security pin (Trivy, Experiment 005): kafka-clients brings lz4-java 1.11.2 (HIGH CVE).
    constraints { implementation("at.yawk.lz4:lz4-java:1.11.4") { because("CVE-2026-106451") } }

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Contracts are shared with Go: generate from the repository's schemas/ directory.
sourceSets { main { proto { srcDir("../schemas") } } }
protobuf { protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" } }

application { mainClass = "io.localarena.streams.Main" }

tasks.test {
    useJUnitPlatform()
    testLogging { events("passed", "failed"); showStandardStreams = false }
}
