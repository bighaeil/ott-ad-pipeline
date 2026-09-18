// Collector 와 동일 버전 조합. 올릴 때는 둘 다 같이 올릴 것.
//   Kotlin 2.1.0 / Spring Boot 3.4.1 / Java 21
//
// Collector 는 WebFlux 지만 이쪽은 일부러 blocking Web MVC + JDBC 다.
// Outbox 는 "소재 결정과 이벤트 INSERT 가 한 트랜잭션" 인 것이 전부인데,
// R2DBC 반응형 트랜잭션으로 가면 그 핵심이 배관에 묻힌다.
plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.spring") version "2.1.0"
    id("org.springframework.boot") version "3.4.1"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.ottads"
version = "0.1.0"

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

repositories { mavenCentral() }

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("io.micrometer:micrometer-registry-prometheus")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    runtimeOnly("org.postgresql:postgresql")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.named<org.springframework.boot.gradle.tasks.bundling.BootJar>("bootJar") {
    archiveFileName.set("ad-decision.jar")
}
