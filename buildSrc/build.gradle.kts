plugins {
    java
}

dependencies {
    implementation(gradleApi())
    // The task type of checkJavaApi and the API of its rich-report rules (LLD-api-stability §4). Being on
    // buildSrc's runtime classpath puts the plugin on every build script's classpath at this version.
    implementation(libs.japicmp.gradle.plugin)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
    // japicmp's report filter needs Guava, which the plugin leaves out of its dependencies and adds to its
    // worker's classpath at this version; the tests run the worker's code in their own JVM.
    testRuntimeOnly(libs.japicmp.worker.guava)
}

// This code runs inside the Gradle daemon, whose JVM is the one that launched the build: 17 in CI, possibly
// newer locally. Compiling for 17 keeps it loadable by every JVM the build supports.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
    options.compilerArgs.add("-Xlint:all")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

// Gradle builds buildSrc's jar for every invocation of the main build but never runs its tests, and no task of
// the main build can depend on a buildSrc task. Making the jar depend on them runs them wherever the build
// logic changed, `check` in CI included (LLD-api-stability §9.1).
tasks.named("jar") {
    dependsOn(tasks.named("test"))
}
