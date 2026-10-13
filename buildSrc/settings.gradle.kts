// buildSrc is a build of its own (LLD-api-stability §9.1): it declares its repositories, and reads the
// root's version catalog so its libraries stay on the versions the modules use.
dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // The japicmp Gradle plugin is published on the Plugin Portal only; japicmp itself comes from Maven
        // Central like everything else.
        gradlePluginPortal {
            content {
                includeGroup("me.champeau.gradle")
            }
        }
    }
    versionCatalogs {
        create("libs") {
            from(files("../gradle/libs.versions.toml"))
        }
    }
}

rootProject.name = "buildSrc"
