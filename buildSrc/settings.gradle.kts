// buildSrc is a build of its own (LLD-api-stability §9.1): it declares its repository, and reads the
// root's version catalog so its test libraries stay on the versions the modules use.
dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
    versionCatalogs {
        create("libs") {
            from(files("../gradle/libs.versions.toml"))
        }
    }
}

rootProject.name = "buildSrc"
