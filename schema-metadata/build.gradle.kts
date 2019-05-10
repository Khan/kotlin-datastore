description = """
Library for annotating data classes for use as datastore models.

This subproject is independent of the google cloud client libraries, and could
be used to represent schema metadata for other uses as well.
"""

group = "org.khanacademy"
version = "0.1.2"

repositories {
    jcenter()
}

plugins {
    kotlin("jvm")
    `maven-publish`
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile(kotlin("reflect"))
    compile("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.8")
}

val toplevelDescription = description
publishing {
    publications {
        create<MavenPublication>("schema-metadata") {
            artifactId = "schema-metadata"
            from(components["java"])
            pom {
                name.set("schema-metadata")
                description.set(toplevelDescription)
                url.set("https://github.com/khan/kotlin-datastore")
                licenses {
                    license {
                        name.set("The MIT License (MIT)")
                        url.set(
                            "https://github.com/Khan/kotlin-datastore/blob/" +
                                "master/LICENSE")
                    }
                }
            }
        }
    }

    repositories {
        maven {
            url = uri("gcs://ka-maven-repository/maven2")
        }
    }
}
