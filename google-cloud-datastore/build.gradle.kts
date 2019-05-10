description = """
Library for Google cloud datastore access using annotated data classes as
the in-code representation of entities.
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
    compile(project(":schema-metadata"))
    compile("com.google.cloud:google-cloud-datastore:1.62.0")
    compile("com.nhaarman.mockitokotlin2:mockito-kotlin:2.1.0")
    compile("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.1")
}

tasks {
    test {
        environment["DATASTORE_ENV"] = "TEST"
        environment["DATASTORE_PROJECT"] = "khan-academy-kotlin-datastore~test"
    }
}

sourceSets {
    test {
        resources {
            // We keep the file pointing the ServiceLoader to our internal test
            // stub in a nonstandard location. Since we have two test stubs,
            // one for testing this library itself and another for testing
            // client code that uses this library, we want to make sure that
            // client code can't accidentally pull in the internal stub because
            // it's on the default resource path for this package.
            srcDir("src/internal-resources")
        }
    }
}

val toplevelDescription = description
publishing {
    publications {
        create<MavenPublication>("kotlin-datastore") {
            artifactId = "kotlin-datastore"
            from(components["java"])
            pom {
                name.set("kotlin-datstore")
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
