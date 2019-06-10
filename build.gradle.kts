
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

description = """
Umbrella project for Khan Academy's datastore access library.

This project is organized into two subprojects (which are packaged into
independent artifacts):

1. schema-metadata
    This contains annotations and types for defining datastore models as kotlin
    data classes. Everything in here is independent of the Google cloud client
    libraries and could be used for expressing schemas for other databases as
    well.

2. google-cloud-datastore
    Code for using data classes defined within the schema-metadata package as
    Google cloud datastore entities. Also contains a default test stub.
"""

group = "org.khanacademy"
version = "0.1.5"

repositories {
    jcenter()
}

plugins {
    kotlin("jvm") version "1.3.11"
    id("org.jetbrains.dokka") version "0.9.17"
}

val kotlinVersion = "1.3.11"

subprojects {
    configurations {
        // Even though these are built-in configurations, they won't exist for
        // subprojects at the time this file is evaluated, so we need to create
        // them.
        create("compile")
        create("testCompile")
    }
}

allprojects {
    tasks.withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }

    tasks.withType<Test> {
        // By default, Gradle won't run the tests if the code hasn't changed;
        // ensure we actually run tests whenever we're asked.
        outputs.upToDateWhen { false }

        testLogging {
            quiet {
                events = setOf(
                    TestLogEvent.FAILED
                )
                exceptionFormat = TestExceptionFormat.FULL
            }
            events = setOf(
                TestLogEvent.FAILED,
                TestLogEvent.PASSED,
                TestLogEvent.SKIPPED,
                TestLogEvent.STANDARD_OUT,
                TestLogEvent.STANDARD_ERROR
            )
            exceptionFormat = TestExceptionFormat.FULL
        }
        useJUnitPlatform()
    }

    dependencies {
        constraints {
            // TODO(colin): use a kotlin "bill of materials" when one is released
            compile(kotlin("stdlib-jdk8", kotlinVersion))
            compile(kotlin("reflect", kotlinVersion))
        }
        testCompile("io.kotlintest:kotlintest-runner-junit5:3.1.11")
    }
}

evaluationDependsOnChildren()

tasks {
    dokka {
        moduleName = rootProject.name
        outputFormat = "html"
        outputDirectory = "$buildDir/docs"
        jdkVersion = 8
        sourceDirs = subprojects.flatMap {
            it.sourceSets.main.get().allSource.sourceDirectories
        }
    }
}
