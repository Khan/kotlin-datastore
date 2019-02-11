description = """
Library for annotating data classes for use as datastore models.

This subproject is independent of the google cloud client libraries, and could
be used to represent schema metadata for other uses as well.
"""

group = "org.khanacademy"
version = "0.0.1"

repositories {
    jcenter()
}

plugins {
    kotlin("jvm")
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile(kotlin("reflect"))
}
