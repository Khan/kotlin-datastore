description = """
Library for Google cloud datastore access using annotated data classes as
the in-code representation of entities.
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
    compile(project(":schema-metadata"))
}
