# kotlin-datastore

A high-level library for accessing Google cloud datastore from kotlin / jvm.

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

# Status

This is in very early stages and is not yet functional code.

# License

MIT, see LICENSE file.
