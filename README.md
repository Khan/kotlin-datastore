# kotlin-datastore

A high-level library for accessing Google cloud datastore from kotlin / jvm.

Datastore models are represented as plain data classes. Datastore access goes
through a global singleton object `DB`. The general design is heavily
influenced by the python db and ndb datastore client libraries.

[Work-in-progress library documentation](https://khan.github.io/kotlin-datastore/docs/index.html)

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

This is currently mostly feature complete and is being used in production,
but we're still numbering releases as pre-1.0 for now to allow some greater
flexibility for iterating on the public API as we start to use it more.

Functionality planned prior to a first release:

- [x] Datastore get-by-key
- [x] Transactions
- [x] Asynchronous operation (via kotlin coroutines)
- [x] Datastore queries (via a `Map`-like interface)
- [x] Datastore writes
- [x] multi-get and -put
- [x] Use of plain kotlin data classes to represent datstore models
- [x] Reflective implementation of converting Google datastore client `Entity`s to data class instances
- [x] Reflective implementation of converting data class instances to Google datastore client `Entity`s
- [x] Annotations for assigning a different in-datastore name to a property
- [x] Annotations for choosing whether to index a property
- [x] Primitive, nonrepeated properties
- [x] Repeated properties
- [x] Nested entity properties
- [x] Timestamp properties
- [x] Key properties
- [x] Location properties
- [x] Json properties
- [x] Computed properties
- [x] Other miscellaneous property types (in general we plan to support equivalents of the
      standard ndb property types at
      https://cloud.google.com/appengine/docs/standard/python/ndb/entity-property-reference#properties_and_value_types)
- [x] Backend using the google-cloud-java datastore client
- [x] Test stub backend

Functionality planned after a first release:

- [ ] Independent transaction propagation mode
- [ ] Nontransactional context to escape from an ongoing transaction
- [ ] Annotation-processor-based generation of nonreflective converters to/from `Entity`
- [ ] Annotation-processor-based typed query builders
- [ ] Generation of bigquery schemas from model data classes

# License

MIT, see LICENSE file.
