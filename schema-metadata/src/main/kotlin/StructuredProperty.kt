package org.khanacademy.metadata

/**
 * A nested entity-like property that flattens out its values for querying.
 *
 * This is for compatibility with ndb.StructuredProperty in python.
 * See https://cloud.google.com/appengine/docs/standard/python/ndb/entity-property-reference#structured
 */
class StructuredProperty<T : Any>(val value: T)
