/**
 * Helper classes for running untyped datastore queries.
 *
 * In the future we'll use annotation processors to generate query builders,
 * which will become the preferred interface for querying.
 *
 * TODO(colin): implement ancestors, ordering of results, query cursors,
 * offset.
 */

package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StructuredQuery.PropertyFilter
import org.khanacademy.metadata.Key

sealed class QueryFilter {
    internal abstract fun toDatastoreFilter(): PropertyFilter
}

enum class FieldCondition {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
}

/**
 * Basic filter on a single property by equality or inequality.
 */
data class FieldQueryFilter(
    val fieldName: String,
    val condition: FieldCondition,
    val value: Any?
) : QueryFilter() {
    internal override fun toDatastoreFilter(): PropertyFilter =
        // This function is pretty gross because the datastore client library
        // uses no generics or abstraction around filter types for this at all.
        // We therefore have to list out all the options manually in order to
        // be able to call the correct overload of the function. There are a
        // lot of them.
        when (val datastoreValue = toDatastoreType(value)) {
            is Blob -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Boolean -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Double -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Long -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is String -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Timestamp -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is DatastoreKey -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            null -> when (condition) {
                FieldCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, NullValue())
                FieldCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, NullValue())
                FieldCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, NullValue())
                FieldCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, NullValue())
                FieldCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, NullValue())
            }
            // TODO(colin): implement querying on other supported property
            // types. See EntityConversion.kt for a more comprehensive list.
            else -> throw IllegalArgumentException(
                "Unable to query on property $fieldName " +
                "(value $datastoreValue)")
        }
}

data class AncestorQueryFilter(val value: Key<*>) : QueryFilter() {
    internal override fun toDatastoreFilter(): PropertyFilter =
        PropertyFilter.hasAncestor(value.toDatastoreKey())
}

/**
 * Builder DSL for (non-type-safe) queries.
 *
 * @see Datastore.query for more complete documentation and usage examples.
 */
class QueryFilterBuilder {
    var properties: List<Property> = listOf()

    fun fieldProperty(name: String): Property.FieldProperty {
        val newProperty = Property.FieldProperty(name)
        properties += newProperty
        return newProperty
    }

    fun build(): List<QueryFilter> {
        return properties.map { it.toQueryFilter() }
    }

    infix fun String.eq(other: Any?) {
        fieldProperty(this) eq other
    }

    infix fun String.lt(other: Any?) {
        fieldProperty(this) lt other
    }

    infix fun String.gt(other: Any?) {
        fieldProperty(this) gt other
    }

    infix fun String.le(other: Any?) {
        fieldProperty(this) le other
    }

    infix fun String.ge(other: Any?) {
        fieldProperty(this) ge other
    }

    // TODO(benkraft): This should really also accept a List<KeyPathElement>,
    // since the ancestor key need not be the key of a real model-kind but a
    // Key<*> does.  In practice, we usually use a real kind (although not
    // necessarily a real key) so this is fine, and List<KeyPathElement> is a
    // bit icky anyway.
    fun hasAncestor(key: Key<*>) {
        val newProperty = Property.AncestorProperty()
        properties += newProperty
        newProperty hasAncestor key
    }

    companion object {
        sealed class Property {
            internal abstract fun toQueryFilter(): QueryFilter

            // TODO(benkraft): It might be cleaner to have these as siblings of
            // Property, but apparently subclasses of sealed classes on the
            // companion object still need to be members of the parent class
            // rather than merely in the same file.  Perhaps a kotlin bug?
            class FieldProperty(private val name: String) : Property() {
                var operation: FieldCondition? = null
                var filterValue: Any? = null

                // We have to use `eq` instead of == because of
                // https://youtrack.jetbrains.com/issue/KT-29316
                // which will be fixed in an upcoming kotlin release.
                infix fun eq(other: Any?) {
                    operation = FieldCondition.EQUAL
                    filterValue = other
                }

                infix fun lt(other: Any?) {
                    operation = FieldCondition.LESS_THAN
                    filterValue = other
                }

                infix fun gt(other: Any?) {
                    operation = FieldCondition.GREATER_THAN
                    filterValue = other
                }

                infix fun le(other: Any?) {
                    operation = FieldCondition.LESS_THAN_OR_EQUAL
                    filterValue = other
                }

                infix fun ge(other: Any?) {
                    operation = FieldCondition.GREATER_THAN_OR_EQUAL
                    filterValue = other
                }

                internal override fun toQueryFilter(): QueryFilter {
                    return FieldQueryFilter(
                        name,
                        operation ?: throw IllegalArgumentException(
                            "You must provide an operation in a query."),
                        filterValue
                    )
                }
            }

            class AncestorProperty() : Property() {
                var filterValue: Key<*>? = null

                infix fun hasAncestor(key: Key<*>) {
                    filterValue = key
                }

                internal override fun toQueryFilter(): QueryFilter {
                    return AncestorQueryFilter(
                        filterValue ?: throw IllegalArgumentException(
                            "You must provide an operation in a query."))
                }
            }
        }
    }
}
