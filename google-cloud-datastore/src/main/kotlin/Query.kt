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
import com.google.cloud.datastore.StructuredQuery.PropertyFilter

enum class QueryFilterCondition {
    EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
}

/**
 * Basic filter on a single property by equality or inequality.
 */
data class QueryFilter(
    val fieldName: String,
    val condition: QueryFilterCondition,
    val value: Any?
) {
    internal fun toDatastoreFilter(): PropertyFilter =
        // This function is pretty gross because the datastore client library
        // uses no generics or abstraction around filter types for this at all.
        // We therefore have to list out all the options manually in order to
        // be able to call the correct overload of the function. There are a
        // lot of them.
        when (val datastoreValue = toDatastoreType(value)) {
            is Blob -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Boolean -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Double -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Long -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is String -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is Timestamp -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            is DatastoreKey -> when (condition) {
                QueryFilterCondition.EQUAL ->
                    PropertyFilter.eq(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN ->
                    PropertyFilter.lt(fieldName, datastoreValue)
                QueryFilterCondition.LESS_THAN_OR_EQUAL ->
                    PropertyFilter.le(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN ->
                    PropertyFilter.gt(fieldName, datastoreValue)
                QueryFilterCondition.GREATER_THAN_OR_EQUAL ->
                    PropertyFilter.ge(fieldName, datastoreValue)
            }
            // TODO(colin): implement querying on other supported property
            // types. See EntityConversion.kt for a more comprehensive list.
            else -> throw IllegalArgumentException(
                "Unable to query on property $fieldName")
        }
}

/**
 * Builder DSL for (non-type-safe) queries.
 *
 * @see Datastore.query for more complete documentation and usage examples.
 */
class QueryFilterBuilder {
    var properties: List<Property> = listOf()

    fun property(name: String): Property {
        val newProperty = Property(name)
        properties += newProperty
        return newProperty
    }

    fun build(): List<QueryFilter> {
        return properties.map { it.toQueryFilter() }
    }

    infix fun String.eq(other: Any?) {
        property(this) eq other
    }

    infix fun String.lt(other: Any?) {
        property(this) lt other
    }

    infix fun String.gt(other: Any?) {
        property(this) gt other
    }

    infix fun String.le(other: Any?) {
        property(this) le other
    }

    infix fun String.ge(other: Any?) {
        property(this) ge other
    }

    companion object {
        class Property(private val name: String) {
            var operation: QueryFilterCondition? = null
            var filterValue: Any? = null
            override operator fun equals(other: Any?): Boolean {
                operation = QueryFilterCondition.EQUAL
                filterValue = other

                return super.equals(other)
            }

            // We have to use `eq` instead of == because of
            // https://youtrack.jetbrains.com/issue/KT-29316
            // which will be fixed in an upcoming kotlin release.
            infix fun eq(other: Any?): Boolean {
                return this.equals(other)
            }

            infix fun lt(other: Any?) {
                operation = QueryFilterCondition.LESS_THAN
                filterValue = other
            }

            infix fun gt(other: Any?) {
                operation = QueryFilterCondition.GREATER_THAN
                filterValue = other
            }

            infix fun le(other: Any?) {
                operation = QueryFilterCondition.LESS_THAN_OR_EQUAL
                filterValue = other
            }

            infix fun ge(other: Any?) {
                operation = QueryFilterCondition.GREATER_THAN_OR_EQUAL
                filterValue = other
            }

            internal fun toQueryFilter(): QueryFilter {
                return QueryFilter(
                    name,
                    operation ?: throw IllegalArgumentException(
                        "You must provide an operation in a query."),
                    filterValue
                )
            }
        }
    }
}
