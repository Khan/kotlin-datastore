/**
 * Functions for converting google datastore entities to our model classes.
 */
package org.khanacademy.datastore

import com.google.cloud.datastore.Entity
import org.khanacademy.metadata.Keyed
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability

/**
 * Convert a Google cloud datastore entity to one of our model classes.
 *
 * This uses reflection internally to accomplish this and will throw if the
 * entity doesn't conform to the model's schema.
 *
 * TODO(colin): support generating non-reflective converters using annotation
 * processors.
 */
fun <T : Keyed<T>> Entity.toTypedModel(tRef: KClass<T>): T {
    val constructor = tRef.primaryConstructor
        ?: throw Exception(
            "Entity must be represented by a data class with primary " +
                "constructor")
    val parameters = constructor.valueParameters
        .map { kParameter ->
            // TODO(colin): instead of hard-coding "key" we should use the name
            // of the single property in the Keyed<T> interface.
            kParameter to if (kParameter.name == "key") {
                key?.toKey<T>()
                    ?: throw IllegalStateException(
                        "Expected entity to have a key when getting.")
            } else {
                getTypedProperty(
                    kParameter.name
                        ?: throw IllegalArgumentException(
                            "Unable to use nameless parameter $kParameter " +
                                "on class ${tRef.simpleName} as a datastore " +
                                "property"),
                    kParameter.type
                )
            }
        }.toMap()
    return constructor.callBy(parameters)
}

/**
 * Throw an error with detailed information if a property is unexpectedly null.
 */
internal fun checkNullability(
    kind: String?,
    name: String,
    type: KType,
    value: Any?
): Any? {
    if (!type.isMarkedNullable && value == null) {
        throw NullPointerException(
            "Property $name of type $type on kind $kind was unexpectedly null")
    }
    return value
}

/**
 * Get a single property off an entity, converting to the given type.
 */
internal fun Entity.getTypedProperty(
    name: String, type: KType
): Any? = checkNullability(
    key?.kind,
    name,
    type,
    if (name in this) {
        getExistingTypedProperty(name, type)
    } else {
        null
    }
)

// Precalculated types for doing conversions from Google datastore entities.
// (These allow us to avoid reconstructing the type objects for every
// property.)

private val ByteArrayTypes = listOf(
    ByteArray::class.createType(),
    ByteArray::class.createType().withNullability(true)
)

private val BooleanTypes = listOf(
    Boolean::class.createType(),
    Boolean::class.createType().withNullability(true)
)

private val DoubleTypes = listOf(
    Double::class.createType(),
    Double::class.createType().withNullability(true)
)

private val LongTypes = listOf(
    Long::class.createType(),
    Long::class.createType().withNullability(true)
)

private val StringTypes = listOf(
    String::class.createType(),
    String::class.createType().withNullability(true)
)

/**
 * Get the value of an entity's property, given that we know it's present.
 */
internal fun Entity.getExistingTypedProperty(
    name: String, type: KType
): Any? = when (type) {
    in ByteArrayTypes -> getBlob(name)?.toByteArray()
    in BooleanTypes -> getBoolean(name)
    in DoubleTypes -> getDouble(name)
    in LongTypes -> getLong(name)
    in StringTypes -> getString(name)
    // TODO(colin): nested entities
    // TODO(colin): key properties
    // TODO(colin): location (lat/lng) properties
    // TODO(colin): repeated properties
    // TODO(colin): timestamp properties
    // TODO(colin): JSON properties
    // TODO(colin): computed properties (do we need these?)
    // TODO(colin): in general we want to support all ndb property types, see:
    // https://cloud.google.com/appengine/docs/standard/python/ndb/entity-property-reference#properties_and_value_types
    else ->
        throw IllegalArgumentException(
            "Unable to use property $name of type $type as a datastore " +
                "property")
}
