/**
 * Functions for converting google datastore entities to our model classes.
 */
package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.BlobValue
import com.google.cloud.datastore.BooleanValue
import com.google.cloud.datastore.DoubleValue
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.LongValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.TimestampValue
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyID
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.KeyPathElement
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Meta
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.reflect.KAnnotatedElement
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability

/**
 * Get the primary constructor for a class, asserting it has one.
 */
internal fun <T : Any> assertedPrimaryConstructor(
    cls: KClass<T>
): KFunction<T> {
    return cls.primaryConstructor
        ?: throw Exception(
            "Entity must be represented by a data class with primary " +
                "constructor")
}

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
    val constructor = assertedPrimaryConstructor(tRef)
    val parameters = constructor.valueParameters
        .map { kParameter ->
            // TODO(colin): instead of hard-coding "key" we should use the name
            // of the single property in the Keyed<T> interface.
            kParameter to if (kParameter.name == "key") {
                key?.toKey<T>()
                    ?: throw IllegalStateException(
                        "Expected entity to have a key when getting.")
            } else {
                getTypedProperty<T>(
                    datastoreName(kParameter)
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

fun Keyed<*>.toDatastoreEntity(): Entity {
    val properties = this::class.declaredMemberProperties

    val entity = Entity.newBuilder(this.key.toDatastoreKey())

    properties.filter { it.name != this::key.name }
        .forEach { entity.setTypedProperty(it, this) }
    return entity.build()
}

/**
 * Find the name for the given parameter in the datastore.
 *
 * Prefer a `@Meta(name = ...)` annotation if present; otherwise, fallback on
 * the name in code.
 */
internal fun datastoreName(kParameter: KParameter): String? =
    metaAnnotationName(kParameter) ?: kParameter.name

/**
 * Read the name out of a @Meta annotation, if present.
 */
internal fun metaAnnotationName(element: KAnnotatedElement): String? {
    val annotName = element.findAnnotation<Meta>()?.name
    return if (annotName != null && annotName != "") {
        annotName
    } else {
        null
    }
}

/**
 * Read the indexing out of a @Meta annotation, if present.
 */
internal fun metaAnnotationIndexed(element: KAnnotatedElement): Boolean? =
    element.findAnnotation<Meta>()?.indexed

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
internal fun <T : Keyed<T>> Entity.getTypedProperty(
    name: String, type: KType
): Any? = checkNullability(
    key?.kind,
    name,
    type,
    if (name in this) {
        getExistingTypedProperty<T>(name, type)
    } else {
        null
    }
)

/**
 * Convert a datastore timestamp type to a java-native LocalDateTime.
 *
 * We use the convention where all times stored in the datastore are expressed
 * as naive date-times expressed in UTC.
 */
internal fun convertTimestamp(timestamp: Timestamp): LocalDateTime =
    LocalDateTime.ofInstant(
        Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()),
        ZoneId.of("UTC")
    )

/**
 * Convert a datastore key to a Key wrapper.
 *
 * TODO(sammy): although less than ideal, reflection here makes it so that we
 * can have the actual type of the model to which the key corresponds at
 * runtime.
 */
internal fun convertKeyUntyped(datastoreKey: DatastoreKey): Key<*>? {
    val parent = datastoreKey.parent?.let { parentDatastoreKey ->
        val parentKey = convertKeyUntyped(parentDatastoreKey)
            ?: throw IllegalArgumentException(
                "Unable to convert parent key $parentDatastoreKey")
        parentKey.parentPath + KeyPathElement(parentKey.kind,
                parentKey.idOrName)
    } ?: listOf()

    // TODO(sammy): use asserted primary constructor instead of !!
    return Key::class.primaryConstructor!!.call(
        datastoreKey.kind,
        if (datastoreKey.hasId()) {
            KeyID(datastoreKey.id)
        } else {
            KeyName(datastoreKey.name)
        },
        parent
    )
}

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

private val TimestampTypes = listOf(
    LocalDateTime::class.createType(),
    LocalDateTime::class.createType().withNullability(true)
)

/**
 * Get the value of an entity's property, given that we know it's present.
 */
internal fun <T : Keyed<T>> Entity.getExistingTypedProperty(
    name: String, type: KType
): Any? = when (type) {
    in ByteArrayTypes -> getBlob(name)?.toByteArray()
    in BooleanTypes -> getBoolean(name)
    in DoubleTypes -> getDouble(name)
    in LongTypes -> getLong(name)
    in StringTypes -> getString(name)
    in TimestampTypes -> getTimestamp(name)?.let(::convertTimestamp)
    Key::class.createType(type.arguments),
    Key::class.createType(type.arguments).withNullability(true) -> getKey(name)?.let { convertKeyUntyped(it) }
    // TODO(colin): nested entities
    // TODO(colin): key properties
    // TODO(colin): location (lat/lng) properties
    // TODO(colin): repeated properties
    // TODO(colin): JSON properties
    // TODO(colin): computed properties (do we need these?)
    // TODO(colin): in general we want to support all ndb property types, see:
    // https://cloud.google.com/appengine/docs/standard/python/ndb/entity-property-reference#properties_and_value_types
    else ->
        throw IllegalArgumentException(
            "Unable to use property $name of type $type as a datastore " +
                "property")
}

/**
 * Set the value of an entity's property, via its builder.
 *
 * TODO(colin): we'd like this to be parameterized by the model type too, but
 * there's something subtle with type variance that's preventing us from doing
 * that. Figure out why.
 */
internal fun <P> Entity.Builder.setTypedProperty(
    property: KProperty<P>,
    modelInstance: Keyed<*>
) {
    val propertyValue: P = property.call(modelInstance)

    // We need to match this property up to the constructor parameters to allow
    // for an API where we can just do `@Meta(name = "x")` instead of both
    // `@param:Meta(...)` and `@property.Meta(...)` being required.
    // (Annotations will only apply to one target by default if there are
    // multiple valid ones, as there are in data class constructors.)
    //
    // The reason we need to deal with the constructor parameters at all rather
    // than just having the annotation apply to the property, is that the
    // `Datastore.get()` method needs to call the constructor reflectively
    // using parameters, so to get the annotations there, we'd then just have
    // to do the inverse matching to match property to constructor parameter.
    //
    // We also can't just assume that constructor parameters are the same as
    // properties as we may have computed properties, which are stored in the
    // datastore, but are not settable manually.
    val parameter = assertedPrimaryConstructor(modelInstance::class)
        .valueParameters.firstOrNull {
            it.name == property.name
        }
    val name =
        metaAnnotationName(property) // Only present on computed properties.
            ?: parameter?.let(::datastoreName)
            ?: property.name

    val indexed =
        metaAnnotationIndexed(property) // Only present on computed properties.
            ?: parameter?.let(::metaAnnotationIndexed)
            ?: false

    val value = when (propertyValue) {
        // Note that we have to duplicate the .setExcludeFromIndexes and .build
        // calls here because both of those functions are generic and do not
        // type check on a builder with unknown type parameters.
        is ByteArray -> BlobValue.newBuilder(Blob.copyFrom(propertyValue))
            .setExcludeFromIndexes(!indexed)
            .build()
        is Boolean -> BooleanValue.newBuilder(propertyValue)
            .setExcludeFromIndexes(!indexed)
            .build()
        is Double -> DoubleValue.newBuilder(propertyValue)
            .setExcludeFromIndexes(!indexed)
            .build()
        is Long -> LongValue.newBuilder(propertyValue)
            .setExcludeFromIndexes(!indexed)
            .build()
        is String -> StringValue.newBuilder(propertyValue)
            .setExcludeFromIndexes(!indexed)
            .build()
        is LocalDateTime -> {
            val timestamp = Timestamp.ofTimeSecondsAndNanos(
                propertyValue.toEpochSecond(ZoneOffset.UTC),
                propertyValue.nano)
            TimestampValue.newBuilder(timestamp)
                .setExcludeFromIndexes(!indexed)
                .build()
        }
        // TODO(colin): implement other property types (see
        // getExistingTypedProperty for more detail)
        // TODO(colin): we may want this to actually set `null` in the
        // datastore when implementing default values for properties.
        null -> null
        else ->
            throw IllegalArgumentException(
                "Unable to store property $name in the datastore")
    }
    value?.let { set(name, it) }
}
