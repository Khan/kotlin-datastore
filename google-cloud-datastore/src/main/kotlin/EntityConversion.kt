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
import com.google.cloud.datastore.KeyValue
import com.google.cloud.datastore.ListValue
import com.google.cloud.datastore.LongValue
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.TimestampValue
import com.google.cloud.datastore.Value
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

// Sentinel value used to distinguish a null value in the datastore from a
// value not present in the datastore at all.
// We need to distinguish this so that we can use default values only if
// something has not been set in the datastore.
val PROPERTY_NOT_PRESENT = object {}

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
        }.filter { (_, value) -> value !== PROPERTY_NOT_PRESENT }
        .toMap()
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
        PROPERTY_NOT_PRESENT
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
 *
 * TODO(colin): combine this with DatastoreKey.toKey() (they behave differently
 * with respect to type inference currently).
 */
internal fun convertKeyUntyped(datastoreKey: DatastoreKey): Key<*>? {
    val parent = datastoreKey.parent?.let { parentDatastoreKey ->
        val parentKey = convertKeyUntyped(parentDatastoreKey)
            ?: throw IllegalArgumentException(
                "Unable to convert parent key $parentDatastoreKey")
        parentKey.parentPath + KeyPathElement(parentKey.kind,
                parentKey.idOrName)
    } ?: listOf()

    val namespace = if (datastoreKey.namespace == "") {
        null
    } else {
        datastoreKey.namespace
    }

    return assertedPrimaryConstructor(Key::class).call(
        datastoreKey.kind,
        if (datastoreKey.hasId()) {
            KeyID(datastoreKey.id)
        } else {
            KeyName(datastoreKey.name)
        },
        parent,
        namespace
    )
}

/**
 * Convert a value from the type the datastore client uses to our version.
 */
internal fun fromDatastoreType(datastoreValue: Any?): Any? =
    when (datastoreValue) {
        is Blob -> datastoreValue.toByteArray()
        is Timestamp -> convertTimestamp(datastoreValue)
        is com.google.cloud.datastore.Key -> convertKeyUntyped(datastoreValue)
        else -> datastoreValue
    }

/**
 * Convert a value from our type to the one the datastore client uses.
 */
internal fun toDatastoreType(kotlinValue: Any?): Any? =
    when (kotlinValue) {
        is ByteArray -> Blob.copyFrom(kotlinValue)
        is LocalDateTime -> Timestamp.ofTimeSecondsAndNanos(
            kotlinValue.toEpochSecond(ZoneOffset.UTC),
            kotlinValue.nano)
        is Key<*> -> kotlinValue.toDatastoreKey()
        else -> kotlinValue
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
): Any? = fromDatastoreType(when(type) {
    in ByteArrayTypes -> getBlob(name)
    // Kotlin doesn't correctly box these basic types as nullable, so
    // we need to do explicit checks ourselves.
    in BooleanTypes -> if (isNull(name)) null else getBoolean(name)
    in DoubleTypes -> if (isNull(name)) null else getDouble(name)
    in LongTypes -> if (isNull(name)) null else getLong(name)
    in StringTypes -> getString(name)
    in TimestampTypes -> getTimestamp(name)
    // The datastore does not distinguish `null` from the empty list, instead
    // treating them both as the empty list.
    // Therefore we don't allow nullable repeated values; use the empty list
    // instead. Note that it's still ok for the type of the list elements to be
    // nullable.
    List::class.createType(type.arguments) ->
        getList<Value<*>>(name).map { fromDatastoreType(it.get()) }
    Key::class.createType(type.arguments),
    Key::class.createType(type.arguments).withNullability(true) ->
        getKey(name)
    // TODO(colin): nested entities
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
})

/**
 * Convert a kotlin-typed value to a datastore Value<V> type.
 *
 * This is distinct from `toDatastoreType`, in that this function adds an
 * additional layer of wrapping in a Value<V>, which allows us to set the
 * indexing options. `toDatastoreType` is concerned solely with converting
 * between the types this library uses as part of its API for defining
 * datastore model properties, and the types the Google datastore client uses
 * for these properties.
 */
internal fun propertyValueToDatastoreValue(
    name: String,
    propertyValue: Any?,
    indexed: Boolean
): Value<*> = when (val datastoreValue = toDatastoreType(propertyValue)) {
    // Note that we have to duplicate the .newBuilder, .setExcludeFromIndexes
    // and .build calls here because both of those functions are generic and do
    // not type check on a builder with unknown type parameters.
    is Blob -> BlobValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is Boolean -> BooleanValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is Double -> DoubleValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is Long -> LongValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is String -> StringValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is Timestamp -> TimestampValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    is List<*> -> ListValue.newBuilder()
        .setExcludeFromIndexes(!indexed)
        .set(datastoreValue.map {
            propertyValueToDatastoreValue(name, it, indexed)
        })
        .build()
    is DatastoreKey -> KeyValue.newBuilder(datastoreValue)
        .setExcludeFromIndexes(!indexed)
        .build()
    // TODO(colin): implement other property types (see
    // getExistingTypedProperty for more detail)
    null -> NullValue.newBuilder()
        .setExcludeFromIndexes(!indexed)
        .build()
    else ->
        throw IllegalArgumentException(
            "Unable to store property $name in the datastore")
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

    set(name, propertyValueToDatastoreValue(name, propertyValue, indexed))
}
