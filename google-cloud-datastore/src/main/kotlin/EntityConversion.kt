/**
 * Functions for converting google datastore entities to our model classes.
 */
package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.BaseEntity
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.BlobValue
import com.google.cloud.datastore.BooleanValue
import com.google.cloud.datastore.DoubleValue
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.EntityValue
import com.google.cloud.datastore.FullEntity
import com.google.cloud.datastore.IncompleteKey
import com.google.cloud.datastore.KeyValue
import com.google.cloud.datastore.LatLng
import com.google.cloud.datastore.LatLngValue
import com.google.cloud.datastore.ListValue
import com.google.cloud.datastore.LongValue
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.TimestampValue
import com.google.cloud.datastore.Value
import org.khanacademy.metadata.GeoPt
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyID
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.KeyPathElement
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Meta
import org.khanacademy.metadata.Property
import org.khanacademy.metadata.StructuredProperty
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
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure

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
    val keyParameter = constructor.valueParameters
        .firstOrNull { it.name == "key" }
        ?: throw IllegalStateException(
            "Expected entity to have a key when getting.")
    val keyArg = keyParameter to key.toKey<T>()
    val allArgs = unkeyedEntityToParameterMap(this, tRef) + keyArg
    return constructor.callBy(allArgs)
}

/**
 * Convert a base entity (without a key) to a kotlin data class instance.
 *
 * FullEntity is an Entity that may not have the key set. Despite its name, it
 * is a superclass of entity, not a subclass.
 */
internal fun <T : Any> unkeyedEntityToTypedObject(
    entity: FullEntity<*>, targetType: KClass<T>
): T = assertedPrimaryConstructor(targetType)
    .callBy(unkeyedEntityToParameterMap(entity, targetType))

/**
 * Convert a base entity to a map for calling a data class constructor.
 *
 * The gcloud library stores an entity in a subclass of BaseEntity, which is
 * basically a glorified map. This turns this glorified map into an actual map
 * of the format we need.
 */
internal fun unkeyedEntityToParameterMap(
    entity: FullEntity<*>, targetType: KClass<*>
): Map<KParameter, Any?> {
    val constructor = assertedPrimaryConstructor(targetType)
    return constructor.valueParameters
        // We don't expect nested entities to have a key, and for root
        // entities, we'll already have gotten the key because it's required by
        // the Entity.Builder constructor.
        .filter { it.name != "key" }
        .map { kParameter ->
            kParameter to entity.getTypedProperty(
                datastoreName(kParameter)
                    ?: throw IllegalArgumentException(
                        "Unable to use nameless parameter $kParameter " +
                            "on class ${targetType.simpleName} as a " +
                            "datastore property"),
                kParameter.type
            )
        }
        .filter { (_, value) -> value !== PROPERTY_NOT_PRESENT }
        .toMap()
}

fun Keyed<*>.toDatastoreEntity(): Entity {
    val builder = Entity.newBuilder(this.key.toDatastoreKey())
    buildEntityPropertiesFromObject(builder, this)
    return builder.build()
}

/**
 * Convert any data class instance to a FullEntity using reflection.
 *
 * A FullEntity is a datastore entity that has all properties present, but may
 * lack a key. Despite what the name suggests, it is a superclass of Entity,
 * not a subclass.
 *
 * Note that this will not set the key; if you need that functionality, use
 * Keyed<*>.toDatastoreEntity() instead.
 */
fun objectToDatastoreEntity(dataclassInstance: Any): FullEntity<*> {
    val builder = FullEntity.newBuilder()
    buildEntityPropertiesFromObject(builder, dataclassInstance)
    return builder.build()
}

internal fun buildEntityPropertiesFromObject(
    entityBuilder: BaseEntity.Builder<*, *>, dataclassInstance: Any
) {
    dataclassInstance::class.declaredMemberProperties.filter {
        // TODO(colin): get this off the Keyed interface?
        it.name != "key"
    }.forEach {
        entityBuilder.setTypedProperty(it, dataclassInstance)
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
internal fun FullEntity<*>.getTypedProperty(
    name: String, type: KType
): Any? = checkNullability(
    key?.kind,
    name,
    type,
    if (name in this ||
        // ndb-style StructuredProperties do some weird property renaming, so
        // their names should never be on the entity. They do their own
        // handling of absence, and we treat them as if they're always present.
        type.isStructuredPropertyType() ||
        type.isStructuredPropertyListType()) {

        getExistingTypedProperty(name, type)
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
internal fun fromDatastoreType(datastoreValue: Any?, targetType: KType): Any? =
    when (datastoreValue) {
        is Blob -> datastoreValue.toByteArray()
        is Timestamp -> convertTimestamp(datastoreValue)
        is com.google.cloud.datastore.Key -> convertKeyUntyped(datastoreValue)
        is LatLng -> GeoPt(datastoreValue.latitude, datastoreValue.longitude)
        is FullEntity<*> ->
            // Note that .jvmErasure is here just a mechanism for converting
            // our KType to a KClass.
            unkeyedEntityToTypedObject(datastoreValue, targetType.jvmErasure)
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
        is GeoPt -> LatLng.of(kotlinValue.latitude, kotlinValue.longitude)
        is Property -> objectToDatastoreEntity(kotlinValue)
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

private val EntityPropertyTypes = listOf(
    Property::class.createType(),
    Property::class.createType().withNullability(true)
)

private val GeoPtPropertyTypes = listOf(
    GeoPt::class.createType(),
    GeoPt::class.createType().withNullability(true)
)

internal fun KType.isListType(): Boolean =
    arguments.isNotEmpty() && this == List::class.createType(arguments)

internal fun KType.unwrapSinglyParameterizedType(): KType =
    arguments[0].type ?: throw IllegalArgumentException(
        "Star-projected types are not allowed as properties.")

/**
 * Get the value of an entity's property, given that we know it's present.
 */
internal fun FullEntity<*>.getExistingTypedProperty(
    name: String, type: KType
): Any? = fromDatastoreType(when {
    type in ByteArrayTypes -> getBlob(name)
    // Kotlin doesn't correctly box these basic types as nullable, so
    // we need to do explicit checks ourselves.
    type in BooleanTypes -> if (isNull(name)) null else getBoolean(name)
    type in DoubleTypes -> if (isNull(name)) null else getDouble(name)
    type in LongTypes -> if (isNull(name)) null else getLong(name)
    type in StringTypes -> getString(name)
    type in TimestampTypes -> getTimestamp(name)
    type in GeoPtPropertyTypes -> getLatLng(name)
    type.isStructuredPropertyType() -> // Is a StructuredProperty<T>
        getExistingStructuredProperty(
            name, type.unwrapSinglyParameterizedType())
    // This check must come before the generic list check below.
    type.isStructuredPropertyListType() ->
        getExistingStructuredProperty(
            name,
            type.unwrapSinglyParameterizedType() // Unwrap List<T>
                .unwrapSinglyParameterizedType(), // Unwrap StructuredProperty
            repeated = true)
    // The datastore does not distinguish `null` from the empty list, instead
    // treating them both as the empty list.
    // Therefore we don't allow nullable repeated values; use the empty list
    // instead. Note that it's still ok for the type of the list elements to be
    // nullable.
    type.isListType() -> {
        val innerType = type.unwrapSinglyParameterizedType()
        getList<Value<*>>(name).map { fromDatastoreType(it.get(), innerType) }
    }
    type.arguments.isNotEmpty() && (
        type == Key::class.createType(type.arguments) ||
        type == Key::class.createType(type.arguments).withNullability(true)) ->
        getKey(name)
    // TODO(colin): location (lat/lng) properties
    // TODO(colin): JSON properties
    // TODO(colin): computed properties (do we need these?)
    // TODO(colin): in general we want to support all ndb property types, see:
    // https://cloud.google.com/appengine/docs/standard/python/ndb/entity-property-reference#properties_and_value_types
    EntityPropertyTypes.any { type.isSubtypeOf(it) } ->
        getEntity<IncompleteKey>(name)
    else -> throw IllegalArgumentException(
        "Unable to use property $name of type $type as a datastore property")
}, type)

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
    is LatLng -> LatLngValue.newBuilder(datastoreValue)
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
    is FullEntity<*> -> EntityValue.newBuilder(datastoreValue)
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
internal fun <P> BaseEntity.Builder<*, *>.setTypedProperty(
    property: KProperty<P>,
    modelInstance: Any
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

    val name = kotlinNameToDatastoreName(modelInstance::class, property.name)

    val indexed =
        metaAnnotationIndexed(property) // Only present on computed properties.
            ?: parameter?.let(::metaAnnotationIndexed)
            ?: false

    when {
        propertyValue is StructuredProperty<*> ->
            setStructuredProperty(property, propertyValue, name, indexed)
        propertyValue is List<*> && propertyValue.isNotEmpty() &&
            propertyValue[0] is StructuredProperty<*> ->

            @Suppress("UNCHECKED_CAST")
            setRepeatedStructuredProperty(
                property, propertyValue as List<StructuredProperty<*>>,
                name, indexed)
        else ->
            set(name, propertyValueToDatastoreValue(
                name, propertyValue, indexed))
    }
}
