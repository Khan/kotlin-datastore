/**
 * Functions for converting ndb-style structured properties.
 *
 * ndb-style structured properties have an interface similar to nested
 * entities, but for storage, they flatten their nested fields out into
 * separate top-level fields on the entity, with names munged into a
 * "."-separated field path. (For instance a structured property called
 * "nested" with fields "a" and "b" would be flattened into top-level fields
 * with names "nested.a" and "nested.b".
 *
 * If your application doesn't need python compatibility, you probably
 * shouldn't use this! Instead, use individual top-level fields or nested
 * entity properties.
 */
package org.khanacademy.datastore

import com.google.cloud.datastore.BaseEntity
import com.google.cloud.datastore.FullEntity
import org.khanacademy.metadata.StructuredProperty
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.KVariance
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure

internal fun KType.isStructuredPropertyType() =
    arguments.isNotEmpty() && (
        this == StructuredProperty::class.createType(arguments) ||
            this == StructuredProperty::class.createType(arguments)
            .withNullability(true)
        )

internal fun KType.isStructuredPropertyListType() =
    isListType() && unwrapSinglyParameterizedType().isStructuredPropertyType()

/**
 * Extract the data associated with an ndb.style StructuredProperty.
 *
 * For compatibility, we store this not as a nested protobuf, but as a series
 * of flattened top-level properties with dot-delimited names like
 * `base.field`. When we have a structured property, we need to swap in
 * extraction of these flattened properties, then transform it back to the
 * nested object we're expecting in kotlin.
 *
 * TODO(colin): should we assert that you can't have a repeated property with
 * repeated fields? That's kind of covered by the other assertions, just not
 * explicitly.
 */
internal fun FullEntity<*>.getExistingStructuredProperty(
    name: String,
    // This is the KType for the property data class itself, which in code
    // is wrapped in a StructuredProperty (or a List<StructuredProperty> for
    // repeated properties).
    targetType: KType,
    // Is the whole structuredProperty repeated (i.e. a list)?
    repeated: Boolean = false
): Any? {
    // Note: here, jvmErasure just functions to turn our KType into a KClass.
    val constructor = assertedPrimaryConstructor(targetType.jvmErasure)
    val constructorParams = constructor.valueParameters
        // We don't expect structured properties to have a key. In theory we
        // should not be using a Keyed<T> as a structured property, but let's
        // filter out the key to avoid any possible unexpected issues.
        .filter { it.name != "key" }
        .map { kParameter ->
            val propertyName = datastoreName(kParameter)
                ?: throw IllegalArgumentException(
                    "Unable to use nameless parameter $kParameter " +
                        "on $targetType as a datastore property")
            val mungedName = "$name.$propertyName"
            if (kParameter.type.isStructuredPropertyType()) {
                // TODO(colin): do we need to add support for nested structured
                // properties?
                throw NotImplementedError(
                    "Nested StructuredProperties are not yet supported.")
            }
            // Repeated structured properties, due to the flattening, end up as
            // individually repeated top-level types. Thus, we need to change
            // this type to a List if the whole property is repeated.
            val mungedType = if (repeated) {
                if (kParameter.type.isListType()) {
                    throw IllegalArgumentException(
                        "Repeated structured properties with repeated " +
                            "fields are not allowed.")
                }
                List::class.createType(listOf(
                    KTypeProjection(KVariance.INVARIANT, kParameter.type)))
            } else {
                kParameter.type
            }
            kParameter to getTypedProperty(
                mungedName,
                mungedType
            )
        }
        .toMap()

    val presentConstructorParams = constructorParams
        .filter { (_, value) -> value !== PROPERTY_NOT_PRESENT }

    if (!repeated) {
        return StructuredProperty(constructor.callBy(presentConstructorParams))
    }

    // Now, if this property was repeated we have to un-flatten the
    // individually repeated fields into one top-level repeated structured
    // property.
    val constructorParamLists = constructorParams.mapValues { (_, value) ->
        when (value) {
            PROPERTY_NOT_PRESENT -> listOf()
            else -> value as List<Any?>
        }
    }

    // Let's first validate that each property has the same number of values
    // and error loudly now if that assumption is violated.
    val numValues = constructorParamLists.values.map { it.size }.max() ?: 0
    if (constructorParamLists.values.any { it.size != numValues }) {
        throw IllegalStateException(
            "Repeated structured property $name of type $targetType did not " +
                "have the same number of values for each field.")
    }

    if (constructorParams.isEmpty()) {
        throw IllegalArgumentException(
            "Empty structured properties are not allowed.")
    }

    // Disallow optional parameters (that is, ones like
    // `val x: String = "some_default"`)
    // While this would be fine with this library only (because we always store
    // a value for every property in the datastore), in general this isn't safe
    // because of how we flatten repeated properties. For example, if we had a
    // model `A` with two different optional properties `b` and `c` (both
    // defaulting to 0) in the datastore, and we put `A(b = 1)` and `A(c = 2)`,
    // this would flatten to `A.b = listOf(1)` and `A.c = listOf(2)`, and then
    // we'd get back out `A(b = 1, c = 2)`, which is not the same as what we
    // put in!
    if (constructorParams.keys.any { it.isOptional }) {
        throw IllegalArgumentException(
            "Default values are not allowed for repeated " +
                "structured properties.")
    }

    return (0 until numValues).map { i ->
        val singleParams =
            constructorParamLists.mapValues { (_, list) -> list[i] }
        StructuredProperty(constructor.callBy(singleParams))
    }
}

/**
 * Set an ndb-style structured property's top-level fields on an Entity.
 */
internal fun BaseEntity.Builder<*, *>.setStructuredProperty(
    property: KProperty<*>,
    propertyValue: StructuredProperty<*>,
    name: String,
    indexed: Boolean
) {
    val propertyTypeConstructor = assertedPrimaryConstructor(
        property.returnType.unwrapSinglyParameterizedType().jvmErasure)

    propertyTypeConstructor.valueParameters.map { innerParameter ->
        val propertyName = datastoreName(innerParameter)
        val mungedName = "$name.$propertyName"
        val value = propertyValue.value::class.memberProperties.first {
            it.name == innerParameter.name
        }.call(propertyValue.value)
        // TODO(colin): should we somehow warn if you have a
        // StructuredProperty that is not indexed?
        set(mungedName, propertyValueToDatastoreValue(
            mungedName, value, indexed))
    }
}

/**
 * Set a repeated ndb-style structured property's top-level fields.
 *
 * Repeated structured properties have to be handled separately, as each of the
 * flattened top-level fields has to be made repeated as well.
 */
internal fun BaseEntity.Builder<*, *>.setRepeatedStructuredProperty(
    property: KProperty<*>,
    propertyValue: List<StructuredProperty<*>>,
    name: String,
    indexed: Boolean
) {
    // Repeated structured properties have to be handled separately, as
    // they're flattened out into a number of top-level repeated fields.
    val propertyTypeConstructor = assertedPrimaryConstructor(
        property.returnType
            .unwrapSinglyParameterizedType() // Unwrap List<T>
            .unwrapSinglyParameterizedType() // Unwrap StructuredProperty<T>
            .jvmErasure) // Convert KType to KClass

    propertyTypeConstructor.valueParameters.map { innerParameter ->
        val propertyName = datastoreName(innerParameter)
        val mungedName = "$name.$propertyName"
        val values = propertyValue.map { structuredProp ->
            structuredProp.value::class.memberProperties.first {
                it.name == innerParameter.name
            }.call(structuredProp.value)
        }
        // TODO(colin): should we somehow warn if you have a
        // StructuredProperty that is not indexed?
        set(mungedName, propertyValueToDatastoreValue(
            mungedName, values, indexed))
    }
}
