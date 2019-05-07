package org.khanacademy.metadata

sealed class KeyIDOrName
data class KeyID(val value: Long) : KeyIDOrName()
data class KeyName(val value: String) : KeyIDOrName()

data class KeyPathElement(val kind: String, val idOrName: KeyIDOrName)

/**
 * Typed wrapper around a datastore key.
 */
data class Key<T : Keyed<T>>(
    val kind: String,
    val idOrName: KeyIDOrName,
    val parentPath: List<KeyPathElement> = listOf(),
    val namespace: String? = null
) {
    fun path(): List<KeyPathElement> = parentPath +
        KeyPathElement(kind, idOrName)

    constructor(
        kind: String,
        name: String,
        parentPath: List<KeyPathElement> = listOf(),
        namespace: String? = null
    ) : this(kind, KeyName(name), parentPath, namespace)

    constructor(
        kind: String,
        id: Long,
        parentPath: List<KeyPathElement> = listOf(),
        namespace: String? = null
    ) : this(kind, KeyID(id), parentPath, namespace)
}

interface Keyed<T : Keyed<T>> {
    val key: Key<T>
}

/**
 * Classes implementing this interface can be used as datastore properties.
 *
 * Note that this is not needed for built-in properties like String or LatLng.
 * It's needed only for user-defined properties and nested entities (local
 * structured properties and ndb-style structured properties), which are
 * expressed as kotlin data classes.
 *
 * This is here so that we can have more checks on types as well as more
 * explicitly specify the intent for any given class.
 *
 * The intent is that this is to be used on kotlin data classes only.
 *
 * Example usage:
 * // Point must implement Property because it's used as a nested entity in
 * // Circle.
 * data class Point(val x: Int, val y: Int) : Property
 * data class Circle(val center: Point, val radius: Int)
 *
 * TODO(colin): check for a data class somehow?
 *
 * TODO(colin): the downside here (beside a bit of verbosity) is that you can't
 * use a third-party class as a property since you don't control the code. Do
 * we need a way around this ever?
 */
interface Property

/**
 * Properties implementing this interface can control their serializtion.
 *
 * If you're writing a custom subclass that wants to control the format in
 * which it's stored in the datastore, or to add hooks that occur on get or
 * put, you can use this class to control this behavior. We guarantee that
 * `fromDatastoreValue` will be called once on `DB.get` (and its variants),
 * with the value stored by this property in the datastore, and we guarantee
 * that `toDatastoreValue` will likewise be called once on `DB.put` (and its
 * variants) with the result returned by `getKotlinValue`, and the result will
 * be stored in the datastore. The get/set and to/from functions are separate
 * to provide a clearer separation between state management of the stored value
 * and transforming between the types.
 *
 * While we are not able to express it as part of the interface, implementing
 * types must provide a no-argument primary constructor that will be called
 * reflectively.
 *
 * To summarize, the following will happen on deserialize:
 * - read from the datastore a value of type D, with this property's name
 * - construct an instance with the 0-argument constructor
 * - call fromDatastoreValue(d: D) to get a value of type K
 * - call setKotlinValue(k: K) on the value obtained from the previous call
 *
 * The following will happen on serialize:
 * - call getKotlinValue to get a value of type K
 * - call toDatastoreValue(k: K) on the value obtained from the previous call
 * - write to the datastore the resulting value of type D
 */
interface CustomSerializationProperty<D, K> : Property {
    fun getKotlinValue(): K
    fun setKotlinValue(value: K)
    fun fromDatastoreValue(datastoreValue: D): K
    fun toDatastoreValue(kotlinValue: K): D
}
