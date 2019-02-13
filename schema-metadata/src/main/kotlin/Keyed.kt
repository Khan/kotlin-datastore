package org.khanacademy.metadata

sealed class KeyIDOrName
data class KeyID(val value: Long) : KeyIDOrName()
data class KeyName(val value: String) : KeyIDOrName()

typealias KeyPathElement = Pair<String, KeyIDOrName>

/**
 * Typed wrapper around a datastore key.
 */
interface Key<T : Keyed<T>> {
    val parentPath: List<KeyPathElement>
    val kind: String
    val idOrName: KeyIDOrName

    fun path(): List<KeyPathElement> = parentPath + (kind to idOrName)
}

interface Keyed<T : Keyed<T>> {
    val key: Key<T>
}
