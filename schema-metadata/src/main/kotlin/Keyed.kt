package org.khanacademy.metadata

sealed class KeyIDOrName
data class KeyID(val value: Long) : KeyIDOrName()
data class KeyName(val value: String) : KeyIDOrName()

typealias KeyPathElement = Pair<String, KeyIDOrName>

/**
 * Typed wrapper around a datastore key.
 */
data class Key<T : Keyed<T>>(
    val kind: String,
    val idOrName: KeyIDOrName,
    val parentPath: List<KeyPathElement> = listOf()
) {
    fun path(): List<KeyPathElement> = parentPath + (kind to idOrName)

    constructor(
        kind: String,
        name: String,
        parentPath: List<KeyPathElement> = listOf()
    ) : this(kind, KeyName(name), parentPath)

    constructor(
        kind: String,
        id: Long,
        parentPath: List<KeyPathElement> = listOf()
    ) : this(kind, KeyID(id), parentPath)
}

interface Keyed<T : Keyed<T>> {
    val key: Key<T>
}
