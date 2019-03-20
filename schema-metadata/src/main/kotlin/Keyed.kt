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
