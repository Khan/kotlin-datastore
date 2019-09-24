/**
 * Functions for converting this library's keys to google datastore keys.
 */
package org.khanacademy.datastore

import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyID
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.KeyPathElement
import org.khanacademy.metadata.Keyed

typealias DatastoreKey = com.google.cloud.datastore.Key

internal fun rootKeyPathToDatastoreKey(
    projectId: String?,
    path: KeyPathElement,
    namespace: String? = null
): DatastoreKey {
    val (kind, idOrName) = path
    // Unfortunately this switching on type is required to distinguish between
    // overloaded java methods, even though the code is the same here for
    // either ID or name.
    // TODO(colin): this implies that this library can only access the
    // datastore from one project per executable. Maybe relax this
    // constraint?
    val builder = when (idOrName) {
        is KeyID -> DatastoreKey.newBuilder(
            projectId ?: DBEnvAndProject.getEnvAndProject().project,
            kind, idOrName.value)
        is KeyName -> DatastoreKey.newBuilder(
            projectId ?: DBEnvAndProject.getEnvAndProject().project,
            kind, idOrName.value)
    }
    namespace?.let { builder.setNamespace(namespace) }
    return builder.build()
}

internal fun keyPathElementToDatastoreKey(
    parent: DatastoreKey, path: KeyPathElement,
    namespace: String? = null
): DatastoreKey {
    val (kind, idOrName) = path
    // Unfortunately this switching on type is required to distinguish between
    // overloaded java methods, even though the code is the same here for
    // either ID or name.
    val builder = when (idOrName) {
        is KeyID ->
            DatastoreKey.newBuilder(parent, kind, idOrName.value)
        is KeyName ->
            DatastoreKey.newBuilder(parent, kind, idOrName.value)
    }
    namespace?.let { builder.setNamespace(it) }
    return builder.build()
}

/**
 * Convert our abstract keys to the Google cloud datastore key class.
 */
fun Key<*>.toDatastoreKey(projectId: String? = null): DatastoreKey {
    val rootPath = path().first()
    val remainingPath = path().drop(1)
    return remainingPath.fold(
        rootKeyPathToDatastoreKey(projectId, rootPath, namespace)
    ) { parent, path ->
        keyPathElementToDatastoreKey(parent, path, namespace)
    }
}

/**
 * Convert a Google cloud datstore key to our abstract key class.
 */
fun <T : Keyed<T>> DatastoreKey.toKey(): Key<T> = Key(
    parentPath = ancestors.map { pathElement ->
        if (pathElement.hasId()) {
            KeyPathElement(pathElement.kind, KeyID(pathElement.id))
        } else {
            KeyPathElement(pathElement.kind, KeyName(pathElement.name))
        }
    },
    kind = kind,
    idOrName = if (hasId()) { KeyID(id) } else { KeyName(name) },
    namespace = if (namespace == "") { null } else { namespace }
)
