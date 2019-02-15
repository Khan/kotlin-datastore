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

internal fun rootKeyPathToDatastoreKey(path: KeyPathElement): DatastoreKey {
    val (kind, idOrName) = path
    // Unfortunately this switching on type is required to distinguish between
    // overloaded java methods, even though the code is the same here for
    // either ID or name.
    return when (idOrName) {
        // TODO(colin): this implies that this library can only access the
        // datastore from one project per executable. Maybe relax this
        // constraint?
        is KeyID -> DatastoreKey.newBuilder(
            DBEnvAndProject.getEnvAndProject().project, kind, idOrName.value)
                .build()
        is KeyName -> DatastoreKey.newBuilder(
            DBEnvAndProject.getEnvAndProject().project, kind, idOrName.value)
                .build()
    }
}

internal fun keyPathElementToDatastoreKey(
    parent: DatastoreKey, path: KeyPathElement
): DatastoreKey {
    val (kind, idOrName) = path
    // Unfortunately this switching on type is required to distinguish between
    // overloaded java methods, even though the code is the same here for
    // either ID or name.
    return when (idOrName) {
        is KeyID ->
            DatastoreKey.newBuilder(parent, kind, idOrName.value).build()
        is KeyName ->
            DatastoreKey.newBuilder(parent, kind, idOrName.value).build()
    }
}

/**
 * Convert our abstract keys to the Google cloud datastore key class.
 */
internal fun <T : Keyed<T>> Key<T>.toDatastoreKey(): DatastoreKey {
    val rootPath = path().first()
    val remainingPath = path().drop(1)
    return remainingPath.fold(
        rootKeyPathToDatastoreKey(rootPath),
        ::keyPathElementToDatastoreKey)
}

internal fun <T : Keyed<T>> DatastoreKey.toKey(): Key<T> = Key(
    parentPath = ancestors.map { pathElement ->
        if (pathElement.hasId()) {
            pathElement.kind to KeyID(pathElement.id)
        } else {
            pathElement.kind to KeyName(pathElement.name)
        }
    },
    kind = kind,
    idOrName = if (hasId()) { KeyID(id) } else { KeyName(name) }
)
