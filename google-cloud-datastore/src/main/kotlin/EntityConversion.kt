/**
 * Functions for converting google datastore entities to our model classes.
 */
package org.khanacademy.datastore

import com.google.cloud.datastore.Entity
import org.khanacademy.metadata.Keyed

/**
 * Convert a Google cloud datastore entity to one of our model classes.
 *
 * This uses reflection internally to accomplish this and will throw if the
 * entity doesn't conform to the model's schema.
 *
 * TODO(colin): support generating non-reflective converters using annotation
 * processors.
 */
fun <T : Keyed<T>> Entity.toTypedModel(): T {
    TODO()
}
