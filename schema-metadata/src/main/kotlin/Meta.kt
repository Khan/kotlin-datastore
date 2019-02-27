package org.khanacademy.metadata

/**
 * Annotation for specifying metadata on properties of datastore models.
 *
 * Currently only lets you specify an alternate name under which a property
 * will be stored.
 *
 * TODO(colin): implement indexing as well.
 */
// Note: the order of targets here matters. PROPERTY is only here for computed
// property renaming, and if it comes first, every @Meta in a data class
// constructor will have to use @param:Meta which is annoying.
@Target(AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.PROPERTY)
annotation class Meta(
    // The name under which a property will be stored in the datastore.
    // We have to use "" as the default because `null` is not an allowed value
    // for annotations.
    val name: String = ""
)
