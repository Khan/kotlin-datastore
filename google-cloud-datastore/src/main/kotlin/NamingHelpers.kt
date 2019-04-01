/**
 * Helpers for translating between kotlin and datastore property names.
 */
package org.khanacademy.datastore

import org.khanacademy.metadata.Meta
import kotlin.reflect.KAnnotatedElement
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.valueParameters

/**
 * Read the name out of a @Meta annotation, if present.
 */
internal fun metaAnnotationName(element: KAnnotatedElement): String? {
    val annotName = element.findAnnotation<Meta>()?.name
    return if (annotName != null && annotName != "") {
        annotName
    } else {
        null
    }
}

/**
 * Find the name for the given parameter in the datastore.
 *
 * Prefer a `@Meta(name = ...)` annotation if present; otherwise, fallback on
 * the name in code.
 */
internal fun datastoreName(kParameter: KParameter): String? =
    metaAnnotationName(kParameter) ?: kParameter.name

/**
 * Get the datastore name for a property by name.
 *
 * Prefer a `@Meta(name = ...)` annotation if present; otherwise, fall back on
 * the provided name.
 *
 * Note that because annotations for all properties except computed properties
 * go on constructor parameters, we need to check both the constructor
 * parameters and properties to cover both cases.
 */
internal fun kotlinNameToDatastoreName(
    cls: KClass<*>,
    kotlinName: String
): String {
    val parameter = assertedPrimaryConstructor(cls)
        .valueParameters.firstOrNull {
        it.name == kotlinName
    }

    val property = cls.memberProperties.firstOrNull {
        it.name == kotlinName
    }

    return parameter?.let { metaAnnotationName(it) }
        ?: property?.let { metaAnnotationName(it) }
        ?: kotlinName
}
