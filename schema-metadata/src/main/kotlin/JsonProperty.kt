package org.khanacademy.metadata

/**
 * A nested entity-like property that serializes its values as a json string.
 *
 * Note that unlike for model classes, renaming is done via jackson's
 * standard annotations, rather than via @Meta. E.g.,
 * @JsonProperty("a_string") val aString: String
 *
 * TODO(colin): do we ever expect models to be used in both contexts, such that
 * this would be particularly annoying?
 */
data class JsonProperty<T>(val value: T)
