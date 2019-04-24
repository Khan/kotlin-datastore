package org.khanacademy.metadata

/**
 * Annotation to indicate whether to disallow puts of this model.
 *
 * You may have a model that is written from another language and read in
 * kotlin code. It may be the case that you only need to access some of the
 * properties of the model from your kotlin code. You can write a kotlin
 * dataclass for that model that mentions only the properties you care about,
 * which will work fine for gets (and in fact be slightly more efficient, as
 * well as cleaner code), but that will erase the missing properties on put().
 * To avoid accidentally erasing properties, you can mark a class @Readonly,
 * which disables put()s on that model.
 */
@Target(AnnotationTarget.CLASS)
annotation class Readonly
