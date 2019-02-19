package org.khanacademy.datastore

import com.google.cloud.datastore.Entity
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Meta

data class AnnotationTestModel(
    @Meta(name = "a_string")
    val aString: String,
    override val key: Key<AnnotationTestModel>
) : Keyed<AnnotationTestModel>

class ModelAnnotationTest : StringSpec({
    "When converting from an Entity, it should use the annotated name" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "AnnotationTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("a_string", "abcd")
            .build()

        entity.toTypedModel(AnnotationTestModel::class).aString shouldBe "abcd"
    }

    "It should throw rather than use the unannotated name" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "AnnotationTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aString", "abcd")
            .build()

        shouldThrow<NullPointerException> {
            entity.toTypedModel(AnnotationTestModel::class)
        }
    }
})
