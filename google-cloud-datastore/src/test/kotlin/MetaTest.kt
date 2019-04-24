package org.khanacademy.datastore

import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.StringValue
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.runBlocking
import org.khanacademy.datastore.testutil.withMockDatastore
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Meta
import org.khanacademy.metadata.Readonly

data class AnnotationTestModel(
    @Meta(name = "a_string")
    val aString: String,
    override val key: Key<AnnotationTestModel>
) : Keyed<AnnotationTestModel>

data class ComputedAnnotationTestModel(
    val aString: String,
    override val key: Key<ComputedAnnotationTestModel>
) : Keyed<ComputedAnnotationTestModel> {

    @Meta(name = "computed_property")
    val computedProperty = aString + "_computed"
}

data class IndexedAnnotationTestModel(
    @Meta(indexed = true)
    val anIndexedProperty: String,

    @Meta(indexed = false)
    val anUnindexedProperty: String,

    val anUnspecifiedPropertyDefaultingFalse: String,
    override val key: Key<IndexedAnnotationTestModel>
) : Keyed<IndexedAnnotationTestModel>

@Readonly
data class ReadonlyTestModel(
    val aString: String,
    override val key: Key<ReadonlyTestModel>
) : Keyed<ReadonlyTestModel>

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

        shouldThrow<IllegalArgumentException> {
            entity.toTypedModel(AnnotationTestModel::class)
        }
    }

    "When converting to an Entity, it should use the annotated name" {
        val key = Key<AnnotationTestModel>(
            "AnnotatedTestModel", "the-first-one")
        val model = AnnotationTestModel(
            aString = "abcd",
            key = key
        )

        val entity = model.toDatastoreEntity()
        entity.getString("a_string") shouldBe "abcd"
        ("aString" in entity) shouldBe false
    }

    "It should use the annotated name for computed properties" {
        val key = Key<ComputedAnnotationTestModel>(
            "ComputedAnnotationTestModel", "the-first-one")
        val model = ComputedAnnotationTestModel(
            aString = "abcd",
            key = key
        )

        val entity = model.toDatastoreEntity()
        entity.getString("computed_property") shouldBe "abcd_computed"
        ("computedProperty" in entity) shouldBe false
    }

    "It should set indexed true/false based on the annotation" {
        val key = Key<IndexedAnnotationTestModel>(
            "IndexedAnnotationTestModel", "the-first-one")
        val model = IndexedAnnotationTestModel(
            anIndexedProperty = "indexed",
            anUnindexedProperty = "unindexed",
            anUnspecifiedPropertyDefaultingFalse = "default",
            key = key
        )

        val entity = model.toDatastoreEntity()
        entity.getValue<StringValue>("anIndexedProperty")
            .excludeFromIndexes() shouldBe false
        entity.getValue<StringValue>("anUnindexedProperty")
            .excludeFromIndexes() shouldBe true
        entity.getValue<StringValue>(
            "anUnspecifiedPropertyDefaultingFalse")
            .excludeFromIndexes() shouldBe true
    }

    "It should throw when attempting to put a @Readonly model" {
        val key = Key<ReadonlyTestModel>("ReadonlyTestModel", "the-first-one")
        val model = ReadonlyTestModel(aString = "abcd", key = key)
        withMockDatastore(listOf()) {
            shouldThrow<ReadonlyModelException> {
                DB.put(model)
            }
            shouldThrow<ReadonlyModelException> {
                DB.putMulti(model, model)
            }
            shouldThrow<ReadonlyModelException> {
                DB.putMulti(model, model, model)
            }
            shouldThrow<ReadonlyModelException> {
                DB.putMulti(model, model, model, model)
            }
            shouldThrow<ReadonlyModelException> {
                runBlocking {
                    DB.putAsync(model).await()
                }
            }
        }
    }
})
