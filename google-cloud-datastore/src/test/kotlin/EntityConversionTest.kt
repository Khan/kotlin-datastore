package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.NullValue
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

data class PrimitiveTestModel(
    val aKey: Key<SecondaryTestModel>?,
    val aString: String?,
    val aLong: Long,
    val aBool: Boolean,
    val aDouble: Double,
    val someBytes: ByteArray,
    val aTimestamp: LocalDateTime?,
    override val key: Key<PrimitiveTestModel>
) : Keyed<PrimitiveTestModel>

data class ComputedTestModel(
    val aString: String,
    override val key: Key<ComputedTestModel>
) : Keyed<ComputedTestModel> {
    val computed1 = aString + "_first"
    val computed2 = aString + "_second"
}

data class SecondaryTestModel(
    override val key: Key<SecondaryTestModel>
) : Keyed<SecondaryTestModel>

class EntityConversionTest : StringSpec({
    "It should correctly convert basic fields" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val keyProp = DatastoreKey.newBuilder(
            "test-project", "SecondaryTestModel", "key-prop").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aKey", keyProp)
            .set("aString", "abcd")
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("aTimestamp", Timestamp.ofTimeSecondsAndNanos(0, 0))
            .set("someBytes", Blob.copyFrom("abcdefg".toByteArray()))
            .build()

        val converted = entity.toTypedModel(PrimitiveTestModel::class)
        converted.key.kind shouldBe "PrimitiveTestModel"
        converted.key.idOrName shouldBe KeyName("the-first-one")
        converted.aKey shouldBe keyProp.toKey<SecondaryTestModel>()
        converted.aString shouldBe "abcd"
        converted.aLong shouldBe 4L
        converted.aBool shouldBe true
        converted.aDouble shouldBe 2.71828
        converted.aTimestamp shouldBe LocalDateTime.ofInstant(
            Instant.EPOCH,
            ZoneId.of("UTC"))
        String(converted.someBytes) shouldBe "abcdefg"
    }

    "It should throw if the entity doesn't match the data class's type" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aString", "abcd")
            .set("aLong", "not actually a Long")
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("someBytes", Blob.copyFrom("abcdefg".toByteArray()))
            .build()
        shouldThrow<ClassCastException> {
            entity.toTypedModel(PrimitiveTestModel::class)
        }
    }

    "It should throw if the entity has an unexpected null" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aString", "abcd")
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("someBytes", NullValue())
            .build()
        shouldThrow<NullPointerException> {
            entity.toTypedModel(PrimitiveTestModel::class)
        }
    }

    "It should throw if the entity has an unexpectedly missing property" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aString", "abcd")
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .build()
        shouldThrow<NullPointerException> {
            entity.toTypedModel(PrimitiveTestModel::class)
        }
    }

    "It should use null if the entity has a missing nullable property" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("someBytes", Blob.copyFrom("abcdefg".toByteArray()))
            .build()
        val converted = entity.toTypedModel(PrimitiveTestModel::class)
        converted.aKey shouldBe null
        converted.aString shouldBe null
        converted.aTimestamp shouldBe null
    }

    "It should use null if the entity has a present nullable property" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aKey", NullValue())
            .set("aString", NullValue())
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("someBytes", Blob.copyFrom("abcdefg".toByteArray()))
            .set("aTimestamp", NullValue())
            .build()
        val converted = entity.toTypedModel(PrimitiveTestModel::class)
        converted.aKey shouldBe null
        converted.aString shouldBe null
        converted.aTimestamp shouldBe null
    }

    "It should correctly transfer basic fields to the entity" {
        val testKey = Key<PrimitiveTestModel>(
            "PrimitiveTestModel", "the-first-one")
        val testModel = PrimitiveTestModel(
            aString = "string_value",
            aLong = 4L,
            aBool = true,
            aDouble = 9.0,
            someBytes = "byte_value".toByteArray(),
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            key = testKey)

        val entity = testModel.toDatastoreEntity()
        entity.getString("aString") shouldBe "string_value"
        entity.getLong("aLong") shouldBe 4L
        entity.getBoolean("aBool") shouldBe true
        entity.getDouble("aDouble") shouldBe 9.0
        String(entity.getBlob("someBytes").toByteArray()) shouldBe "byte_value"
        entity.getTimestamp("aTimestamp").seconds shouldBe 0
        entity.key.kind shouldBe "PrimitiveTestModel"
        entity.key.name shouldBe "the-first-one"
    }

    "It will skip null when transferring to the entity" {
        val testKey = Key<PrimitiveTestModel>(
            "PrimitiveTestModel", "the-first-one")
        val testModel = PrimitiveTestModel(
            aString = null,
            aLong = 4L,
            aBool = true,
            aDouble = 9.0,
            someBytes = "byte_value".toByteArray(),
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            key = testKey)

        val entity = testModel.toDatastoreEntity()
        ("aString" in entity) shouldBe false
    }

    "It will transfer computed properties to the entity" {
        val testKey = Key<ComputedTestModel>(
            "ComputedTestModel", "the-first-one")
        val testModel = ComputedTestModel(
            aString = "a_string_value",
            key = testKey
        )

        val entity = testModel.toDatastoreEntity()
        entity.getString("computed1") shouldBe "a_string_value_first"
        entity.getString("computed2") shouldBe "a_string_value_second"
    }
})
