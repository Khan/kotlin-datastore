package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.Value
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
    val aLong: Long?,
    val aBool: Boolean?,
    val aDouble: Double?,
    val someBytes: ByteArray?,
    val aTimestamp: LocalDateTime?,
    val nonNullableBool: Boolean,
    val nonNullableBytes: ByteArray,
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

data class DefaultValueModel(
    val aStringWithDefault: String = "default_value",
    val aStringWithNullDefault: String? = null,
    override val key: Key<DefaultValueModel>
) : Keyed<DefaultValueModel>

data class RepeatedValueModel(
    val aRepeatedValue: List<String>,
    val aRepeatedNullableValue: List<String?>,
    val aRepeatedTimestampValue: List<LocalDateTime?>,
    override val key: Key<RepeatedValueModel>
) : Keyed<RepeatedValueModel>

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
            .set("nonNullableBool", false)
            .set("nonNullableBytes", Blob.copyFrom("hijklmn".toByteArray()))
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
        String(converted.someBytes ?: ByteArray(0)) shouldBe "abcdefg"
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
            .set("nonNullableBool", NullValue())
            .set("aDouble", 2.71828)
            .set("nonNullableBytes", NullValue())
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
        shouldThrow<IllegalArgumentException> {
            entity.toTypedModel(PrimitiveTestModel::class)
        }
    }

    "It should throw if the entity has a missing nullable property" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aLong", 4L)
            .set("aBool", true)
            .set("aDouble", 2.71828)
            .set("someBytes", Blob.copyFrom("abcdefg".toByteArray()))
            .build()
        shouldThrow<IllegalArgumentException> {
            entity.toTypedModel(PrimitiveTestModel::class)
        }
    }

    "It should use null if the entity has a present nullable property" {
        val datastoreKey = DatastoreKey.newBuilder(
            "test-project", "PrimitiveTestModel", "the-first-one").build()
        val entity = Entity.newBuilder(datastoreKey)
            .set("aKey", NullValue())
            .set("aString", NullValue())
            .set("aLong", NullValue())
            .set("aBool", NullValue())
            .set("aDouble", NullValue())
            .set("someBytes", NullValue())
            .set("aTimestamp", NullValue())
            .set("nonNullableBool", true)
            .set("nonNullableBytes", Blob.copyFrom("not null!".toByteArray()))
            .build()
        val converted = entity.toTypedModel(PrimitiveTestModel::class)
        converted.aKey shouldBe null
        converted.aString shouldBe null
        converted.aBool shouldBe null
        converted.aDouble shouldBe null
        converted.someBytes shouldBe null
        converted.aTimestamp shouldBe null
    }

    "It should correctly transfer basic fields to the entity" {
        val testKey = Key<PrimitiveTestModel>(
            "PrimitiveTestModel", "the-first-one")
        val keyProp = Key<SecondaryTestModel>(
            "SecondaryTestModel", "key-prop")
        val testModel = PrimitiveTestModel(
            aKey = keyProp,
            aString = "string_value",
            aLong = 4L,
            aBool = true,
            aDouble = 9.0,
            someBytes = "byte_value".toByteArray(),
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            nonNullableBool = false,
            nonNullableBytes = "byte_value_2".toByteArray(),
            key = testKey)

        val entity = testModel.toDatastoreEntity()
        entity.getString("aString") shouldBe "string_value"
        entity.getLong("aLong") shouldBe 4L
        entity.getBoolean("aBool") shouldBe true
        entity.getDouble("aDouble") shouldBe 9.0
        String(entity.getBlob("someBytes").toByteArray()) shouldBe "byte_value"
        entity.getTimestamp("aTimestamp").seconds shouldBe 0
        entity.getBoolean("nonNullableBool") shouldBe false
        String(entity.getBlob("nonNullableBytes").toByteArray()) shouldBe
            "byte_value_2"
        entity.key.kind shouldBe "PrimitiveTestModel"
        entity.key.name shouldBe "the-first-one"
        entity.getKey("aKey").kind shouldBe "SecondaryTestModel"
        entity.getKey("aKey").name shouldBe "key-prop"
    }

    "It will explicitly set null when transferring to the entity" {
        val testKey = Key<PrimitiveTestModel>(
            "PrimitiveTestModel", "the-first-one")
        val testModel = PrimitiveTestModel(
            aKey = null,
            aString = null,
            aLong = 4L,
            aBool = true,
            aDouble = 9.0,
            someBytes = "byte_value".toByteArray(),
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            nonNullableBool = false,
            nonNullableBytes = "byte_value_2".toByteArray(),
            key = testKey)

        val entity = testModel.toDatastoreEntity()
        ("aString" in entity) shouldBe true
        entity.getString("aString") shouldBe null
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

    "It will use the default value if a property is missing in the datastore" {
        val testKey = Key<DefaultValueModel>(
            "DefaultValueModel", "the-first-one")
        val entity = Entity.newBuilder(testKey.toDatastoreKey())
            .build()
        val result = entity.toTypedModel(DefaultValueModel::class)
        result.aStringWithDefault shouldBe "default_value"
        result.aStringWithNullDefault shouldBe null
    }

    "It will set default values in the datastore, not just on the object" {
        val testKey = Key<DefaultValueModel>(
            "DefaultValueModel", "the-first-one")
        val entity = DefaultValueModel(key = testKey).toDatastoreEntity()
        entity.getString("aStringWithDefault") shouldBe "default_value"
        entity.getString("aStringWithNullDefault") shouldBe null
    }

    "It converts basic repeated values correctly" {
        val testKey = Key<RepeatedValueModel>(
            "RepeatedValueModel", "the-first-one")
        val instance = RepeatedValueModel(
            key = testKey,
            aRepeatedValue = listOf("a", "b", "c"),
            aRepeatedNullableValue = listOf(),
            aRepeatedTimestampValue = listOf()
        )

        val entity = instance.toDatastoreEntity()

        entity.getList<StringValue>("aRepeatedValue").map { it.get() } shouldBe
            listOf("a", "b", "c")

        val restored = entity.toTypedModel(RepeatedValueModel::class)
        restored.aRepeatedValue shouldBe listOf("a", "b", "c")
        restored.aRepeatedNullableValue shouldBe listOf()
    }

    "It allows null items for repeated nullable values" {
        val testKey = Key<RepeatedValueModel>(
            "RepeatedValueModel", "the-first-one")
        val instance = RepeatedValueModel(
            key = testKey,
            aRepeatedValue = listOf(),
            aRepeatedNullableValue = listOf("a", null, "c"),
            aRepeatedTimestampValue = listOf()
        )
        val entity = instance.toDatastoreEntity()

        entity.getList<Value<*>>("aRepeatedNullableValue")
            .map { it.get() } shouldBe listOf("a", null, "c")

        val restored = entity.toTypedModel(RepeatedValueModel::class)
        restored.aRepeatedValue shouldBe listOf()
        restored.aRepeatedNullableValue shouldBe listOf("a", null, "c")
    }

    "It converts complex (timestamp) repeated values correctly" {
        val epochSeconds = 1004L
        val localDateTime = LocalDateTime.ofEpochSecond(
            epochSeconds, 0, ZoneOffset.UTC)
        val timestamp = Timestamp.ofTimeSecondsAndNanos(epochSeconds, 0)

        val testKey = Key<RepeatedValueModel>(
            "RepeatedValueModel", "the-first-one")
        val instance = RepeatedValueModel(
            key = testKey,
            aRepeatedValue = listOf(),
            aRepeatedNullableValue = listOf(),
            aRepeatedTimestampValue = listOf(localDateTime)
        )
        val entity = instance.toDatastoreEntity()

        entity.getList<Value<*>>("aRepeatedTimestampValue")
            .map { it.get() } shouldBe listOf(timestamp)

        val restored = entity.toTypedModel(RepeatedValueModel::class)
        restored.aRepeatedTimestampValue shouldBe listOf(localDateTime)
    }
})
