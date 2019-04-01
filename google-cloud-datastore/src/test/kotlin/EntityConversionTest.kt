package org.khanacademy.datastore

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.EntityValue
import com.google.cloud.datastore.IncompleteKey
import com.google.cloud.datastore.LatLng
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.Value
import io.kotlintest.matchers.string.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.metadata.GeoPt
import org.khanacademy.metadata.JsonProperty
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Property
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset

data class PrimitiveTestModel(
    val aKey: Key<SecondaryTestModel>?,
    val aString: String?,
    val aLong: Long?,
    val aBool: Boolean?,
    val aDouble: Double?,
    val someBytes: ByteArray?,
    val aGeoPt: GeoPt?,
    val aTimestamp: LocalDateTime?,
    val aDate: LocalDate?,
    val aTime: LocalTime?,
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

data class LocalStructuredProperty(
    val innerProp1: String,
    val innerProp2: LocalDateTime
) : Property

data class EntityPropertyModel(
    val basic: String,
    val structured: LocalStructuredProperty,
    val repeatedStructured: List<LocalStructuredProperty>,
    override val key: Key<EntityPropertyModel>
) : Keyed<EntityPropertyModel>

data class JsonPropertyValue(
    val innerString: String,
    val innerInt: Int,

    @com.fasterxml.jackson.annotation.JsonProperty("inner_nullable_string")
    val innerNullableString: String?
)

data class JsonPropertyModel(
    val json: JsonProperty<JsonPropertyValue>,
    override val key: Key<JsonPropertyModel>
) : Keyed<JsonPropertyModel>

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
            .set("aDate", Timestamp.ofTimeSecondsAndNanos(86400, 0))
            .set("aTime", Timestamp.ofTimeSecondsAndNanos(63, 0))
            .set("aGeoPt", LatLng.of(1.0, 2.0))
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
        converted.aDate shouldBe LocalDate.of(1970, 1, 2)
        converted.aTime shouldBe LocalTime.of(0, 1, 3)
        converted.aGeoPt shouldBe GeoPt(latitude = 1.0, longitude = 2.0)
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
            .set("aDate", NullValue())
            .set("aTime", NullValue())
            .set("aGeoPt", NullValue())
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
        converted.aDate shouldBe null
        converted.aTime shouldBe null
        converted.aGeoPt shouldBe null
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
            aDate = LocalDate.of(2019, 4, 17),
            aTime = LocalTime.of(0, 1, 2),
            aGeoPt = GeoPt(latitude = 3.0, longitude = 4.0),
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
        entity.getTimestamp("aDate").seconds shouldBe
            LocalDateTime.of(2019, 4, 17, 0, 0, 0)
                .toEpochSecond(ZoneOffset.UTC)
        entity.getTimestamp("aTime").seconds shouldBe 62
        entity.getLatLng("aGeoPt").latitude shouldBe 3.0
        entity.getLatLng("aGeoPt").longitude shouldBe 4.0
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
            aDate = LocalDate.of(2019, 4, 17),
            aTime = LocalTime.of(0, 1, 2),
            aGeoPt = null,
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

    "It converts local structured (entity) properties correctly" {
        val testKey = Key<EntityPropertyModel>(
            "EntityPropertyModel", "the-first-one")
        val innerDateTime = LocalDateTime.ofEpochSecond(
            1005L, 0, ZoneOffset.UTC)
        val instance = EntityPropertyModel(
            basic = "a_string",
            structured = LocalStructuredProperty(
                innerProp1 = "inner_string",
                innerProp2 = innerDateTime
            ),
            repeatedStructured = listOf(
                LocalStructuredProperty("repeat_1", innerDateTime),
                LocalStructuredProperty("repeat_2", innerDateTime)
            ),
            key = testKey
        )
        val entity = instance.toDatastoreEntity()

        val inner = entity.getEntity<IncompleteKey>("structured")
        inner.getString("innerProp1") shouldBe "inner_string"
        inner.getTimestamp("innerProp2").seconds shouldBe 1005L
        entity.getString("basic") shouldBe "a_string"
        val innerRepeated = entity.getList<EntityValue>("repeatedStructured")
        innerRepeated.size shouldBe 2
        innerRepeated[0].get().getString("innerProp1") shouldBe "repeat_1"
        innerRepeated[1].get().getString("innerProp1") shouldBe "repeat_2"

        val restored = entity.toTypedModel(EntityPropertyModel::class)
        restored.basic shouldBe "a_string"
        restored.structured.innerProp1 shouldBe "inner_string"
        restored.structured.innerProp2 shouldBe innerDateTime
        restored.repeatedStructured.size shouldBe 2
        restored.repeatedStructured[0].innerProp1 shouldBe "repeat_1"
        restored.repeatedStructured[1].innerProp1 shouldBe "repeat_2"
    }

    "It converts json to a JsonProperty" {
        val testKey = Key<JsonPropertyModel>(
            "JsonPropertyModel", "the-first-one")
        val jsonValue = """{"innerString": "abcde", "innerInt": 1,""" +
            """"inner_nullable_string": null}"""
        val entity = Entity.newBuilder(testKey.toDatastoreKey())
            .set("json", jsonValue)
            .build()
        val restored = entity.toTypedModel(JsonPropertyModel::class)
        restored.json.value.innerString shouldBe "abcde"
        restored.json.value.innerInt shouldBe 1
        restored.json.value.innerNullableString shouldBe null
    }

    "It round-trips a JsonProperty" {
        val testKey = Key<JsonPropertyModel>(
            "JsonPropertyModel", "the-first-one")
        val instance = JsonPropertyModel(
            JsonProperty(JsonPropertyValue("efghi", 10, null)), testKey)
        val entity = instance.toDatastoreEntity()
        entity.getString("json") shouldContain "efghi"

        val restored = entity.toTypedModel(JsonPropertyModel::class)
        restored shouldBe instance
    }

    "It throws on invalid JSON" {
        val testKey = Key<JsonPropertyModel>(
            "JsonPropertyModel", "the-first-one")
        val jsonValue = """{"innerString" "abcde" "innerInt" 1""" +
            """"inner_nullable_string", null}"""
        val entity = Entity.newBuilder(testKey.toDatastoreKey())
            .set("json", jsonValue)
            .build()
        shouldThrow<JsonParseException> {
            entity.toTypedModel(JsonPropertyModel::class)
        }
    }

    "It throws on JSON that doesn't match the expected schema" {
        val testKey = Key<JsonPropertyModel>(
            "JsonPropertyModel", "the-first-one")
        val jsonValue = """{"innerString": 3, "innerInt": "abcde",""" +
            """"inner_nullable_string": null}"""
        val entity = Entity.newBuilder(testKey.toDatastoreKey())
            .set("json", jsonValue)
            .build()
        shouldThrow<InvalidFormatException> {
            entity.toTypedModel(JsonPropertyModel::class)
        }
    }
})
