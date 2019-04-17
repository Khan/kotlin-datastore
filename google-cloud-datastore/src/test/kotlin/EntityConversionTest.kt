package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Blob
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.EntityValue
import com.google.cloud.datastore.IncompleteKey
import com.google.cloud.datastore.LatLng
import com.google.cloud.datastore.NullValue
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.TimestampValue
import com.google.cloud.datastore.Value
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.metadata.GeoPt
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Property
import org.khanacademy.metadata.StructuredProperty
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
    val aGeoPt: GeoPt?,
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

data class LocalStructuredProperty(
    val innerProp1: String,
    val innerProp2: LocalDateTime
) : Property

data class StructuredPropertyWithRepeatedField(
    val innerProp1: String,
    val innerProp2: List<LocalDateTime>
) : Property

data class EntityPropertyModel(
    val basic: String,
    val structured: LocalStructuredProperty,
    val repeatedStructured: List<LocalStructuredProperty>,
    override val key: Key<EntityPropertyModel>
) : Keyed<EntityPropertyModel>

data class StructuredPropertyModel(
    val basic: String,
    val structured: StructuredProperty<LocalStructuredProperty>,
    val repeatedStructured: List<StructuredProperty<LocalStructuredProperty>>,
    override val key: Key<StructuredPropertyModel>
) : Keyed<StructuredPropertyModel>

data class RepeatedFieldStructuredPropertyModel(
    val basic: String,
    val structured: StructuredProperty<StructuredPropertyWithRepeatedField>,
    override val key: Key<RepeatedFieldStructuredPropertyModel>
) : Keyed<RepeatedFieldStructuredPropertyModel>

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

    "It converts (ndb-style) structured properties correctly" {
        val testKey = Key<StructuredPropertyModel>(
            "StructuredPropertyTestModel", "the-first-one")
        val innerDateTime = LocalDateTime.ofEpochSecond(
            1006L, 0, ZoneOffset.UTC)
        val instance = StructuredPropertyModel(
            basic = "basic_value",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "inner_string",
                innerProp2 = innerDateTime
            )),
            repeatedStructured = listOf(), // Tested separately
            key = testKey
        )
        val entity = instance.toDatastoreEntity()

        entity.getString("basic") shouldBe "basic_value"
        entity.getString("structured.innerProp1") shouldBe "inner_string"
        entity.getTimestamp("structured.innerProp2").seconds shouldBe 1006L
        ("innerProp1" in entity) shouldBe false
        ("innerProp2" in entity) shouldBe false

        val restored = entity.toTypedModel(StructuredPropertyModel::class)
        restored.basic shouldBe "basic_value"
        restored.structured.value.innerProp1 shouldBe "inner_string"
        restored.structured.value.innerProp2 shouldBe innerDateTime
    }

    "It converts (ndb-style) repeated structured properties correctly" {
        val testKey = Key<StructuredPropertyModel>(
            "StructuredPropertyTestModel", "the-first-one")
        val innerDateTime = LocalDateTime.ofEpochSecond(
            1006L, 0, ZoneOffset.UTC)
        val instance = StructuredPropertyModel(
            basic = "basic_value",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "inner_string",
                innerProp2 = innerDateTime
            )),
            repeatedStructured = listOf(
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "repeat1_inner_string",
                    innerProp2 = innerDateTime
                )),
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "repeat2_inner_string",
                    innerProp2 = innerDateTime
                ))
            ),
            key = testKey
        )
        val entity = instance.toDatastoreEntity()

        entity.getString("basic") shouldBe "basic_value"
        entity.getList<StringValue>("repeatedStructured.innerProp1")
            .map { it.get() } shouldBe listOf(
            "repeat1_inner_string", "repeat2_inner_string")

        entity.getList<TimestampValue>("repeatedStructured.innerProp2")
            .map { it.get().seconds } shouldBe listOf(
            1006L, 1006L)

        val restored = entity.toTypedModel(StructuredPropertyModel::class)
        restored.basic shouldBe "basic_value"
        restored.repeatedStructured.map { it.value.innerProp1 } shouldBe
            listOf("repeat1_inner_string", "repeat2_inner_string")
        restored.repeatedStructured.map { it.value.innerProp2 } shouldBe
            listOf(innerDateTime, innerDateTime)
    }

    "It converts (ndb-style) structured properties with repeated fields" {
        val testKey = Key<RepeatedFieldStructuredPropertyModel>(
            "RepeatedFieldStructuredPropertyModel", "the-first-one")
        val innerDateTime1 = LocalDateTime.ofEpochSecond(
            1007L, 0, ZoneOffset.UTC)
        val innerDateTime2 = LocalDateTime.ofEpochSecond(
            1008L, 0, ZoneOffset.UTC)
        val instance = RepeatedFieldStructuredPropertyModel(
            basic = "a_string",
            structured = StructuredProperty(
                StructuredPropertyWithRepeatedField(
                    innerProp1 = "inner_string",
                    innerProp2 = listOf(innerDateTime1, innerDateTime2))
            ),
            key = testKey
        )
        val entity = instance.toDatastoreEntity()

        entity.getString("basic") shouldBe "a_string"
        entity.getString("structured.innerProp1") shouldBe "inner_string"
        entity.getList<TimestampValue>("structured.innerProp2").map {
            it.get().seconds
        } shouldBe listOf(1007L, 1008L)

        val restored = entity.toTypedModel(
            RepeatedFieldStructuredPropertyModel::class)
        restored.basic shouldBe "a_string"
        restored.structured.value.innerProp1 shouldBe "inner_string"
        restored.structured.value.innerProp2 shouldBe
            listOf(innerDateTime1, innerDateTime2)
    }
})
