package org.khanacademy.datastore

import com.google.cloud.Timestamp
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.StringValue
import com.google.cloud.datastore.TimestampValue
import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.datastore.testutil.withMockDatastore
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Property
import org.khanacademy.metadata.StructuredProperty
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

data class StructuredPropertyWithRepeatedField(
    val innerProp1: String,
    val innerProp2: List<LocalDateTime>
) : Property

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

// Invalid because optional fields are not allowed on repeated structured
// properties (and this is used repeated, below)
data class InvalidStructuredProperty(
    val innerProp1: String = "abcd"
) : Property

data class InvalidStructuredPropertyModel(
    val structured: List<StructuredProperty<InvalidStructuredProperty>>,
    override val key: Key<InvalidStructuredPropertyModel>
) : Keyed<InvalidStructuredPropertyModel>

data class InvalidDoublyRepeatedModel(
    val structured: List<StructuredProperty<
        StructuredPropertyWithRepeatedField>>,
    override val key: Key<InvalidDoublyRepeatedModel>
) : Keyed<InvalidDoublyRepeatedModel>

data class NestedStructuredProperty(
    val inner: StructuredProperty<LocalStructuredProperty>
) : Property

data class UnsupportedNestedModel(
    val structured: StructuredProperty<NestedStructuredProperty>,
    override val key: Key<UnsupportedNestedModel>
) : Keyed<UnsupportedNestedModel>

class StructuredPropertyTest : StringSpec({
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

    "It queries (ndb-style) structured properties correctly" {
        val testKey = Key<StructuredPropertyModel>(
            "StructuredPropertyModel", "the-first-one")
        val secondTestKey = Key<StructuredPropertyModel>(
            "StructuredPropertyModel", "the-second-one")

        val innerDateTime = LocalDateTime.ofEpochSecond(
            1009L, 0, ZoneOffset.UTC)

        val testModels = listOf(StructuredPropertyModel(
            basic = "a_string",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "first_inner",
                innerProp2 = innerDateTime
            )),
            repeatedStructured = listOf(),
            key = testKey
        ), StructuredPropertyModel(
            basic = "another_string",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "second_inner",
                innerProp2 = innerDateTime + Duration.ofSeconds(1)
            )),
            repeatedStructured = listOf(),
            key = secondTestKey
        ))

        withMockDatastore(testModels) {
            val result =
                DB.query<StructuredPropertyModel>("StructuredPropertyModel") {
                    "structured.innerProp1" eq "second_inner"
                }.toList()
            val anotherResult =
                DB.query<StructuredPropertyModel>("StructuredPropertyModel") {
                    "structured.innerProp2" lt
                        innerDateTime + Duration.ofSeconds(1)
                }.toList()

            result.size shouldBe 1
            (result[0].key.idOrName as KeyName).value shouldBe "the-second-one"

            anotherResult.size shouldBe 1
            (anotherResult[0].key.idOrName as KeyName).value shouldBe
                "the-first-one"
        }
    }

    "It queries repeated (ndb-style) structured properties correctly" {
        val testKey = Key<StructuredPropertyModel>(
            "StructuredPropertyModel", "the-first-one")
        val secondTestKey = Key<StructuredPropertyModel>(
            "StructuredPropertyModel", "the-second-one")

        val innerDateTime = LocalDateTime.ofEpochSecond(
            1009L, 0, ZoneOffset.UTC)

        val testModels = listOf(StructuredPropertyModel(
            basic = "a_string",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "first_inner",
                innerProp2 = innerDateTime
            )),
            repeatedStructured = listOf(
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "first_inner_1",
                    innerProp2 = innerDateTime
                )),
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "first_inner_2",
                    innerProp2 = innerDateTime
                ))
            ),
            key = testKey
        ), StructuredPropertyModel(
            basic = "another_string",
            structured = StructuredProperty(LocalStructuredProperty(
                innerProp1 = "second_inner",
                innerProp2 = innerDateTime + Duration.ofSeconds(1)
            )),
            repeatedStructured = listOf(
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "second_inner_1",
                    innerProp2 = innerDateTime
                )),
                StructuredProperty(LocalStructuredProperty(
                    innerProp1 = "second_inner_2",
                    innerProp2 = innerDateTime
                ))
            ),
            key = secondTestKey
        ))

        withMockDatastore(testModels) {
            val result =
                DB.query<StructuredPropertyModel>("StructuredPropertyModel") {
                    "repeatedStructured.innerProp1" eq "second_inner_2"
                }.toList()
            val anotherResult =
                DB.query<StructuredPropertyModel>("StructuredPropertyModel") {
                    "repeatedStructured.innerProp1" gt "first_inner_1"
                }.toList()

            result.size shouldBe 1
            (result[0].key.idOrName as KeyName).value shouldBe "the-second-one"

            anotherResult.size shouldBe 2
            val keyNames = anotherResult
                .map { (it.key.idOrName as KeyName).value }
            keyNames shouldContain "the-first-one"
            keyNames shouldContain "the-second-one"
        }
    }

    "It queries (ndb-style) structured properties with repeated fields" {
        val testKey = Key<RepeatedFieldStructuredPropertyModel>(
            "RepeatedFieldStructuredPropertyModel", "the-first-one")
        val secondTestKey = Key<RepeatedFieldStructuredPropertyModel>(
            "RepeatedFieldStructuredPropertyModel", "the-second-one")
        val innerDateTime1 = LocalDateTime.ofEpochSecond(
            1007L, 0, ZoneOffset.UTC)
        val innerDateTime2 = LocalDateTime.ofEpochSecond(
            1008L, 0, ZoneOffset.UTC)

        val instances = listOf(RepeatedFieldStructuredPropertyModel(
            basic = "a_string",
            structured = StructuredProperty(
                StructuredPropertyWithRepeatedField(
                    innerProp1 = "inner_string",
                    innerProp2 = listOf(innerDateTime1, innerDateTime2))
            ),
            key = testKey
        ), RepeatedFieldStructuredPropertyModel(
            basic = "another_string",
            structured = StructuredProperty(
                StructuredPropertyWithRepeatedField(
                    innerProp1 = "inner_string_2",
                    innerProp2 = listOf(
                        innerDateTime1 + Duration.ofSeconds(10),
                        innerDateTime2 + Duration.ofSeconds(10)
                    )
                )
            ),
            key = secondTestKey
        ))

        withMockDatastore(instances) {
            val result =
                DB.query<RepeatedFieldStructuredPropertyModel>(
                    "RepeatedFieldStructuredPropertyModel"
                ) {
                    "structured.innerProp2" eq innerDateTime2
                }.toList()
            val anotherResult =
                DB.query<RepeatedFieldStructuredPropertyModel>(
                    "RepeatedFieldStructuredPropertyModel"
                ) {
                    "structured.innerProp2" le (
                        innerDateTime1 + Duration.ofSeconds(10))
                }.toList()

            result.size shouldBe 1
            (result[0].key.idOrName as KeyName).value shouldBe "the-first-one"

            anotherResult.size shouldBe 2
        }
    }

    "It disallows optional properties on repeated structured properties" {
        val testKey = Key<InvalidStructuredPropertyModel>(
            "InvalidStructuredPropertyModel", "the-first-one")
        val instance = InvalidStructuredPropertyModel(
            structured = listOf(
                StructuredProperty(InvalidStructuredProperty())),
            key = testKey
        )

        shouldThrow<IllegalArgumentException> {
            instance
                .toDatastoreEntity()
                .toTypedModel(InvalidStructuredPropertyModel::class)
        }
    }

    "It throws if repeated properties don't have a fixed number of elements" {
        val testKey = Key<StructuredPropertyModel>(
            "StructuredPropertyModel", "the-first-one")

        val entity = Entity.newBuilder(testKey.toDatastoreKey())
            .set("basic", "a_string")
            .set("structured.innerProp1", "first_inner")
            .set(
                "structured.innerProp2",
                Timestamp.ofTimeSecondsAndNanos(1009L, 0))
            .set(
                "repeatedStructured.innerProp1",
                listOf(
                    StringValue("first_inner_1"),
                    StringValue("first_inner_2")))
            .set(
                "repeatedStructured.innerProp2",
                listOf(
                    TimestampValue(Timestamp.ofTimeSecondsAndNanos(1009L, 0))))
            .build()

        shouldThrow<IllegalStateException> {
            entity.toTypedModel(StructuredPropertyModel::class)
        }
    }

    "It throws if repeated properties have repeated fields" {
        val testKey = Key<InvalidDoublyRepeatedModel>(
            "InvalidDoublyRepeatedModel", "the-first-one")
        val instance = InvalidDoublyRepeatedModel(
            structured = listOf(),
            key = testKey
        )

        shouldThrow<IllegalArgumentException> {
            instance
                .toDatastoreEntity()
                .toTypedModel(InvalidDoublyRepeatedModel::class)
        }
    }

    "It throws on nested structured properties" {
        val testKey = Key<UnsupportedNestedModel>(
            "UnsupportedNestedModel", "the-first-one")
        val entity = Entity.newBuilder(testKey.toDatastoreKey()).build()
        shouldThrow<NotImplementedError> {
            entity.toTypedModel(UnsupportedNestedModel::class)
        }
    }
})
