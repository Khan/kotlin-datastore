package org.khanacademy.datastore.testutil

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.datastore.DB
import org.khanacademy.datastore.Datastore
import org.khanacademy.datastore.get
import org.khanacademy.datastore.keysOnlyQuery
import org.khanacademy.datastore.put
import org.khanacademy.datastore.query
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyID
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import java.time.LocalDateTime
import java.time.ZoneOffset

data class TestKind(
    override val key: Key<TestKind>
) : Keyed<TestKind>

data class TestQueryKind(
    override val key: Key<TestQueryKind>,
    val aString: String,
    val aTimestamp: LocalDateTime
) : Keyed<TestQueryKind>

class DatastoreTestStubTest : StringSpec({
    "It should by default throw on any datastore access" {
        Datastore(ThrowingDatastore())
        shouldThrow<NotImplementedError> {
            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("an-entity")
            ))
        }
    }

    "If we use a lightweight datastore mock, it will read from that" {
        Datastore(ThrowingDatastore())
        withMockDatastore(
            TestKind(Key("TestKind", "an-entity"))
        ) {
            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("an-entity")
            )) shouldNotBe null

            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("another-entity")
            )) shouldBe null
        }
    }

    "If we put an entity, that will be visible to future gets" {
        Datastore(ThrowingDatastore())
        val newKey = Key<TestKind>("TestKind", "another-entity")

        withMockDatastore(
            TestKind(Key("TestKind", "an-entity"))
        ) {
            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("an-entity")
            )) shouldNotBe null

            DB.get(newKey) shouldBe null
            DB.put(TestKind(newKey))
            DB.get(newKey) shouldNotBe null

            // This shouldn't have affected any other entities.
            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("an-entity")
            )) shouldNotBe null
        }
    }

    val queryFixtures = listOf(
        TestQueryKind(
            key = Key("TestQueryKind", 1),
            aString = "a",
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 2),
            aString = "b",
            aTimestamp = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 3),
            aString = "c",
            aTimestamp = LocalDateTime.ofEpochSecond(1, 1, ZoneOffset.UTC)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 4),
            aString = "d",
            aTimestamp = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC)
        ),
        TestKind(
            key = Key("TestKind", 1)
        )
    )

    "It should correctly evaluate simple equality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aString" eq "d"
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It should correctly evaluate timestamp equality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aTimestamp" eq
                    LocalDateTime.ofEpochSecond(1, 1, ZoneOffset.UTC)
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 3)
        }
    }

    "It should correctly evaluate simple inequality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aString" le "c"
            }.toList()
            result.size shouldBe 3
            result.map { (it.key.idOrName as KeyID).value } shouldBe
                listOf(1L, 2L, 3L)
        }
    }

    "It should correctly evaluate timestamp inequality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aTimestamp" gt
                    LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC)
            }.toList()
            result.size shouldBe 2
            result.map { (it.key.idOrName as KeyID).value } shouldBe
                listOf(3L, 4L)
        }
    }

    "Keys only queries should also return the same results, just with keys" {
        withMockDatastore(queryFixtures) {
            val result = DB.keysOnlyQuery<TestQueryKind>("TestQueryKind") {
                "aString" le "c"
            }.toList()
            result.size shouldBe 3
            result.map { (it.idOrName as KeyID).value } shouldBe
                listOf(1L, 2L, 3L)
        }
    }
})
