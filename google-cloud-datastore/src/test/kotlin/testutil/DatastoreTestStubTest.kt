package org.khanacademy.datastore.testutil

import com.google.cloud.datastore.DatastoreTypeConverter
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
import org.khanacademy.datastore.toDatastoreKey
import org.khanacademy.datastore.toKey
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyID
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.KeyPathElement
import org.khanacademy.metadata.Keyed
import java.time.LocalDateTime
import java.time.ZoneOffset

data class TestKind(
    override val key: Key<TestKind>
) : Keyed<TestKind>

data class TestQueryKind(
    override val key: Key<TestQueryKind>,
    val aString: String,
    val aNullableLong: Long?,
    val aTimestamp: LocalDateTime,
    val aKey: Key<TestKind>
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
            aNullableLong = 42,
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", 1)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 2),
            aString = "b",
            aNullableLong = null,
            aTimestamp = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", 2)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 3),
            aString = "c",
            aNullableLong = 0,
            aTimestamp = LocalDateTime.ofEpochSecond(1, 1, ZoneOffset.UTC),
            aKey = Key(
                "TestKind", 1,
                parentPath = listOf(KeyPathElement("AParentKind", KeyID(1))))

        ),
        TestQueryKind(
            key = Key("TestQueryKind", 4),
            aString = "d",
            aNullableLong = -42,
            aTimestamp = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", "some-string-key")
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

    "It should correctly evaluate key equality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aKey" eq Key<TestKind>("TestKind", 1)
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 1)
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

    "It should correctly evaluate key inequality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aKey" ge Key<TestKind>("TestKind", 2)
            }.toList()
            result.size shouldBe 2
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 2)
            result[1].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It should correctly evaluate key inequality queries with parents" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aKey" lt Key<TestKind>("TestKind", 1)
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 3)
        }
    }

    "It should correctly compare keys with parents and namespaces" {
        val basicKey = Key<TestKind>("TestKind", 1)
        val earlyNamespaceKey = Key<TestKind>("TestKind", 1, namespace = "aaa")
        val lateNamespaceKey = Key<TestKind>("TestKind", 1, namespace = "zzz")
        val lateNamespaceKeyWithParent = Key<TestKind>(
            "TestKind", 1, namespace = "zzz", parentPath = listOf(
                KeyPathElement("AParentKind", KeyID(1))))
        val sorted =
            listOf(
                earlyNamespaceKey, basicKey, lateNamespaceKey,
                lateNamespaceKeyWithParent
            )
                .map { it.toDatastoreKey() }
                .map(DatastoreTypeConverter::keyToPb)
                .sortedWith(Comparator<KeyPb> { a, b -> a.compareTo(b) })
                .map(DatastoreTypeConverter::keyFromPb)
                .map { it.toKey<TestKind>() }
        sorted shouldBe listOf(
            basicKey, earlyNamespaceKey, lateNamespaceKeyWithParent,
            lateNamespaceKey)
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

    "It should correctly compare numbers to null" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aNullableLong" lt 0L
            }.toList()
            result.size shouldBe 2
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 2)
            result[1].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It should correctly find null" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "aNullableLong" eq null
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 2)
        }
    }
})
