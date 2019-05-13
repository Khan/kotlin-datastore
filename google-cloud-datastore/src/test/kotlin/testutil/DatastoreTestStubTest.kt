package org.khanacademy.datastore.testutil

import com.google.cloud.datastore.DatastoreTypeConverter
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.datastore.DB
import org.khanacademy.datastore.Datastore
import org.khanacademy.datastore.Quadruple
import org.khanacademy.datastore.SortOrder
import org.khanacademy.datastore.get
import org.khanacademy.datastore.getMulti
import org.khanacademy.datastore.keysOnlyQuery
import org.khanacademy.datastore.orderBy
import org.khanacademy.datastore.put
import org.khanacademy.datastore.putMulti
import org.khanacademy.datastore.query
import org.khanacademy.datastore.toDatastoreKey
import org.khanacademy.datastore.toKey
import org.khanacademy.datastore.transactional
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
    val aKey: Key<TestKind>,
    val manyStrings: List<String?>
) : Keyed<TestQueryKind>

data class TestAncestorQueryKind(
    override val key: Key<TestAncestorQueryKind>,
    val aString: String
) : Keyed<TestAncestorQueryKind>

/**
 * Wrapper update to verify transaction context is propagated.
 */
fun wrappedUpdate(key: Key<TestQueryKind>) {
    val entity = DB.get(key)
    DB.put(entity!!.copy(aNullableLong = 99L))
}

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

    "It should get(multi) the same entities that we put(multi)" {
        val firstKey = Key<TestKind>("TestKind", "first-key")
        val secondKey = Key<TestQueryKind>("TestQueryKind", "second-key")
        val thirdKey = Key<TestKind>("TestKind", "third-key")
        val fourthKey = Key<TestKind>("TestKind", "fourth-key")

        val firstKind = TestKind(firstKey)
        val secondKind = TestQueryKind(
            secondKey,
            aString = "second-key",
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            aKey = firstKey,
            aNullableLong = null,
            manyStrings = listOf(null))
        val thirdKind = TestKind(thirdKey)
        val fourthKind = TestKind(fourthKey)

        // test putMulti/getMulti for two entities
        withMockDatastore(
            TestKind(Key("TestKind", "an-entity"))
        ) {
            DB.getMulti(firstKey, secondKey) shouldBe Pair(null, null)

            DB.putMulti(firstKind, secondKind)

            DB.getMulti(firstKey, secondKey) shouldBe
                    Pair(firstKind, secondKind)
        }

        // test putMulti/getMulti for three entities
        withMockDatastore(
            TestKind(Key("TestKind", "an-entity"))
        ) {
            DB.getMulti(firstKey, secondKey, thirdKey) shouldBe Triple(null,
                null, null)

            DB.putMulti(firstKind, secondKind, thirdKind)

            DB.getMulti(firstKey, secondKey, thirdKey) shouldBe
                    Triple(firstKind, secondKind, thirdKind)
        }

        // test putMulti/getMulti for four entities
        withMockDatastore(
            TestKind(Key("TestKind", "an-entity"))
        ) {
            DB.getMulti(firstKey, secondKey, thirdKey, fourthKey) shouldBe
                    Quadruple(null, null, null, null)

            DB.putMulti(firstKind, secondKind, thirdKind, fourthKind)

            DB.getMulti(firstKey, secondKey, thirdKey, fourthKey) shouldBe
                    Quadruple(firstKind, secondKind, thirdKind, fourthKind)
        }
    }

    val queryFixtures = listOf(
        TestQueryKind(
            key = Key("TestQueryKind", 1),
            aString = "a",
            aNullableLong = 42,
            aTimestamp = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", 1),
            manyStrings = listOf("4", "2")
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 2),
            aString = "b",
            aNullableLong = null,
            aTimestamp = LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", 2),
            manyStrings = listOf()
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 3),
            aString = "c",
            aNullableLong = 0,
            aTimestamp = LocalDateTime.ofEpochSecond(1, 1, ZoneOffset.UTC),
            aKey = Key(
                "TestKind", 1,
                parentPath = listOf(KeyPathElement("AParentKind", KeyID(1)))),
            manyStrings = listOf(null, null, null)
        ),
        TestQueryKind(
            key = Key("TestQueryKind", 4),
            aString = "d",
            aNullableLong = -42,
            aTimestamp = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC),
            aKey = Key("TestKind", "some-string-key"),
            manyStrings = listOf("2", "4", "", null, "42")
        ),

        TestAncestorQueryKind(
            key = Key(
                "TestAncestorQueryKind", 5,
                parentPath = listOf(KeyPathElement("TestKind", KeyID(1)))
            ),
            aString = "a"
        ),
        TestAncestorQueryKind(
            key = Key(
                "TestAncestorQueryKind", 6,
                parentPath = listOf(
                    KeyPathElement("TestKind", KeyID(2)),
                    KeyPathElement("TestQueryKind", KeyID(2))
                )
            ),
            aString = "a"
        ),
        TestAncestorQueryKind(
            key = Key(
                "TestAncestorQueryKind", 7,
                parentPath = listOf(
                    KeyPathElement("TestKind", KeyID(2)),
                    KeyPathElement("TestQueryKind", KeyID(2))
                )
            ),
            aString = "b"
        ),

        TestKind(
            key = Key("TestKind", 1)
        )
    )

    "It should return the expected models in the order they were requested" {
        withMockDatastore(queryFixtures) {
            // when there is a key that does not have an entity matching, put
            // null in its place in the return value.
            val pair = DB.getMulti(Key<TestKind>("TestKind", 9),
                Key<TestQueryKind>("TestQueryKind", 4))
            pair.first?.key shouldBe null
            pair.second?.key shouldBe Key<TestQueryKind>("TestQueryKind", 4)

            // test the return values across the supported capacities (2, 3, 4)
            val trip = DB.getMulti(Key<TestKind>("TestKind", 1),
                Key<TestQueryKind>("TestQueryKind", 2),
                Key<TestQueryKind>("TestQueryKind", 3))
            trip.first?.key shouldBe Key<TestKind>("TestKind", 1)
            trip.second?.key shouldBe Key<TestQueryKind>("TestQueryKind", 2)
            trip.third?.key shouldBe Key<TestQueryKind>("TestQueryKind", 3)

            val quad = DB.getMulti(Key<TestKind>("TestKind", 1),
                Key<TestQueryKind>("TestQueryKind", 2),
                Key<TestQueryKind>("TestQueryKind", 3),
                Key<TestQueryKind>("TestQueryKind", 1))
            quad.first?.key shouldBe Key<TestKind>("TestKind", 1)
            quad.second?.key shouldBe Key<TestQueryKind>("TestQueryKind", 2)
            quad.third?.key shouldBe Key<TestQueryKind>("TestQueryKind", 3)
            quad.fourth?.key shouldBe Key<TestQueryKind>("TestQueryKind", 1)
        }
    }

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

    "It should correctly evaluate simple list equality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "manyStrings" eq "4"
            }.toList()
            result.size shouldBe 2
            result.map { (it.key.idOrName as KeyID).value } shouldBe
                listOf(1L, 4L)
        }
    }

    "It should correctly evaluate multiple list equality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "manyStrings" eq "4"
                "manyStrings" eq "42"
            }.toList()
            result.size shouldBe 1
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It should correctly evaluate list inequality queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind") {
                "manyStrings" gt "41"
                "manyStrings" lt "3"
            }.toList()
            // This does not match: while we have an entity that has
            // both a manyString < "3" and a manyString > "41", we
            // don't have a list-value that satisfies both conditions.
            result.size shouldBe 0
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

    "It should correctly evaluate ancestor queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestAncestorQueryKind>(
                "TestAncestorQueryKind"
            ) {
                hasAncestor(Key<TestKind>("TestKind", 1))
            }.toList()
            result.size shouldBe 1
            result[0].key.kind shouldBe "TestAncestorQueryKind"
            result[0].key.idOrName shouldBe KeyID(5L)
        }
    }

    "It should correctly evaluate ancestor queries for a prefix of the path" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestAncestorQueryKind>(
                "TestAncestorQueryKind"
            ) {
                hasAncestor(Key<TestKind>("TestKind", 2))
            }.toList()
            result.size shouldBe 2
            result[0].key.kind shouldBe "TestAncestorQueryKind"
            result[0].key.idOrName shouldBe KeyID(6L)
            result[1].key.kind shouldBe "TestAncestorQueryKind"
            result[1].key.idOrName shouldBe KeyID(7L)
        }
    }

    "It should correctly evaluate ancestor queries for the parent key" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestAncestorQueryKind>(
                "TestAncestorQueryKind"
            ) {
                hasAncestor(Key<TestKind>(
                    "TestQueryKind", 2,
                    parentPath = listOf(
                        KeyPathElement("TestKind", KeyID(2))
                    )
                ))
            }.toList()
            result.size shouldBe 2
            result[0].key.kind shouldBe "TestAncestorQueryKind"
            result[0].key.idOrName shouldBe KeyID(6L)
            result[1].key.kind shouldBe "TestAncestorQueryKind"
            result[1].key.idOrName shouldBe KeyID(7L)
        }
    }

    // We don't really do it -- why would you -- but it is valid to query based
    // on the entire key as an "ancestor"!
    "It should correctly evaluate ancestor queries for the entire key" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestAncestorQueryKind>(
                "TestAncestorQueryKind"
            ) {
                hasAncestor(Key<TestKind>(
                    "TestAncestorQueryKind", 6,
                    parentPath = listOf(
                        KeyPathElement("TestKind", KeyID(2)),
                        KeyPathElement("TestQueryKind", KeyID(2))
                    )
                ))
            }.toList()
            result.size shouldBe 1
            result[0].key.kind shouldBe "TestAncestorQueryKind"
            result[0].key.idOrName shouldBe KeyID(6L)
        }
    }

    "It should correctly evaluate mixed ancestor/property queries" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestAncestorQueryKind>(
                "TestAncestorQueryKind"
            ) {
                hasAncestor(Key<TestKind>("TestKind", 2))
                "aString" eq "a"
            }.toList()
            result.size shouldBe 1
            result[0].key.kind shouldBe "TestAncestorQueryKind"
            result[0].key.idOrName shouldBe KeyID(6L)
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

    "It should update entities in a transaction" {
        withMockDatastore(queryFixtures) {
            val key = Key<TestQueryKind>("TestQueryKind", 1)
            DB.transactional {
                val entity = DB.get(key)
                entity shouldNotBe null
                // unfortunately the compiler cannot infer that the above is a
                // non-null assertion.
                entity!!.aNullableLong shouldBe 42L
                DB.put(entity.copy(aNullableLong = 43L))
            }
            DB.get(key)?.aNullableLong shouldBe 43L
        }
    }

    "It should update entities in a transaction across function boundaries" {
        val key = Key<TestQueryKind>("TestQueryKind", 1)

        withMockDatastore(queryFixtures) {
            DB.get(key)?.aNullableLong shouldBe 42L
            DB.transactional {
                wrappedUpdate(key)
            }
            DB.get(key)?.aNullableLong shouldBe 99L
        }

        // Ensure that if wrappedUpdate() got a non-mock datastore, we'd have
        // thrown.
        shouldThrow<NotImplementedError> {
            DB.get(key)
        }
    }

    "Puts in a transaction are not visible until it has committed" {
        withMockDatastore(queryFixtures) {
            val key = Key<TestQueryKind>("TestQueryKind", 1)
            DB.transactional {
                val entity = DB.get(key)
                entity shouldNotBe null
                // unfortunately the compiler cannot infer that the above is a
                // non-null assertion.
                entity!!.aNullableLong shouldBe 42L
                DB.put(entity.copy(aNullableLong = 43L))

                DB.get(key)?.aNullableLong shouldBe 42L
            }
            DB.get(key)?.aNullableLong shouldBe 43L
        }
    }

    "It should correctly order queries (ascending)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("aNullableLong")
            ) {
                "aNullableLong" gt -10000L
            }.toList()
            result[0].aNullableLong shouldBe -42L
        }
    }

    "It should correctly order queries (descending)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("aNullableLong", SortOrder.DESC)
            ) {
                "aNullableLong" gt -10000L
            }.toList()
            result[0].aNullableLong shouldBe 42L
        }
    }

    "It correctly orders queries on repeated properties (asc)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("manyStrings", SortOrder.ASC)
            ) {
                "aString" ge "a"
            }.toList()
            // Entity 3 and 4 both have null as a value, and 3 was inserted
            // first, which is the implementation-dependent ordering the stub
            // uses.
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 3)
        }
    }

    "It correctly orders queries on repeated properties (desc)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("manyStrings", SortOrder.DESC)
            ) {
                "aString" ge "a"
            }.toList()
            // Entity 4 has the maximum value ("42") and there are no ties.
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It correctly orders queries on filtered repeated properties (asc)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("manyStrings", SortOrder.ASC)
            ) {
                "manyStrings" ge "2"
            }.toList()
            // Entity 1 has the tied minimum value ("2") that satisfies the
            // query, and it was inserted first, which is the
            // implementation-dependent ordering the stub uses.
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 1)
        }
    }

    "It correctly orders queries on filtered repeated properties (desc)" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>(
                "TestQueryKind",
                orderBy("manyStrings", SortOrder.DESC)
            ) {
                "manyStrings" ge "2"
            }.toList()
            // Entity 4 has the maximum value ("42") that satisfies the
            // query, and there are no ties.
            result[0].key shouldBe Key<TestQueryKind>("TestQueryKind", 4)
        }
    }

    "It allows queries with no filters" {
        withMockDatastore(queryFixtures) {
            val result = DB.query<TestQueryKind>("TestQueryKind").toList()
            result.size shouldBe 4
            result.map { it.key } shouldBe listOf(
                Key<TestQueryKind>("TestQueryKind", 1),
                Key<TestQueryKind>("TestQueryKind", 2),
                Key<TestQueryKind>("TestQueryKind", 3),
                Key<TestQueryKind>("TestQueryKind", 4)
            )
        }
    }
})
