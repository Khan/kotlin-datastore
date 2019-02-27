package org.khanacademy.datastore.testutil

import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.datastore.DB
import org.khanacademy.datastore.Datastore
import org.khanacademy.datastore.get
import org.khanacademy.datastore.put
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed

data class TestKind(
    override val key: Key<TestKind>
) : Keyed<TestKind>

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

            // This shoudln't have affected any other entities.
            DB.get(Key<TestKind>(
                parentPath = listOf(),
                kind = "TestKind",
                idOrName = KeyName("an-entity")
            )) shouldNotBe null
        }
    }
})
