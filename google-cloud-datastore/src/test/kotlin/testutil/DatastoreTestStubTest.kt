package org.khanacademy.datastore.testutil

import com.google.cloud.datastore.Entity
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import org.khanacademy.datastore.DB
import org.khanacademy.datastore.DBEnvAndProject
import org.khanacademy.datastore.Datastore
import org.khanacademy.datastore.get
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
            Entity.newBuilder(com.google.cloud.datastore.Key.newBuilder(
                DBEnvAndProject.getEnvAndProject().project,
                "TestKind", "an-entity").build()).build()
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
})
