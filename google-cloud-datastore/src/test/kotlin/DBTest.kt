package org.khanacademy.datastore

import com.google.cloud.datastore.Transaction
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.khanacademy.datastore.testutil.withMockDatastore
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Meta
import java.util.concurrent.CompletableFuture

data class TestModel(override val key: Key<TestModel>) : Keyed<TestModel>

data class RenamingTestModel(
    @Meta(name = "y")
    val x: String,
    override val key: Key<RenamingTestModel>
) : Keyed<RenamingTestModel>

fun makeMockDatastore(): com.google.cloud.datastore.Datastore = mock {
    on { newTransaction() } doAnswer { mock() }
}

/**
 * Simple wrapper function around DB.get to check context propagation.
 *
 * (We need to ensure that transaction context is visible not just within a
 * DB.transactional{} block but also in nested functions.)
 */
fun wrappedDBGet(key: Key<TestModel>): TestModel? {
    return DB.get(key)
}

class DBTest : StringSpec({
    val testModel1 = TestModel(Key(
        parentPath = listOf(),
        kind = "TestModel",
        idOrName = KeyName("test_model_1")
    ))
    val testModel2 = TestModel(Key(
        parentPath = listOf(),
        kind = "TestModel",
        idOrName = KeyName("test_model_2")
    ))
    val testModel3 = TestModel(Key(
        parentPath = listOf(),
        kind = "TestModel",
        idOrName = KeyName("test_model_3")
    ))
    val secondaryModel = SecondaryTestModel(Key(
        parentPath = listOf(),
        kind = "SecondaryTestModel",
        idOrName = KeyName("secondary_test_model")
    ))
    val readOnlyTestModel = ReadonlyTestModel("1", Key(
        parentPath = listOf(),
        kind = "ReadonlyTestModel",
        idOrName = KeyName("read_only_test_model_1")
    ))

    "In a non-transactional context, it should call the datastore directly" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.get(testModel1.key)
        verify(testDatastore).get(testModel1.key.toDatastoreKey())
        verify(testDatastore, never()).get(testModel2.key.toDatastoreKey())
    }

    "It should call the datastore with each of the key arguments" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.getMulti(testModel1.key, secondaryModel.key)
        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            secondaryModel.key.toDatastoreKey())

        DB.getMulti(testModel1.key, testModel2.key, secondaryModel.key)
        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey(),
            secondaryModel.key.toDatastoreKey())

        DB.getMulti(secondaryModel.key, testModel3.key, testModel1.key,
            testModel2.key)
        verify(testDatastore).get(secondaryModel.key.toDatastoreKey(),
            testModel3.key.toDatastoreKey(), testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey())
    }

    "It should call the datastore with a list of keys" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.getMulti(listOf(testModel1.key, testModel2.key))
        verify(testDatastore).get(
            testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey())
    }

    "It should call the datastore with a list of models to put" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.putMulti(listOf(testModel1, testModel2))
        verify(testDatastore).put(
            testModel1.toDatastoreEntity(),
            testModel2.toDatastoreEntity())
    }

    "It should refuse to put a list of read-only models" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        shouldThrow<ReadonlyModelException> {
            DB.putMulti(listOf(readOnlyTestModel))
        }
    }

    "It should call the datastore with each of the key arguments async" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        val op1 = DB.getMultiAsync(testModel1.key, secondaryModel.key)

        val op2 = DB.getMultiAsync(testModel1.key, testModel2.key,
            secondaryModel.key)

        val op3 = DB.getMultiAsync(secondaryModel.key, testModel3.key,
            testModel1.key, testModel2.key)

        runBlocking {
            op1.await()
            op2.await()
            op3.await()
        }

        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            secondaryModel.key.toDatastoreKey())
        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey(),
            secondaryModel.key.toDatastoreKey())
        verify(testDatastore).get(secondaryModel.key.toDatastoreKey(),
            testModel3.key.toDatastoreKey(), testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey())
    }

    "It should call the datastore with a list of keys asynchronously" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        val op1 = DB.getMultiAsync(listOf(testModel1.key, testModel2.key))

        val op2 = DB.getMultiAsync(listOf(testModel1.key, testModel2.key,
            testModel3.key))

        runBlocking {
            op1.await()
            op2.await()
        }

        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey())
        verify(testDatastore).get(testModel1.key.toDatastoreKey(),
            testModel2.key.toDatastoreKey(),
            testModel3.key.toDatastoreKey())
    }

    "It should call the datastore with a list of models asynchronously" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        val op1 = DB.putMultiAsync(listOf(testModel1, testModel2))

        val op2 = DB.putMultiAsync(listOf(testModel1, testModel2,
            testModel3))

        runBlocking {
            op1.await()
            op2.await()
        }

        verify(testDatastore).put(testModel1.toDatastoreEntity(),
            testModel2.toDatastoreEntity())
        verify(testDatastore).put(testModel1.toDatastoreEntity(),
            testModel2.toDatastoreEntity(),
            testModel3.toDatastoreEntity())
    }

    "It should call the datastore when waiting on async operations" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        val op = DB.getAsync(testModel2.key)
        runBlocking {
            op.await()
        }
        verify(testDatastore).get(testModel2.key.toDatastoreKey())
        verify(testDatastore, never()).get(testModel1.key.toDatastoreKey())
    }

    "It should call the datastore when waiting on multiple async operations" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        val firstOp = DB.getAsync(testModel2.key)
        val secondOp = DB.getAsync(testModel3.key)
        runBlocking {
            firstOp.await()
            secondOp.await()
        }
        verify(testDatastore).get(testModel2.key.toDatastoreKey())
        verify(testDatastore).get(testModel3.key.toDatastoreKey())
        verify(testDatastore, never()).get(testModel1.key.toDatastoreKey())
    }

    "It should call get on the transaction in a transactional context" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        lateinit var testTransaction: Transaction
        DB.transactional {
            DB.get(testModel1.key)
            DB.get(testModel2.key)
            testTransaction = clientOrTransaction as Transaction
        }
        verify(testTransaction).get(testModel1.key.toDatastoreKey())
        verify(testTransaction).get(testModel2.key.toDatastoreKey())
        verify(testTransaction, never()).get(testModel3.key.toDatastoreKey())
    }

    "It should call get on the transaction when waiting on async operations" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        lateinit var testTransaction: Transaction
        DB.transactional {
            val op1 = DB.getAsync(testModel1.key)
            val op2 = DB.getAsync(testModel2.key)
            testTransaction = clientOrTransaction as Transaction
            runBlocking {
                op1.await()
                op2.await()
            }
        }
        verify(testTransaction).get(testModel1.key.toDatastoreKey())
        verify(testTransaction).get(testModel2.key.toDatastoreKey())
        verify(testTransaction, never()).get(testModel3.key.toDatastoreKey())
    }

    "It should call get on two separate interleaved async transactions" {
        // This is a pretty complicated test, so a bit of explanation:
        // Because transactions may be executing concurrently, interleaved on
        // the same thread, we need to make sure that they each keep track of
        // their own operations, rather than accidentally mixing up operations
        // in interleaved ongoing transactions.
        // To test this, we start two transactions asynchronously: one that
        // does a first op, then waits for a signal from the second transaction
        // before doing a second op; and a second that starts and does a single
        // op before signalling back to the first one.
        // If we're correctly preserving transaction context, the operations
        // should have executed in their own transactions. If not, some
        // possible modes of failure are: the operations will have leaked into
        // one global transaction, the second operation from transaction 1 will
        // happen in transaction 2, or some operations will happen
        // nontransactionally.
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        lateinit var testTransaction: Transaction
        lateinit var otherTransaction: Transaction
        val flag = CompletableFuture<Nothing>()
        val flagWaiter = GlobalScope.async(Dispatchers.IO) {
            flag.get()
        }
        runBlocking {
            val t2 = DB.async {
                DB.transactional {
                    testTransaction = clientOrTransaction as Transaction
                    val firstOp = DB.getAsync(testModel1.key)
                    // Wait on a future so that we can simulate another
                    // transaction happening while this one is going on and
                    // make sure that context works correctly.
                    runBlocking { flagWaiter.await() }
                    val secondOp = DB.getAsync(testModel3.key)
                    runBlocking {
                        firstOp.await()
                        secondOp.await()
                    }
                }
            }
            val t1 = DB.async {
                DB.transactional {
                    otherTransaction = clientOrTransaction as Transaction
                    val op = DB.getAsync(testModel2.key)
                    runBlocking {
                        op.await()
                    }
                    flag.complete(null)
                }
            }
            t1.await()
            t2.await()
            testTransaction shouldNotBe otherTransaction
            verify(testTransaction).get(testModel1.key.toDatastoreKey())
            verify(testTransaction).get(testModel3.key.toDatastoreKey())
            verify(testTransaction, never()).get(testModel2.key.toDatastoreKey())
            verify(otherTransaction).get(testModel2.key.toDatastoreKey())
        }
    }

    "It should correctly detect if we're in a transaction" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.inTransaction() shouldBe false
        DB.transactional {
            DB.inTransaction() shouldBe true
        }
        val deferredResult = DB.async {
            DB.inTransaction()
        }
        runBlocking { deferredResult.await() } shouldBe false
        val deferredTxnResult = DB.async {
            DB.transactional {
                DB.inTransaction()
            }
        }
        runBlocking { deferredTxnResult.await() } shouldBe true
    }

    "It should correctly use a transaction even when in a function call" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        lateinit var testTransaction: Transaction
        DB.transactional {
            wrappedDBGet(testModel3.key)
            wrappedDBGet(testModel2.key)
            testTransaction = clientOrTransaction as Transaction
        }
        verify(testTransaction).get(testModel3.key.toDatastoreKey())
        verify(testTransaction).get(testModel2.key.toDatastoreKey())
        verify(testTransaction, never()).get(testModel1.key.toDatastoreKey())
    }

    "It correctly uses property renaming for queries" {
        val testKey = Key<RenamingTestModel>("RenamingTestModel", 1L)
        withMockDatastore(RenamingTestModel(x = "value", key = testKey)) {
            DB.query<RenamingTestModel>("RenamingTestModel") {
                "x" eq "value"
            }.toList().size shouldBe 1

            // You can also query by the datastore name.
            DB.query<RenamingTestModel>("RenamingTestModel") {
                "y" eq "value"
            }.toList().size shouldBe 1
        }
    }

    "DB access throws if you use a thread started in a transaction" {
        withMockDatastore(listOf()) {
            shouldThrow<IllegalStateException> {
                DB.transactional {
                    val thread = Thread {
                        DB.get(Key<TestModel>("TestModel", 1L))
                    }
                    thread.start()
                    thread.join()
                }
            }
        }
    }

    "Child threads should inherit the value of DB" {
        val key = Key<TestModel>("TestModel", 1L)
        val obj = TestModel(key = key)
        withMockDatastore(obj) {
            var result: TestModel? = null
            val thread = Thread {
                result = DB.get(Key<TestModel>("TestModel", 1L))
            }
            thread.start()
            thread.join()
            result shouldBe obj
        }
    }
})
