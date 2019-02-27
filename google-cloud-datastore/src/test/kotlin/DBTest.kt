package org.khanacademy.datastore

import com.google.cloud.datastore.Transaction
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.never
import com.nhaarman.mockitokotlin2.verify
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.KeyName
import org.khanacademy.metadata.Keyed
import java.util.concurrent.CompletableFuture

data class TestModel(override val key: Key<TestModel>) : Keyed<TestModel>

fun makeMockDatastore(): com.google.cloud.datastore.Datastore = mock {
    on { newTransaction() } doAnswer { mock() }
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

    "In a non-transactional context, it should call the datastore directly" {
        val testDatastore = makeMockDatastore()
        Datastore(testDatastore)
        DB.get(testModel1.key)
        verify(testDatastore).get(testModel1.key.toDatastoreKey())
        verify(testDatastore, never()).get(testModel2.key.toDatastoreKey())
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
})
