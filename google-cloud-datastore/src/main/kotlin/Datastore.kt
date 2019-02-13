/**
 * Interface for interacting with the datastore, and implementation.
 *
 * This implements all the functionality available via the DB singleton.
 */
package org.khanacademy.datastore

import com.google.cloud.datastore.DatastoreReaderWriter
import com.google.cloud.datastore.Transaction
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.Keyed
import kotlin.coroutines.CoroutineContext

data class TransactionOptions(
    // If we're in a transaction when we request to start another one, should
    // we just tack on the operations to the existing transaction?
    // (If false, we'll throw in this situation.)
    val propagationAllowed: Boolean
) {
    companion object {
        val DEFAULT = TransactionOptions(propagationAllowed = true)
    }
}

/**
 * Context in which we can run datastore operations.
 *
 * This is the interface for the `DB` object that represents the
 * external-facing API of this library.
 *
 * Context objects serve to allow us to keep track of transactions in the face
 * of potentially asynchronous operations.
 */
interface Datastore : CoroutineScope {
    /**
     * Synchronous get by key of an object from the datastore.
     *
     * Will throw if we retrieve an entity from the datastore and we can't
     * convert it to type T, but will return null if nothing is found in the
     * datastore.
     */
    fun <T : Keyed<T>> get(key: Key<T>): T?

    /**
     * Asynchronous get by key of an object from the datastore.
     *
     * This uses the standard kotlin `async` function, with a coroutine context
     * that tracks ongoing transactions correctly.
     */
    fun <T : Keyed<T>> getAsync(key: Key<T>): Deferred<T?> = async {
        get(key)
    }

    /**
     * Are we currently working in a transaction?
     */
    fun inTransaction(): Boolean

    /**
     * Run a function, with all contained datastore operations transactional.
     *
     * WARNING: currently if there are any async operations in the transaction,
     * and you don't `.await()` them in the transaction, whether they
     * participate in the transaction or not is undefined behavior.
     * TODO(colin): we can probably do something clever with coroutine scopes
     * to force ourselves to wait on them automatically.
     *
     * Note that clients must only use this methods for transactions, and they
     * must not construct a transaction in any other way.
     */
    fun <T> transactional(
        options: TransactionOptions,
        block: Datastore.() -> T
    ): T
    fun <T> transactional(
        block: Datastore.() -> T
    ): T = transactional(TransactionOptions.DEFAULT, block)
    // TODO(colin): getMulti, all the various put() functions, and queries.
}

/**
 * Implementation of `Datastore` that tracks whether we're in a transaction.
 *
 * TODO(colin): It does not appear that `Transaction` objects are safe for
 * concurrent use by multiple threads for `put` operations, so we'll need to
 * synchronize those operations.
 * TODO(colin): more carefully verify this thread-safety behavior.
 */
internal class DatastoreWithContext(
    // Outside a transaction, this is a com.google.cloud.datastore.Datastore.
    // Inside one, it's a com.google.cloud.datastore.Transaction.
    internal val clientOrTransaction: DatastoreReaderWriter
) : Datastore {
    override val coroutineContext: CoroutineContext

    override fun <T : Keyed<T>> get(key: Key<T>): T? =
        clientOrTransaction.get(key.toDatastoreKey())?.toTypedModel<T>()

    override fun inTransaction(): Boolean = clientOrTransaction is Transaction

    override fun <T> transactional(
        options: TransactionOptions,
        block: Datastore.() -> T
    ): T = when {
        inTransaction() && options.propagationAllowed ->
            with(this, block)
        inTransaction() && !options.propagationAllowed ->
            throw IllegalStateException(
                "Nesting transactions is not allowed when propagationAllowed" +
                    "is false")
        else -> {
            val txn = (
                clientOrTransaction as com.google.cloud.datastore.Datastore
            ).newTransaction()
            val context = try {
                // NOTE: The context constructor will set the value of
                // `localDB`.
                DatastoreWithContext(txn)
            } finally {
                // We have to set back to the nontransactional context right now,
                // and then run the actual block in the transactional context
                // explicitly.
                // If we don't do this, then if the block we're executing
                // transactionally does any async operations that may suspend
                // execution, its transaction may leak into the next thing that
                // runs on that thread. Doing it this way lets the runtime take
                // care of activating and restoring the context automatically
                // whenever the async code in the transaction is executing.
                // We have to do this in a `finally` to ensure that if the
                // context constructor threw an exception between setting
                // `localDB` and adding itself to the coroutine context, that
                // we clean up properly.
                localDB.set(this)
            }
            val deferredResult = context.async {
                with(context, block)
            }
            val result = runBlocking {
                deferredResult.await()
            }
            txn.commit()
            result
        }
    }

    init {
        // We have to set this before adding it to the coroutine context, which
        // is why it has to be in the constructor.
        localDB.set(this)
        // Since the purpose of this context is to make RPCs to the datastore
        // service, make sure we're running async operations in a thread pool
        // for IO-bound operations.
        // Additionally, since async operations may cross threads during their
        // lifetime, we include the value of the context thread-local as a
        // coroutine-local, ensuring we track transactions over the lifetime of
        // a coroutine.
        coroutineContext = Dispatchers.IO + localDB.asContextElement()
    }
}
