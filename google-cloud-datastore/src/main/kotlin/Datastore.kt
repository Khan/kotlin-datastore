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
import kotlin.reflect.KClass

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

// Public API extension functions

/**
 * Synchronous get by key of an object from the datastore.
 *
 * Will throw if we retrieve an entity from the datastore and we can't convert
 * it to type T, but will return null if nothing is found in the datastore.
 *
 * For technical reasons, this is implemented out of line, below.
 */
inline fun <reified T : Keyed<T>> Datastore.get(key: Key<T>): T? =
    internalGet(this, key, T::class)

/**
 * Asynchronous get by key of an object from the datastore.
 *
 * This uses the standard kotlin `async` function, with a coroutine context
 * that tracks ongoing transactions correctly.
 *
 * For technical reasons, this is implemented out of line, below.
 */
inline fun <reified T : Keyed<T>> Datastore.getAsync(
    key: Key<T>
): Deferred<T?> = internalGetAsync(this, key, T::class)

fun Datastore.inTransaction(): Boolean = clientOrTransaction is Transaction

/**
 * Run a function, with all contained datastore operations transactional.
 *
 * WARNING: currently if there are any async operations in the transaction,
 * and you don't `.await()` them in the transaction, whether they
 * participate in the transaction or not is undefined behavior.
 * TODO(colin): we can probably do something clever with coroutine scopes
 * to force ourselves to wait on them automatically.
 *
 * Note that clients must only use these extension methods for transactions,
 * and they must not construct a transaction in any other way.
 */
fun <T> Datastore.transactional(
    options: TransactionOptions,
    block: Datastore.() -> T
): T = when {
    inTransaction() && options.propagationAllowed ->
        block()
    inTransaction() && !options.propagationAllowed ->
        throw IllegalStateException(
            "Nesting transactions is not allowed when propagationAllowed" +
                "is false")
    else -> {
        val txn = (
            clientOrTransaction as com.google.cloud.datastore.Datastore
            ).newTransaction()
        // We have to set back to the nontransactional context right now,
        // and then run the actual block in the transactional context
        // explicitly.
        // If we don't do this, then if the block we're executing
        // transactionally does any async operations that may suspend
        // execution, its transaction may leak into the next thing that
        // runs on that thread. Doing it this way lets the runtime take
        // care of activating and restoring the context automatically
        // whenever the async code in the transaction is executing.
        // We use the restoreLocalDBAfter helper to ensure that if the
        // context constructor threw an exception between setting
        // `localDB` and adding itself to the coroutine context, that
        // we clean up properly.
        val context = restoreLocalDBAfter {
            // NOTE: This constructor will set the value of localDB.
            Datastore(txn)
        }
        val deferredResult = context.async {
            context.block()
        }
        val result = runBlocking {
            deferredResult.await()
        }
        txn.commit()
        result
    }
}

/**
 * Run datastore operations in a transaction with default options.
 *
 * @see transactional
 */
fun <T> Datastore.transactional(
    block: Datastore.() -> T
): T = transactional(TransactionOptions.DEFAULT, block)

// TODO(colin): getMulti, all the various put() functions, and queries.

/**
 * Main implementation for interacting with the datastore.
 *
 * This is the interface for the `DB` object that represents the
 * external-facing API of this library, though most of the API is implemented
 * as extension functions on this type, above.
 *
 * This tracks the context of what transaction (if any) we're currently working
 * in so that we can assign async operations to the correct transaction.
 *
 * TODO(colin): It does not appear that `Transaction` objects are safe for
 * concurrent use by multiple threads for `put` operations, so we'll need to
 * synchronize those operations.
 * TODO(colin): more carefully verify this thread-safety behavior.
 */
class Datastore internal constructor(
    // Outside a transaction, this is a com.google.cloud.datastore.Datastore.
    // Inside one, it's a com.google.cloud.datastore.Transaction.
    internal val clientOrTransaction: DatastoreReaderWriter
) : CoroutineScope {
    override val coroutineContext: CoroutineContext

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

// Internal API helper functions
// These exist so that we can inline the public API functions to a public
// helper, but they're not intended to be used directly.
// Note that these are not defined as extension functions so that IDEs don't
// try to autocomplete them on the DB singleton object.

/**
 * Internal Datastore.get used as an inlining target.
 *
 * @suppress
 */
fun <T : Keyed<T>> internalGet(
    datastore: Datastore, key: Key<T>, tReference: KClass<T>
): T? = datastore.clientOrTransaction
    .get(key.toDatastoreKey())
    ?.toTypedModel(tReference)

/**
 * Internal Datastore.getAsync used as an inlining target.
 *
 * @suppress
 */
fun <T : Keyed<T>> internalGetAsync(
    datastore: Datastore, key: Key<T>, tReference: KClass<T>
): Deferred<T?> = datastore.async {
    internalGet(datastore, key, tReference)
}

/**
 * Run some code, setting localDB back to its current value afterwards.
 *
 * Uses try/finally, so this works even on exception.
 */
internal fun <T> restoreLocalDBAfter(block: () -> T): T {
    val origLocalDB = localDB.get()
    try {
        return block()
    } finally {
        localDB.set(origLocalDB)
    }
}
