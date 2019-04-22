/**
 * Interface for interacting with the datastore, and implementation.
 *
 * This implements all the functionality available via the DB singleton.
 */
package org.khanacademy.datastore

import com.google.cloud.datastore.DatastoreReaderWriter
import com.google.cloud.datastore.Entity
import com.google.cloud.datastore.Query
import com.google.cloud.datastore.StructuredQuery
import com.google.cloud.datastore.Transaction
import kotlin.reflect.full.findAnnotation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.Keyed
import org.khanacademy.metadata.Readonly
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

data class Quadruple<A, B, C, D>(val first: A, val second: B, val third: C,
                                 val fourth: D)

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
 * Synchronous get multi by object keys from datastore.
 */
inline fun <reified A : Keyed<A>, reified B : Keyed<B>> Datastore.getMulti(
    a: Key<A>, b: Key<B>): Pair<A?, B?> =
        internalGetMulti(this, a, b, A::class, B::class)

inline fun <reified A : Keyed<A>, reified B : Keyed<B>, reified C : Keyed<C>>
Datastore.getMulti(a: Key<A>, b: Key<B>, c: Key<C>): Triple<A?, B?, C?> =
    internalGetMulti(this, a, b, c, A::class, B::class, C::class)

inline fun <
    reified A : Keyed<A>,
    reified B : Keyed<B>,
    reified C : Keyed<C>,
    reified D : Keyed<D>
> Datastore.getMulti(
    a: Key<A>, b: Key<B>, c: Key<C>, d: Key<D>
): Quadruple<A?, B?, C?, D?> =
    internalGetMulti(this, a, b, c, d, A::class, B::class, C::class,
        D::class)
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

/**
 * Asynchronous get multi by object keys from datastore.
 */
inline fun <reified A : Keyed<A>, reified B : Keyed<B>> Datastore.getMultiAsync(
    a: Key<A>, b: Key<B>
): Deferred<Pair<A?, B?>?> =
    internalGetMultiAsync(this, a, b, A::class, B::class)

inline fun <
    reified A : Keyed<A>,
    reified B : Keyed<B>,
    reified C : Keyed<C>
> Datastore.getMultiAsync(
    a: Key<A>, b: Key<B>, c: Key<C>
): Deferred<Triple<A?, B?, C?>?> =
    internalGetMultiAsync(this, a, b, c, A::class, B::class, C::class)

inline fun <
    reified A : Keyed<A>,
    reified B : Keyed<B>,
    reified C : Keyed<C>,
    reified D : Keyed<D>
> Datastore.getMultiAsync(
    a: Key<A>, b: Key<B>, c: Key<C>, d: Key<D>
): Deferred<Quadruple<A?, B?, C?, D?>?> =
    internalGetMultiAsync(this, a, b, c, d, A::class, B::class, C::class,
        D::class)

/**
 * Exception thrown when we try to put a read-only model.
 */
class ReadonlyModelException(msg: String) : Exception(msg)

/**
 * Throw if the model instance's class is annotated @Readonly.
 */
internal fun <T : Keyed<T>> throwIfReadonly(instance: Keyed<T>) {
    if (instance::class.findAnnotation<Readonly>() != null) {
        throw ReadonlyModelException(
            "Cannot put() an instance of ${instance::class.simpleName}")
    }
}

/**
 * Synchronous put of an object to the datastore.
 */
fun <T : Keyed<T>> Datastore.put(modelInstance: Keyed<T>): Key<T> {
    throwIfReadonly(modelInstance)
    return this.clientOrTransaction.put(modelInstance.toDatastoreEntity())
        .key.toKey()
}

/**
 * Synchronous put of multiple objects to the datastore.
 */
fun <A : Keyed<A>, B : Keyed<B>> Datastore.putMulti(
    a: A, b: B
): Pair<Key<A>, Key<B>> {
    throwIfReadonly(a)
    throwIfReadonly(b)
    val entities = this.clientOrTransaction.put(a.toDatastoreEntity(),
        b.toDatastoreEntity())
    return Pair(entities[0].key.toKey(), entities[1].key.toKey())
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>> Datastore.putMulti(
    a: A, b: B, c: C
): Triple<Key<A>, Key<B>, Key<C>> {
    throwIfReadonly(a)
    throwIfReadonly(b)
    throwIfReadonly(c)
    val entities = this.clientOrTransaction.put(a.toDatastoreEntity(),
        b.toDatastoreEntity(), c.toDatastoreEntity())
    return Triple(entities[0].key.toKey(), entities[1].key.toKey(),
        entities[2].key.toKey())
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>, D : Keyed<D>> Datastore.putMulti(
    a: A, b: B, c: C, d: D
): Quadruple<Key<A>, Key<B>, Key<C>, Key<D>> {
    throwIfReadonly(a)
    throwIfReadonly(b)
    throwIfReadonly(c)
    throwIfReadonly(d)
    val entities = this.clientOrTransaction.put(a.toDatastoreEntity(),
        b.toDatastoreEntity(), c.toDatastoreEntity(), d.toDatastoreEntity())
    return Quadruple(entities[0].key.toKey(), entities[1].key.toKey(),
        entities[2].key.toKey(), entities[3].key.toKey())
}

/**
 * Asynchronous put of an object to the datastore.
 */
fun <T : Keyed<T>> Datastore.putAsync(
    modelInstance: Keyed<T>
): Deferred<Key<T>> =
    DB.async { put(modelInstance) }

fun Datastore.inTransaction(): Boolean = clientOrTransaction is Transaction

/**
 * Asynchronous put of multiple objects to the datastore.
 */
fun <A : Keyed<A>, B : Keyed<B>> Datastore.putMultiAsync(
    a: A, b: B
): Deferred<Pair<Key<A>, Key<B>>> =
    DB.async {
        putMulti(a, b)
    }

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>> Datastore.putMultiAsync(
    a: A, b: B, c: C
): Deferred<Triple<Key<A>, Key<B>, Key<C>>> =
    DB.async {
        putMulti(a, b, c)
    }

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>, D : Keyed<D>>
Datastore.putMultiAsync(
    a: A, b: B, c: C, d: D
): Deferred<Quadruple<Key<A>, Key<B>, Key<C>, Key<D>>> =
    DB.async {
        putMulti(a, b, c, d)
    }

/**
 * Query for objects matching all the given filters.
 *
 * For technical reasons, this is implemented out of line, below.
 */
inline fun <reified T : Keyed<T>> Datastore.query(
    kind: String, order: OrderBy = NoOrdering, vararg filters: QueryFilter
): Sequence<T> = internalQuery(this, kind, order, filters.toList(), T::class)

/**
 * Query DSL for all objects matching the given filters.
 *
 * Within the provided block, specify expressions consisting of the (string)
 * name of the property on which you're querying, an infix operator (one of eq,
 * lt, gt, le, ge), and the value you wish to compare against.
 *
 * The query will consist of an `and` of all such conditions.
 *
 * Usage example:
 *
 * DB.query<SomeModelClass>("SomeModelClass", orderBy("aProperty")) {
 *     "aProperty" eq 3
 *     "somethingElse" lt "abcd"
 * }
 *
 * Usage example with multiple orderings:
 *
 * DB.query<SomeModelClass>(
 *     "someModelClass", orderBy(Pair("aProperty", SortOrder.ASC),
 *                               Pair("somethingElse", SortOrder.DESC))
 * ) {
 *     "aProperty" eq 3
 *     "somethingElse" lt "abcd"
 * }
 */
inline fun <reified T : Keyed<T>> Datastore.query(
    kind: String,
    order: OrderBy = NoOrdering,
    builderBlock: QueryFilterBuilder.() -> Any?
): Sequence<T> {
    val builder = QueryFilterBuilder()
    builder.builderBlock()
    return this.query(kind, order, *(builder.build().toTypedArray()))
}

/**
 * Query for keys matching all the given filters.
 *
 * For technical reasons, this is implemented out of line, below.
 */
inline fun <reified T : Keyed<T>> Datastore.keysOnlyQuery(
    kind: String,
    order: OrderBy = NoOrdering,
    vararg filters: QueryFilter
): Sequence<Key<T>> = internalKeysOnlyQuery<T>(
    this, kind, order, filters.toList(), T::class)

/**
 * Query DSL for all keys corresponding to the given filters.
 *
 * @see query for usage information
 */
inline fun <reified T : Keyed<T>> Datastore.keysOnlyQuery(
    kind: String,
    order: OrderBy = NoOrdering,
    builderBlock: QueryFilterBuilder.() -> Any?
): Sequence<Key<T>> {
    val builder = QueryFilterBuilder()
    builder.builderBlock()
    return this.keysOnlyQuery(kind, order, *(builder.build().toTypedArray()))
}

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
 * Internal Datastore.getMulti used as an inlining target.
 */
fun <A : Keyed<A>, B : Keyed<B>> internalGetMulti(
    datastore: Datastore, a: Key<A>, b: Key<B>, aClass: KClass<A>,
    bClass: KClass<B>
): Pair<A?, B?> {
    val entities = datastore.clientOrTransaction.get(
        a.toDatastoreKey(), b.toDatastoreKey())
    val formattedEntities = replaceMissingWithNull(listOf(a, b), entities)

    return Pair(
        formattedEntities[0]?.toTypedModel(aClass),
        formattedEntities[1]?.toTypedModel(bClass))
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>> internalGetMulti(
    datastore: Datastore, a: Key<A>, b: Key<B>, c: Key<C>, aClass: KClass<A>,
    bClass: KClass<B>, cClass: KClass<C>
): Triple<A?, B?, C?> {
    val entities = datastore.clientOrTransaction.get(
        a.toDatastoreKey(), b.toDatastoreKey(), c.toDatastoreKey())
    val formattedEntities = replaceMissingWithNull(listOf(a, b, c), entities)

    return Triple(
        formattedEntities[0]?.toTypedModel(aClass),
        formattedEntities[1]?.toTypedModel(bClass),
        formattedEntities[2]?.toTypedModel(cClass))
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>, D : Keyed<D>> internalGetMulti(
    datastore: Datastore, a: Key<A>, b: Key<B>, c: Key<C>, d: Key<D>,
    aClass: KClass<A>, bClass: KClass<B>, cClass: KClass<C>, dClass: KClass<D>
): Quadruple<A?, B?, C?, D?> {
    val entities = datastore.clientOrTransaction.get(
        a.toDatastoreKey(), b.toDatastoreKey(), c.toDatastoreKey(),
        d.toDatastoreKey())
    val formattedEntities = replaceMissingWithNull(listOf(a, b, c, d), entities)

    return Quadruple(
        formattedEntities[0]?.toTypedModel(aClass),
        formattedEntities[1]?.toTypedModel(bClass),
        formattedEntities[2]?.toTypedModel(cClass),
        formattedEntities[3]?.toTypedModel(dClass))
}

private fun replaceMissingWithNull(
    keys: List<Key<*>>, entities: Iterator<Entity>?
): List<Entity?> {
    val keyEntityMap = HashMap<DatastoreKey, Entity>()

    if (entities == null) { return keys.map { null } }

    for (entity in entities) {
        keyEntityMap.put(entity.key, entity)
    }

    return keys.map { keyEntityMap[it.toDatastoreKey()] }
}

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
 * Internal Datastore.getMultiAsync used as an inlining target.
 */
fun <A : Keyed<A>, B : Keyed<B>> internalGetMultiAsync(
    datastore: Datastore, a: Key<A>, b: Key<B>, aClass: KClass<A>,
    bClass: KClass<B>
): Deferred<Pair<A?, B?>?> = datastore.async {
    internalGetMulti(datastore, a, b, aClass, bClass)
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>> internalGetMultiAsync(
    datastore: Datastore, a: Key<A>, b: Key<B>, c: Key<C>, aClass: KClass<A>,
    bClass: KClass<B>, cClass: KClass<C>
): Deferred<Triple<A?, B?, C?>?> = datastore.async {
    internalGetMulti(datastore, a, b, c, aClass, bClass, cClass)
}

fun <A : Keyed<A>, B : Keyed<B>, C : Keyed<C>, D : Keyed<D>>
internalGetMultiAsync(
    datastore: Datastore, a: Key<A>, b: Key<B>, c: Key<C>, d: Key<D>,
    aClass: KClass<A>, bClass: KClass<B>, cClass: KClass<C>, dClass: KClass<D>
): Deferred<Quadruple<A?, B?, C?, D?>?> = datastore.async {
    internalGetMulti(datastore, a, b, c, d, aClass, bClass, cClass, dClass)
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

/**
 * Convert our filters to a single possibly composite datastore client filter.
 *
 * Throw if the list of filters is empty.
 */
internal fun convertDatastoreFilters(
    filters: List<QueryFilter>,
    cls: KClass<*>
): StructuredQuery.Filter {
    val datastoreFilters = filters.map { it.toDatastoreFilter(cls) }
    return when (datastoreFilters.size) {
        0 -> throw IllegalArgumentException(
            "You must provide at least one query filter.")
        1 -> datastoreFilters[0]
        else ->
            // .and() takes one required filter and varargs with any number of
            // additional filters. Therefore we need to pass the first filter
            // separately from the rest of them here.
            StructuredQuery.CompositeFilter.and(
                datastoreFilters[0],
                *datastoreFilters.drop(1).toTypedArray()
            )
    }
}

/**
 * Internal Datastore.query used as an inlining target
 *
 * @suppress
 */
fun <T : Keyed<T>> internalQuery(
    datastore: Datastore,
    kind: String,
    order: OrderBy,
    filters: List<QueryFilter>,
    tReference: KClass<T>
): Sequence<T> {
    val filter = convertDatastoreFilters(filters, tReference)
    val query = Query.newEntityQueryBuilder()
        .setKind(kind)
        .setFilter(filter)
        .applyOrdering(order)
        .build()
    val result = datastore.clientOrTransaction.run(query)
    return result.asSequence()
        .map { entity -> entity.toTypedModel(tReference) }
}

/**
 * Internal Datastore.keysOnlyQuery used as an inlining target
 *
 * @suppress
 */
fun <T : Keyed<T>> internalKeysOnlyQuery(
    datastore: Datastore,
    kind: String,
    order: OrderBy,
    filters: List<QueryFilter>,
    tReference: KClass<T>
): Sequence<Key<T>> {
    val filter = convertDatastoreFilters(filters, tReference)
    val query = Query.newKeyQueryBuilder()
        .setKind(kind)
        .setFilter(filter)
        .applyOrdering(order)
        .build()
    val result = datastore.clientOrTransaction.run(query)
    return result.asSequence()
        .map { key -> key.toKey<T>() }
}
