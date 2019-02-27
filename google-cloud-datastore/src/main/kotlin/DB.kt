/**
 * Environment and management of our DB singleton object.
 *
 * The DB singleton is the way external clients should use the datastore.
 */
package org.khanacademy.datastore

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Scope for running the lazy initialization of env/project.
 *
 * We use the IO dispatcher to avoid the possibility of deadlock waiting on the
 * mutex if someone accidentally calls the initialization from a coroutine not
 * running in a blocking-ok context.
 */
private object InitializationScope : CoroutineScope {
    override val coroutineContext = Dispatchers.IO
}

/**
 * Holder for the datastore environment.
 *
 * If `init` is called before the first datastore access, we use its arguments
 * as the environment; otherwise, we use the value passed in the environment
 * variables DATASTORE_ENV and DATASTORE_PROJECT.
 */
internal object DBEnvAndProject {
    // Used to ensure only one caller can initialize the env/project at a time.
    private val Lock = Mutex()

    // When creating a datastore backend, we'll set the environment from this
    // process environment variable if not set from code.
    const val PROCESS_ENV_VAR = "DATASTORE_ENV"
    // Likewise, we'll set the datastore project from this process environment
    // variable if not set from code.
    const val PROCESS_PROJECT_ENV_VAR = "DATASTORE_PROJECT"

    private lateinit var env: DatastoreEnvWithProject

    // Note: the caller is responsible for locking using the `Lock` mutex
    private fun init(env: DatastoreEnv, project: String) {
        if (this::env.isInitialized) {
            throw IllegalStateException(
                "Datastore environment has already been initialized.")
        }
        this.env = DatastoreEnvWithProject(env, project)
    }

    internal suspend fun initWithLocking(env: DatastoreEnv, project: String) =
        Lock.withLock {
            init(env, project)
        }

    private suspend fun maybeInitFromProcessEnv(): DatastoreEnvWithProject =
        Lock.withLock {
            if (!DBEnvAndProject::env.isInitialized) {
                val datastoreEnvString = System.getenv(PROCESS_ENV_VAR)
                    // TODO(colin): more specific exception class?
                    ?: throw Exception(
                        "You must provide a datastore environment. Either " +
                            "call `DBEnv.init(env, project)` or set the " +
                            "process environment variable $PROCESS_ENV_VAR.")
                val validEnvStrings = DatastoreEnv.values().map { it.name }
                if (datastoreEnvString !in validEnvStrings) {
                    throw IllegalArgumentException(
                        "$datastoreEnvString is not a valid environment. " +
                            "Options are: $validEnvStrings"
                    )
                }
                val datastoreEnv = DatastoreEnv.valueOf(datastoreEnvString)

                val datastoreProject = System.getenv(PROCESS_PROJECT_ENV_VAR)
                    ?: throw Exception(
                        "You must provide a datastore project. Either call " +
                            "`DBEnv.init(env, project)` or set the process " +
                            "environment variable $PROCESS_PROJECT_ENV_VAR.")
                init(datastoreEnv, datastoreProject)
            }
            DBEnvAndProject.env
        }

    internal fun getEnvAndProject(): DatastoreEnvWithProject {
        // This check is redundant, but it allows us to avoid the extra
        // overhead of firing off a coroutine every time we want to access the
        // env after it's initialized, as well as the deadlock risk of using a
        // `runBlocking` if this gets called from inside a coroutine.
        // Note that the fact that we can do this without holding the lock
        // depends on the fact that our setters for this.env, which do use the
        // lock, enforce that we can only set this.env once per process.
        if (this::env.isInitialized) {
            return this.env
        }
        val deferredInit = InitializationScope.async {
            maybeInitFromProcessEnv()
        }
        return runBlocking(InitializationScope.coroutineContext) {
            deferredInit.await()
        }
    }
}

/**
 * Manually initialize the datastore environment.
 *
 * If this isn't called prior to the first use of this object, we'll use
 * the environment from the process environment variable DATASTORE_ENV
 * and the project from the process environment variable DATASTORE_PROJECT.
 *
 * If the environment has already been initialized, throw
 * IllegalStateException.
 *
 * This is a thread-blocking function.
 */
fun initializeEnvAndProject(env: DatastoreEnv, project: String) {
    runBlocking(InitializationScope.coroutineContext) {
        DBEnvAndProject.initWithLocking(env, project)
    }
}

/**
 * Wrapper for the Google cloud datastore client.
 *
 * This allows us to share it across threads, even though the datastore context
 * must be thread local.
 */
private val datastoreClient: Deferred<com.google.cloud.datastore.Datastore> =
    InitializationScope.async(start = CoroutineStart.LAZY) {
        val envAndProject = DBEnvAndProject.getEnvAndProject()
        DatastoreBackendService()
            .backendForEnv(envAndProject.env)
            .getDatastore(envAndProject)
    }

/**
 * Internal value for tracking datastore context.
 *
 * Datastore context is how we track what transaction we're part of, if any.
 *
 * When we do async datastore operations, we need to make sure that when
 * control resumes, we get back the same context, so that we're still working
 * in the same transaction (or outside one). Part of this is having context be
 * thread local, but what we actually want is for context to be
 * coroutine-local. The transition from thread-local to coroutine local is
 * handled by the Datastore implementations.
 *
 * TODO(colin): we'll eventually also want to manage this in a way where we can
 * do some operations outside of an ongoing transaction (like ndb's
 * @non_transactional), as well as starting an independent transaction
 * (propagation mode INDEPENDENT).
 */
internal val localDB: ThreadLocal<org.khanacademy.datastore.Datastore> =
    ThreadLocal()

/**
 * Primary external-facing API for interacting with the datastore.
 *
 * Use this singleton for all datastore access, regardless of context.
 *
 * The internals of this singleton ensure that the datastore context
 * (transaction vs. not, which transaction if applicable) follows the way the
 * code reads, regardless of how asynchronous operations are actually executed
 * over potentially multiple background threads.
 */
val DB: org.khanacademy.datastore.Datastore
    get() =
        // NOTE: Datastore sets localDB in its constructor, so this
        // persistently initializes the context if it's initially null.
        localDB.get() ?: runBlocking(InitializationScope.coroutineContext) {
            Datastore(datastoreClient.await())
        }
