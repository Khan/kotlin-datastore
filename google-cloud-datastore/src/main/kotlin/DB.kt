/**
 * Environment and management of our DB singleton object.
 *
 * The DB singleton is the way external clients should use the datastore.
 */
package org.khanacademy.datastore

/**
 * Holder for the datastore environment.
 *
 * If `init` is called before the first datastore access, we use its arguments
 * as the environment; otherwise, we use the value passed in the environment
 * variables DATASTORE_ENV and DATASTORE_PROJECT.
 */
internal object DBEnvAndProject {
    // When creating a datastore backend, we'll set the environment from this
    // process environment variable if not set from code.
    const val PROCESS_ENV_VAR = "DATASTORE_ENV"
    // Likewise, we'll set the datastore project from this process environment
    // variable if not set from code.
    const val PROCESS_PROJECT_ENV_VAR = "DATASTORE_PROJECT"

    private lateinit var env: DatastoreEnvWithProject

    internal fun init(env: DatastoreEnv, project: String) {
        synchronized(this) {
            if (this::env.isInitialized) {
                throw IllegalStateException(
                    "Datastore environment has already been initialized.")
            }
            this.env = DatastoreEnvWithProject(env, project)
        }
    }

    internal fun getEnvAndProject(): DatastoreEnvWithProject {
        if (!this::env.isInitialized) {
            val datastoreEnv = System.getenv(PROCESS_ENV_VAR)
                ?.let { DatastoreEnv.valueOf(it) }
                // TODO(colin): more specific exception class?
                ?: throw Exception(
                    "You must provide a datastore environment. Either call " +
                        "`DBEnv.init(env, project)` or set the process " +
                        "environment variable $PROCESS_ENV_VAR.")
            val datastoreProject = System.getenv(PROCESS_PROJECT_ENV_VAR)
                ?: throw Exception(
                    "You must provide a datastore project. Either call " +
                        "`DBEnv.init(env, project)` or set the process " +
                        "environment variable $PROCESS_PROJECT_ENV_VAR.")
            init(datastoreEnv, datastoreProject)
        }
        return this.env
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
 */
fun initializeEnvAndProject(env: DatastoreEnv, project: String) =
    DBEnvAndProject.init(env, project)

/**
 * Wrapper for the Google cloud datastore client.
 *
 * This allows us to share it across threads, even though the datastore context
 * must be thread local.
 */
internal val datastoreClient: com.google.cloud.datastore.Datastore by lazy {
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
        // NOTE: DatastoreWithContext sets localDB in its constructor,
        // so this persistently initializes the context if it's initially null.
        localDB.get() ?: Datastore(datastoreClient)
