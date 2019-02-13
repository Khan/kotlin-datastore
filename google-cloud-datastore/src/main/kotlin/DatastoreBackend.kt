/**
 * Environment-specific (test/dev/prod) datastore implementation loading.
 *
 * We'd like to be able to swap out implementations of the datastore depending
 * on environment, using the JVM ServiceLoader pattern. We'd also like to make
 * sure that (because the code that is going to consume the loaded services is
 * internal to this project) clients of this library have supplied at most one
 * implementation for a given environment.
 *
 * Clients can provide their own implementation if desired by providing a class
 * that implements org.khanacademy.datastore.DatastoreBackend, though we're
 * providing default implementations for prod/dev using the google cloud client
 * library and a test stub.
 * TODO(colin): implement these.
 *
 * Most clients won't want to use this file directly and instead should use the
 * DB singleton, which uses this service loader to select an implementation.
 */
package org.khanacademy.datastore

import java.util.ServiceLoader

/**
 * Environment to which a given DatastoreBackend applies.
 */
enum class DatastoreEnv {
    PROD,
    DEV,
    TEST
}

/**
 * Combination of an environment and a Google cloud project name.
 */
data class DatastoreEnvWithProject(
    // DEV, TEST, or PROD
    val env: DatastoreEnv,
    // The Google cloud project name for the datastore we're accessing.
    // For example, `khan-academy`
    val project: String
)

/**
 * Wrapper for datastore access that indicates the applicable environment.
 */
interface DatastoreBackend {
    // The environment(s) to which this backend applies.
    // We will verify that you've supplied exactly one implementation for the
    // current environment when loading the service.
    val envs: List<DatastoreEnv>

    fun getDatastore(
        envAndProject: DatastoreEnvWithProject
    ): com.google.cloud.datastore.Datastore
}

/**
 * Wrapper for a ServiceLoader<DatastoreBackend>.
 *
 * This exists for testing purposes; ServiceLoader is a final class and
 * therefore hard to swap out for testing.
 */
internal interface DatastoreBackendServiceLoader {
    fun allBackends(): List<DatastoreBackend>
}

/**
 * Default loader for a datastore backend; delegates to ServiceLoader.
 */
private class DefaultDatastoreBackendServiceLoader :
    DatastoreBackendServiceLoader {

    override fun allBackends(): List<DatastoreBackend> =
        ServiceLoader.load(DatastoreBackend::class.java)
            .asIterable()
            .toList()
}

/**
 * The service class that provides an environment-appropriate backend.
 */
internal class DatastoreBackendService(
    // In normal usage, don't provide a loader; this is here primarily for
    // testing the service itself.
    loader: DatastoreBackendServiceLoader? = null
) {

    private val registry: Map<DatastoreEnv, DatastoreBackend>

    fun backendForEnv(env: DatastoreEnv): DatastoreBackend =
        registry[env] ?: throw IllegalArgumentException(
            "No datastore backend found for env $env")

    init {
        val backends = (loader ?: DefaultDatastoreBackendServiceLoader())
            .allBackends()

        val buildingRegistry =
            mutableMapOf<DatastoreEnv, DatastoreBackend>()

        // Build the registry (in a somewhat complicated fashion in order to be
        // able to provide a good error message if you accidentally have two
        // implementations available for a given environment).
        for (backend in backends) {
            for (env in backend.envs) {
                if (env in buildingRegistry) {
                    val existing = buildingRegistry.getValue(env)::class
                        .qualifiedName
                    val other = backend::class.qualifiedName
                    throw Exception(
                        "More than one datastore backend for $env: " +
                            "$existing, $other")
                }
                buildingRegistry[env] = backend
            }
        }
        registry = buildingRegistry
    }
}
