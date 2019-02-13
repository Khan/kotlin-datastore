package org.khanacademy.datastore

import com.nhaarman.mockitokotlin2.mock

class InternalDatastoreTestStub : DatastoreBackend {
    override val envs: List<DatastoreEnv> = listOf(DatastoreEnv.TEST)
    // TODO(colin): come up with a better mocking strategy here that doesn't
    // leak state across tests if you forget to mock this yourself.
    override fun getDatastore(
        envAndProject: DatastoreEnvWithProject
    ): com.google.cloud.datastore.Datastore = mock()
}
