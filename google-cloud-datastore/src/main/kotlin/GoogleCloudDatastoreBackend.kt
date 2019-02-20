package org.khanacademy.datastore

import com.google.cloud.datastore.DatastoreOptions

/**
 * DatastoreBackend using the google-cloud-java client library for datastore.
 *
 * This is usable for prod, as well as using the gcloud datastore emulator in
 * the development environment. Refer to the datastore emulator docs for setup
 * instructions for the dev environment. This class will not automatically set
 * up the emulator in dev.
 * See https://cloud.google.com/datastore/docs/tools/datastore-emulator
 * for more info on dev vs. prod configuration.
 * TODO(colin): is there a way we can configure it to set up and use the
 * emulator automatically? (we might be able to use a call out to `ps` to find
 * the emulator process and set the env variables correctly using that?)
 * TODO(colin): INFRA-2792 we should at least assert that if we're in DEV, the
 * env variable required to use the emulator is set, so that we're not
 * accidentally writing to prod from the devserver.
 */
class GoogleCloudDatastoreBackend : DatastoreBackend {
    override val envs = listOf(DatastoreEnv.PROD, DatastoreEnv.DEV)
    override fun getDatastore(
        envAndProject: DatastoreEnvWithProject
    ): com.google.cloud.datastore.Datastore =
        DatastoreOptions.newBuilder()
            .setProjectId(envAndProject.project)
            .build()
            .service
}
