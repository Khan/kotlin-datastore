package org.khanacademy.datastore

import com.google.cloud.datastore.Datastore
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class DatastoreBackendTest : StringSpec({
    val prodAndDevBackend = object : DatastoreBackend {
        override val envs = listOf(DatastoreEnv.DEV, DatastoreEnv.PROD)
        override fun getDatastore(env: DatastoreEnv): Datastore = mock()
    }
    val testBackend = object : DatastoreBackend {
        override val envs = listOf(DatastoreEnv.TEST)
        override fun getDatastore(env: DatastoreEnv): Datastore = mock()
    }
    "It should use the env parameter to return a backend" {
        val mockLoader = mock<DatastoreBackendServiceLoader> {
            on { allBackends() } doReturn listOf(prodAndDevBackend, testBackend)
        }

        val backendService = DatastoreBackendService(mockLoader)
        backendService.backendForEnv(DatastoreEnv.PROD) shouldBe
            prodAndDevBackend
        backendService.backendForEnv(DatastoreEnv.DEV) shouldBe
            prodAndDevBackend
        backendService.backendForEnv(DatastoreEnv.TEST) shouldBe testBackend
    }

    "It should throw if you provide multiple implementations for an env" {
        val mockLoader = mock<DatastoreBackendServiceLoader> {
            on { allBackends() } doReturn listOf(
                prodAndDevBackend, testBackend, testBackend)
        }

        shouldThrow<Exception> {
            DatastoreBackendService(mockLoader)
        }
    }

    "It should throw if there's no implementation for an env you're using" {
        val mockLoader = mock<DatastoreBackendServiceLoader> {
            on { allBackends() } doReturn listOf(prodAndDevBackend)
        }

        shouldThrow<IllegalArgumentException> {
            DatastoreBackendService(mockLoader)
                .backendForEnv(DatastoreEnv.TEST)
        }
    }
})
