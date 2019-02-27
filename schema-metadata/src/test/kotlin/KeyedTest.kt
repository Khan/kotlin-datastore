package org.khanacademy.metadata

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class KeyTestModel(override val key: Key<KeyTestModel>) : Keyed<KeyTestModel>

class KeyedTest : StringSpec({
    "The alternate string-based key constructor should set a key name" {
        val key = Key<KeyTestModel>("KeyTestModel", "some_key_name")
        key.idOrName shouldBe KeyName("some_key_name")
    }

    "The alternate long-based key constrctor should set a key id" {
        val key = Key<KeyTestModel>("KeyTestModel", 1L)
        key.idOrName shouldBe KeyID(1L)
    }
})
