package org.khanacademy.datastore

import com.google.cloud.datastore.DatastoreTypeConverter
import com.google.cloud.datastore.StructuredQuery
import com.google.datastore.v1.PropertyFilter
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.khanacademy.metadata.Key
import org.khanacademy.metadata.Keyed
import java.time.LocalDateTime
import java.time.ZoneOffset

data class KeyTestModel(
    override val key: Key<KeyTestModel>
) : Keyed<KeyTestModel>

// Value used for toDatastoreFilter(cls) when we're testing internals with
// queries that don't correspond to a model class. In this case, we don't care
// abour property renaming, so it doesn't really matter what we pass as the
// class.
val dummyClass = KeyTestModel::class

class QueryTest : StringSpec({
    "It should convert byte array conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.EQUAL,
            "abcd".toByteArray()
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe PropertyFilter.Operator.EQUAL
        String(filter.propertyFilter.value.blobValue.toByteArray()) shouldBe
            "abcd"
    }

    "It should convert boolean conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.LESS_THAN,
            true
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe PropertyFilter.Operator.LESS_THAN
        filter.propertyFilter.value.booleanValue shouldBe true
    }

    "It should convert double conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.GREATER_THAN,
            3.0
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe PropertyFilter.Operator.GREATER_THAN
        filter.propertyFilter.value.doubleValue shouldBe 3.0
    }

    "It should convert long conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.LESS_THAN_OR_EQUAL,
            5L
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe
            PropertyFilter.Operator.LESS_THAN_OR_EQUAL
        filter.propertyFilter.value.integerValue shouldBe 5L
    }

    "It should convert string conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.GREATER_THAN_OR_EQUAL,
            "abcde"
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe
            PropertyFilter.Operator.GREATER_THAN_OR_EQUAL
        filter.propertyFilter.value.stringValue shouldBe "abcde"
    }

    "It should convert timestamp conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.GREATER_THAN_OR_EQUAL,
            LocalDateTime.ofEpochSecond(10, 0, ZoneOffset.UTC)
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe
            PropertyFilter.Operator.GREATER_THAN_OR_EQUAL
        filter.propertyFilter.value.timestampValue.seconds shouldBe 10L
    }

    "It should convert key conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(FieldQueryFilter(
            "aField",
            FieldCondition.LESS_THAN,
            Key<KeyTestModel>("KeyTestModel", "the-first-one")
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "aField"
        filter.propertyFilter.op shouldBe
            PropertyFilter.Operator.LESS_THAN
        val keyPath = filter.propertyFilter.value.keyValue.pathList
        keyPath.size shouldBe 1
        keyPath[0].kind shouldBe "KeyTestModel"
        keyPath[0].name shouldBe "the-first-one"
        keyPath[0].id shouldBe 0 // 0 is the protobuf default, not a real ID
    }

    "It should convert ancestor conditions correctly" {
        val filter = DatastoreTypeConverter.filterToPb(AncestorQueryFilter(
            Key<KeyTestModel>("KeyTestModel", "the-first-one")
        ).toDatastoreFilter(dummyClass))
        filter.propertyFilter.property.name shouldBe "__key__"
        filter.propertyFilter.op shouldBe
            PropertyFilter.Operator.HAS_ANCESTOR
        val keyPath = filter.propertyFilter.value.keyValue.pathList
        keyPath.size shouldBe 1
        keyPath[0].kind shouldBe "KeyTestModel"
        keyPath[0].name shouldBe "the-first-one"
        keyPath[0].id shouldBe 0 // 0 is the protobuf default, not a real ID
    }

    "The query DSL should correctly construct filters" {
        val builder = QueryFilterBuilder()
        with(builder) {
            "field1" eq 1L
            "field2" lt 2L
            "field3" gt 3L
            "field4" le 4L
            "field5" ge 5L
            hasAncestor(Key<KeyTestModel>("KeyTestModel", "the-first-one"))
        }

        val filters = builder.build()
        filters shouldBe listOf(
            FieldQueryFilter("field1", FieldCondition.EQUAL, 1L),
            FieldQueryFilter("field2", FieldCondition.LESS_THAN, 2L),
            FieldQueryFilter("field3", FieldCondition.GREATER_THAN, 3L),
            FieldQueryFilter("field4", FieldCondition.LESS_THAN_OR_EQUAL, 4L),
            FieldQueryFilter(
                "field5", FieldCondition.GREATER_THAN_OR_EQUAL, 5L),
            AncestorQueryFilter(
                Key<KeyTestModel>("KeyTestModel", "the-first-one"))
        )
    }

    "It correctly applies default query non-ordering" {
        val builder = com.google.cloud.datastore.Query.newEntityQueryBuilder()
            .applyOrdering(NoOrdering)
        builder.build().orderBy shouldBe listOf<StructuredQuery.OrderBy>()
    }

    "It correctly applies single ordering" {
        val builder = com.google.cloud.datastore.Query.newEntityQueryBuilder()
            .applyOrdering(orderBy("aField", SortOrder.DESC))
        val query = builder.build()
        query.orderBy.size shouldBe 1
        query.orderBy[0].property shouldBe "aField"
        query.orderBy[0].direction shouldBe
            StructuredQuery.OrderBy.Direction.DESCENDING
    }

    "It correctly applies multiple ordering" {
        val builder = com.google.cloud.datastore.Query.newEntityQueryBuilder()
            .applyOrdering(orderBy(
                Pair("aField", SortOrder.DESC),
                Pair("anotherField", SortOrder.ASC)
            ))
        val query = builder.build()
        query.orderBy.size shouldBe 2
        query.orderBy[0].property shouldBe "aField"
        query.orderBy[0].direction shouldBe
            StructuredQuery.OrderBy.Direction.DESCENDING
        query.orderBy[1].property shouldBe "anotherField"
        query.orderBy[1].direction shouldBe
            StructuredQuery.OrderBy.Direction.ASCENDING
    }
})
