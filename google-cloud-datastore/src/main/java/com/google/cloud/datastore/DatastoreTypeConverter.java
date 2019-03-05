package com.google.cloud.datastore;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Filter;

/**
 * Java shim to allow us to look at the contents of queries and entities.
 *
 * Query filters have no methods that let you look at the filter conditions. We
 * need to be able to do this for our test stub. The only thing we may be able
 * to access is the package-public converter to a datastore filter protobuf.
 *
 * Likewise, for comparing query filter values, we want to use the protobuf
 * representation of the value, so we need to be able to convert entities to
 * their own protobuf representations.
 *
 * This shim lives in the Google cloud datastore package to allows us to access
 * these method for our test stub.
 *
 * Don't use this outside the test stub; this is likely only package-visible
 * for a reason and we don't want to have production code using it.
 */
public class DatastoreTypeConverter {
    public static Filter filterToPb(StructuredQuery.Filter filter) {
        return filter.toPb();
    }

    public static Entity entityToPb(com.google.cloud.datastore.Entity entity) {
        return entity.toPb();
    }

    public static Query.ResultType queryResultType(Query query) {
        return query.getType();
    }
}
