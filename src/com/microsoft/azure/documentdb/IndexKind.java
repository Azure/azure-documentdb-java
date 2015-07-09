package com.microsoft.azure.documentdb;

// These are the indexing types available for indexing a path.
// For additional details, refer to
// http://azure.microsoft.com/documentation/articles/documentdb-indexing-policies/#ConfigPolicy.
public enum IndexKind {
    // The index entries are hashed to serve point look up queries.
    // Can be used to serve queries like: SELECT * FROM docs d WHERE d.prop = 5
    Hash,

    // The index entries are ordered. Range indexes are optimized for inequality predicate queries with efficient range
    // scans.
    // Can be used to serve queries like: SELECT * FROM docs d WHERE d.prop > 5
    Range
}
