## Changes in 1.9.1 : ##

    - Added support for BoundedStaleness consistency level.
    - Added support for direct connectivity for CRUD operations for partitioned collections.
    - Fixed a bug in querying a database with SQL.
    - Fixed a bug in the session cache where session token may be set incorrectly.

## Changes in 1.9.0 : ##

    - Added support for cross partition parallel queries.
    - Added support for TOP/ORDER BY queries for partitioned collections. 
    - Added support for strong consistency.
    - Added support for name based requests when using direct connectivity.
    - Fixed to make ActivityId stay consistent across all request retries.
    - Fixed a bug related to the session cache when recreating a collection with the same name.
    - Fixed issues with Java Doc for Java 1.8.

## Changes in 1.8.1 : ##

    - Fixed a bug in PartitionKeyDefinitionMap to cache single partition collection and not making extra fetch partition key requests.
	- Fixed a bug in incorrect partition key value scenario to not retry.

## Changes in 1.8.0 : ##

    - Added support for endpoint management for geo-distributed databases. User can customize
      how endpoints are selected for each request by setting the ConnectionPolicy.EnableEndpointDiscovery 
      and ConnectionPolicy.PreferredLocations properties when creating a DocumentClient instance.
    - Added support for automatic retry on throttled requests with options to customize the max retry attempts
      and max retry wait time.  See RetryOptions and ConnectionPolicy.getRetryOptions().
    - Deprecated IPartitionResolver based custom partitioning code. Please use partitioned collections for higher storage and throughput.

## Changes in 1.7.1 : ##

    - Setting Cookie policy on httpclient to ignoring server cookies as we don't use them.
    - Added support to retry on throttled requests.
    - Removed e.printstacktrace from source code and replaced with Logger operations as appropriate.
    - Changed test code structure and added mocking framework for unit testing.

## Changes in 1.7.0 : ##

	- Added support for expiring documents by setting the default time-to-live on collections and time-to-live override on documents.

## Changes in 1.6.0 : ##

	- Added support to set offer throughput for collections created with variable pricing structure.
	- Added support to create collections with multiple partitions by specifying a partition key definition.
	- Added support to send partition key in RequestOptions and FeedOptions to specify the scope of the request or the query.
	- Added support to specify a partition key in the Permission object to scope the permission to a partition. 
     
## Changes in 1.5.1 : ##

- Fixed a bug in HashPartitionResolver to generate hash values in little-endian order to be consistent with other SDKs.

## Changes in 1.5.0 : ##

- Added Client-side sharding framework to the SDK. Implemented HashPartionResolver and RangePartitionResolver classes.

## Changes in 1.4.0 : ##

- Implement Upsert. New upsertXXX methods added to support Upsert feature.
- Implement ID Based Routing. No public API changes, all changes internal.

## Changes in 1.3.0 : ##

- Release skipped to bring version number in alignment with other SDKs

## Changes in 1.2.0 : ##

- Supports GeoSpatial index.
- Validates id property for all resources. Ids for resources cannot contain ?, /, #, \\, characters or end with a space.
- Adds new header "index transformation progress" to ResourceResponse.

## Changes in 1.1.0 : ##

- Implements V2 indexing policy
