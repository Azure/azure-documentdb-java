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