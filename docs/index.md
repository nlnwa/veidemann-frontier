# Veidemann Frontier

## Datamodel
The data model for queueing uris in the frontier is quite complex and is a mix of data stored in RethinkDB and indexes
in Redis. Detailed description of datamodel and operations done on the data can be found in the following documents: 
* [Crawl queue in Redis](redis_uri_queue.md)
* [Crawl Host Groups in Redis](redis_crawl_host_groups.md)
* [Already included in Redis](redis_already_included.md)