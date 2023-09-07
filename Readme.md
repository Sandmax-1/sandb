
Hash Indexes:

- Index stored in memory as a hash map containing keys and offsets to file position.
- This is very simple, but is actually used by Bitcask (the default storage engine for Riak) a no-sql key-value store
- This is well suited to a situations where each key is updated frequently, for example lets say products and stock in a shop. 
Where keys would be the products and stock the value. We have lots of writes (people buying things, restocking), but not too many distict keys.
You have a lot of writes per key, but you could feasibly keep the keys in memory

Compaction:
- As I've said we only ever append to a file, so how do we avoid filling up the disk? 
- A good solution to this is splitting up the log file into segments. 
- When the segment file reaches a certain size we close the file and start writing to a new file. 
- This allows for compaction of older segments. This entails throwing away duplicate keys in the segments. As 
we are only appening to files the latest record for a given key is the current record for the file so any older records can be thrown away 
- In these update heavy scenarios after compaction the segments should have significantly reduced in size and it is possible to merge multiple segments together. 
- Segments are never modified after they have been written so the compaction process writes to a new segment file.
- As such the compaction process can be run in a background thread so we can maintain the dbs availability and read from the old segment files during this process. Then once complete we switch over to the new segment files. Then the old segment files can be deleted.
- Each segment now has it's own in memory hash table and when searching for a value for a given key we search each hash map starting rom the newest to the oldest. 
- To delete a record a special tombstone record iis appended that will get cleaned up in the compaction process
- If our process/machine crashes we will lose our in memory hash maps. These can be recreated directly from the segment files, however this can take some time if the segment files are large. So some dbs will speed up recovery by storing a snapshot of the segments hashmap on disk for quicker recovery.

Append log:
Append only logs can seem wasteful at first glance,why don't we just update the file in place overwriting old values? However they provide some benefits:

- Appending and segment merging are sequential write operations which are generally much faster than random writes.

- Concurrency and crash recovery are much simpler if isegment files are append only, or immutable. You don't have to worry about the case where a crash happend while a value was being overwritten. 

- Merging old segments avoids the problem of data files getting fragmented over time.

Downsides:
- In order for this db to work we need to be able to keep the hashmaps in memory. If we have a very large number of keys this can become infeasible. In principle you could keep the hashmaps on disk, but it is difficult to make on disk hash tables perform efficiently. They require a lot of random access I/O, hash collisions are difficult to manage and they can be expensive to grow when full. 

- Range queries are not efficient as you have to look up key individually in the hash maps. 


