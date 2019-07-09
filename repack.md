# REPACK PROCEDURE

## STATIC DATA
* add a ACCESS SHARE lock on the origin's table
* create a new empty table in the repack schema
* copy the data
* build the indices

## DYNAMIC DATA
* add a ACCESS SHARE lock on the origin's table
* create a new empty table in the repack schema
* create a collecting table with the same origin table's data type
* create a trigger on the origin table collecting the data changes associated with the transaction id
* copy the data noting transaction id at the start of the data copy
* build the primary or the unique key (compulsory for replaying the data)
* replay the logged data on the destination's table
* for each index on the source table, build that index on the destination table, then replay the logged data 
*  when all the indices are in place, replay the last logged data
* acquire an exclusive lock on the origin and destination table preventing the writes but not the reads
* replay the last rows on the destination table
* swap the relations
* release the locks

