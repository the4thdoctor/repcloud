# repack procedure

## static data
* add a lock for share on the origin's table
* create a new empty table in the repack schema
* copy the data
* build the indices

## dynamic data
* add a lock for share on the origin's table
* create a new empty table in the repack schema
* create a collecting table with the same table's data type
* create a trigger which collects the changes along with the transaction id
* copy the data saving the transaction id of the start data copy
* replay the data
* for each index in the table build an index then replay the data 
*  replay the last data
* lock the origin table in exclusive mode
* replay the final rows 
* swap the relations
* release the locks

