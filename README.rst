repcloud
------------------------------
repcloud is a repacker for postgresql tables. Unlikely pgrepack there's no need for extension or external libraries.

The procedure can repack the tables using a similar strategy like pgrepack but without the physical file swap.

This allow the procedure to be executed on environment where is not possible to install external libraries or 
there is no super user access. (e.g. cloud hosted databases, hence the name) 

When repacking the process creates a copy of the original table and using a select insert copies the existing data into the new relation.
A trigger on the original table stores the data changes for which are replayed on the new one before attempting the swap.

All the existing indices, foreign keys and referencing foreign keys are created before the swap.
Views and materialised views referencing the repacked table are dropped and created as well.

Sponsors
...................................
Coding repcloud has been possible thanks to the sponsorhip of `Cleo AI. https://www.meetcleo.com/ <https://www.meetcleo.com/>`_  


.. image:: images/cleo_logo.png
        :target: https://www.meetcleo.com/


Configuration 
...................................

The script which executes the repack is rpcl. At its first execution the it creates a directory in the user's home named .repcloud
Under this directory there are three other subfolders.

.repcloud/logs where the procedure's logs are stored 
.repcloud/pid where the procedure's pid file is stored
.repcloud/config where the configurations are stored.
In config is also copied the file config-example.toml which is the template for any other configuration.

the command line rpcl accepts the following options:

* --config specifies the config file to use in .repcloud/config. if omitted defaults to default.toml
* --connection specifies which connection to use within the configuration file. if omitted any connection is used for repacking
* --debug forces the process in foreground with log sent both to file and console

Without debug and with the log_dest set to file the process starts in background.

rpcl accepts the following commands

* show_connections shows the connections defined within the configuration file
* create_schema creates the repack helper schemas in the target database
* drop_schema drops the repack helper schemas from the target database. if any table failed the repack its copy is dropped as well
* repack_tables repack the tables listed within the connection
* prepare_repack prepares the tables for the repack. creates the new table, copy the data and builds the indices. then it stops before the swap.

Please note that prepare_repack requires much more space than repack_tables as all the tables are copied and prepared for the repack instead of repacking and dropping 
them one by one.


In the configuration file the notifier and notifier.email sections allow to setup an email notification for when the repack or prepare repack is complete.
	

Limitations
............................

The procedure needs to be able to drop all the objects involved in the repack. Therefore the login user must be the object's owner or 
should be able to drop the objects.

The swap requires an exclusive lock on the old  relation for the time necessary to move the new relation into the correct schema and drop the old relation.
If an error occur during this phase everything is rolled back. The procedure resumes the replay and will attempt again the swap after a sufficient amount of data has been replayed.

Currently there is no support for single index repack or tablespace change.

Currently there is no stop method for the background repack process.

A connection must have the header in the form of [connections.<connection_name>]

Each connection requires the database connection data: user,password,port,host,database,sslmode.

The lists schemas and tables allow to specify which schema or tables we want to repack. If omitted the repack will process any table within the database.

The parameter max_replay_rows specifies how many rows should be replayed at once during the replay phase.
lock_timeout specifies how long the process should wait for acquiring the lock on the table to swap before giving up. If the lock_timeout expires the swap is delayed
until the a sufficient amount of rows are replayed again.

check_time specifies the time between two checks for changed data on the repacked table. The value will be matched against the replay speed in order to determine
if the replay can reach the consistent status with the original table.
If it's not possible the swap attempt aborts.

License
...................................
repcloud is released under the terms of the `PostgreSQL license - https://opensource.org/licenses/postgresql <https://opensource.org/licenses/postgresql>`_  
