repcloud
------------------------------
repcloud is a repacker for PostgreSQL tables. Unlikely pgrepack there's no need for extension or external libraries.

The procedure can repack the tables using a similar strategy like pgrepack, but without the physical file swap.

This allow the procedure to be executed on an environment where it is not possible, to install external libraries, or
there is no super user access (e.g. cloud hosted databases, hence the name).

When repacking the process creates a copy of the original table and using a select insert copies the existing data into the new relation.
A trigger on the original table stores the data changes for which are replayed on the new one before attempting the swap.

All the existing indices, foreign keys, and referencing foreign keys are created before the swap.
Views and materialised views referencing the repacked table are dropped and created as well.

Acknowledgement
...................................
Coding repcloud has been possible thanks to the sponsorhip of `Cleo AI. https://www.meetcleo.com/ <https://www.meetcleo.com/>`_

.. image:: https://github.com/the4thdoctor/repcloud/blob/master/images/cleo_logo.png
        :target: https://www.meetcleo.com/

Configuration
...................................

The script, which executes the repack, is rpcl. At its first execution the it creates a directory in the user's home named .repcloud
Under this directory there are three other subfolders.

.repcloud/logs where the procedure's logs are stored
.repcloud/pid where the procedure's pid file is stored
.repcloud/config where the configurations are stored.
THe file config-example.toml is copied into the the folder ./replcoud/config. It is a template for the configuration.

the command line rpcl accepts the following options:

* --config specifies the config file to use in .repcloud/config. If omitted tje defaults configuration default.toml will be used
* --connection specifies which connection to use within the configuration file. if omitted any connection is used for repacking
* --debug forces the process in foreground with log sent both, to file and console

Without debug and with the log_dest set to file, the process starts in background.

rpcl accepts the following commands

* show_connections shows the connections defined within the configuration file
* create_schema creates the repack helper schemas in the target database
* drop_schema drops the repack helper schemas from the target database. if any table failed the repack, its copy is dropped as well
* repack_tables repacks the tables listed within the connection
* prepare_repack prepares the tables for the repack, creates the new table, copies the data, and builds the indices. It stops before the swap.

Please note that prepare_repack requires much more space than repack_tables because all tables are copied and prepared for the repack instead of repacking and dropping
them one by one.


In the configuration file the notifier and notifier.email sections allow to setup an email notification, which is triggered when the repack or prepare repack process is complete.

Fillfactor
+++++++++++++++
The tool supports the **fillfactor** setup for the repacked tables. This is possible using a specific configuration file  stored in the directory *~/.repcloud/config/table_conf*

The file describing the storage settings must be named after the configuration and the connection which the settings apply in the form *<configuration>_<connection>.toml*.

For example, if we are using the configuration *default.toml* where there is the connection *repack* the table configuration file's name should be 
*default_repack.toml* 

If the table settings file is not present then the default values.

Inside the directory *~/.repcloud/config/table_conf* there is an example file to help the configuration.

The configuration at moment supports only **fillfactor** as storage parameter.

A global fillfactor which applies to any table in the database can be set under the section **[storage]**.

Schema wide fillfactor is supported adding the value under the section **[storage.schemaname]**.

Fillfactor for tables can be set using the section named after the schema and the table **[storage.schemaname.tablename]**.

The example configuration file sets the fillfactor:

  * for all the tables in the database to 100
  * for all the tables in the schema foo to 80
  * for the table foo.bar to 30


::

    #table configuration example
    # storage data. currently only fillfactor is allowed
    
    #set the fillfactor for all the tables 
    [storage]
    fillfactor = 100 
    
    #sets the fillfactor for all the tables in the schema foo
    [storage.foo]
    fillfactor = 80 
    
    #set the fillfactor for the table foo.bar
    [storage.foo.bar]
    fillfactor = 30 



Limitations
............................

The procedure needs to be able to drop all the objects involved in the repack. Therefore the login user must be the object's owner or
should be able to drop the objects.

The swap requires an exclusive lock on the old relation for the time necessary to move the new relation into the correct schema and drop the old relation.
If an error occurs during this phase, everything is rolled back. The procedure resumes the replay and will attempt again the swap after a sufficient amount of data has been replayed.

Currently there is no support for single index repack or tablespace change.

Currently there is no stop method for the background repack process.

A connection must have the header in the form of [connections.<connection_name>]

Each connection requires the database connection data: user, password, port, host, database, sslmode.

The lists schemas and tables allow to specify which schema or tables we want to repack. If omitted the repack will process any table within the database.

The parameter max_replay_rows specifies how many rows should be replayed at once during the replay phase.
lock_timeout specifies how long the process should wait for acquiring the lock on the table to swap before giving up. If the lock_timeout expires, the swap is delayed
until a sufficient amount of rows are replayed again.

check_time specifies the time between two checks for changed data on the repacked table. The value will be matched against the replay speed in order to determine
if the replay can reach the consistent status with the original table.
If it's not possible the swap attempt aborts.

License
------------------------------
repcloud is released under the terms of the `PostgreSQL license - https://opensource.org/licenses/postgresql <https://opensource.org/licenses/postgresql>`_
