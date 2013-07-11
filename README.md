#Force.com Replicator

This tool is used to make local copies of Salesforce standard and custom objects. 
It simply exports all fields for an object you define in config.json, 
every consecutive run after the initial will select all rows with a 
LastModifiedDate greater than that object's last sync date.

It is currently configured to work with MySQL and InnoDB via PDO but should be 
flexible enough to implement other DBMSs and engines without much hassle.

The code that interacts with the Salesforce Bulk API is taken from a tool suite
called [Workbench](https://github.com/ryanbrainard/forceworkbench).


###Usage & Configuration

The first thing you will need to do is edit the config.json file. 
If you choose to leave the Salesforce password blank, there will be a prompt whenever it is required.

Currently, this is intended for command line use but implementation as part of a
larger project or webpage is simply a matter of removing the password prompt and echo statements.

#####PHP

Depending on the number of records and fields you are replicating you may need
to increase php's *memory\_limit* parameter.

#####MySQL

You may also need to increase your MySQL server's *max\_allowed\_packet*
parameter to allow for the large inserts, the value you set will be relative to
DB.php's batchSize parameter and the number of fields being replicated.

