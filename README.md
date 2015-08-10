#Force.com Replicator

This tool is used to make local copies of Salesforce standard and custom objects. 
It simply exports all fields for an object you define in config.json, 
every consecutive run after the initial will select all rows with a 
LastModifiedDate greater than that object's last sync date.

It is currently configured to work with MySQL and InnoDB via PDO but should be 
flexible enough to implement other DBMSs and engines without much hassle.

The code that interacts with the Salesforce Bulk API is taken from a tool suite
called [Workbench](https://github.com/ryanbrainard/forceworkbench).


###Configuration

The config.json file is where you define which objects to sync along with your Salesforce credentials.

The fields are as follows:

* objects
  * {objectName}
    * fields 
      * {field\_api\_name\_1} - if just an asterix is provided, all fields will be retrieved
      * {field\_api\_name\_2}
      * {field\_api\_name\_n}

* salesforce
  * user
  * pass - optional, prompts on each run if not present
  * endpoint - optional, defaults to endpoint provided in partner.wsdl (production)

* database
  * type - currently only supports MySQL
  * host - hostname to connect to
  * user
  * pass
  * database - must already exist

See the provided config.json for an example.

#####PHP

Depending on the number of records and fields you are replicating you may need
to increase php's *memory\_limit* parameter.

#####MySQL

You may also need to increase your MySQL server's *max\_allowed\_packet*
parameter to allow for the large inserts, the value you set will be relative to
DB.php's batchSize parameter and the number of fields being replicated.

