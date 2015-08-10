<?php

/**
 * FORCE.COM REPLICATOR
 * @author Tom Gangemi
 *
 * Replicate.php
 * The main class used to perform replication.
 */

require_once('Salesforce.php');
require_once('StorageDB.php');

class Replicator {

	private $sf = null;             // Salesforce interface
	private $db = null;             // database interface
	private $config = null;         // the config file in memory
	private $schemaSynced = false;  // has the schema been synced
	private $fieldTypes = null;     // keep track of field types
	private $fieldsToQuery = null;

	const CONFIG_FILE = 'config.json';

	function __construct() {

		$this->loadConfig();
		
		// initialise Salesforce and database interfaces
		if(isset($this->config['salesforce']['pass']))
			$pass = $this->config['salesforce']['pass'];
		else
			$pass = null;

		$this->sf = new Salesforce($this->config['salesforce']['user'], $pass);
		if(isset($this->config['salesforce']['endpoint']))
			$this->sf->setEndpoint($this->config['salesforce']['endpoint']);

		$this->db = new StorageDB(
			$this->config['database']['host'], 
			$this->config['database']['user'], 
			$this->config['database']['pass'], 
			$this->config['database']['database']
		);
		
		// convert all fields and object names to lowercase and add default fields
		$this->config['objects'] = array_change_key_case($this->config['objects'], CASE_LOWER);
		foreach($this->config['objects'] as $objectName => $object) {
			$this->config['objects'][$objectName]['fields'] = array_map('strtolower', $object['fields']);
			array_unshift($this->config['objects'][$objectName]['fields'], 'lastmodifieddate');
			array_unshift($this->config['objects'][$objectName]['fields'], 'id');
		}

	}

	/**
	 * Export data from salesforce and import into local database, will sync the schema first.
	 */
	public function syncData() {

		// store everything in UTC
		$originalTz = date_default_timezone_get();
		date_default_timezone_set('UTC');

		// make sure schema is synced
		if(!$this->schemaSynced)
			$this->syncSchema();

		// load field types into memory
		if($this->fieldTypes === null)
			foreach(array_keys($this->fieldsToQuery) as $objectName)
				$this->fieldTypes[$objectName] = $this->db->getFields($objectName);

		// start syncing each object
		foreach($this->config['objects'] as $objectName => $object) {

			echo "Starting sync - $objectName".PHP_EOL;

			$thisSync = date("Y-m-d H:i:s");
			$previousSync = date("Y-m-d\TH:i:s\Z", strtotime($this->db->getMostRecentSync($objectName)));
			
			// open memory stream for storing csv
			$fh = fopen("php://memory", 'r+');			

			$result = $this->sf->batchQuery($objectName, $this->fieldsToQuery[$objectName], $previousSync, $fh);
			if($result === false) {
				echo "No new or modified records found" . PHP_EOL;
				continue;
			}			

			$header = fgetcsv($fh, 0, ',', '"', chr(0));
			$fieldCount = count($header);

			$fieldsToConvert = array();
			$fieldsToEscape = array();
			// decide which fields need to be treated before inserting into local db
			foreach($header as $k => $field) {
				$header[$k] = $field = strtolower($field);
				$convertFunction = $this->getConvertFunction($this->fieldTypes[$objectName][$field]);
				
				if($convertFunction != null)
					$fieldsToConvert[$k] = $convertFunction;

				if($this->isEscapeRequired($this->fieldTypes[$objectName][$field]))
					$fieldsToEscape[] = $k;
			}			
			
			echo "Begin converting and upserting data...".PHP_EOL;

			$this->db->beginTransaction();
			$this->db->logSyncHistory($objectName, $thisSync);

			$upsertCsv = array();
			$rowCount = 0;
			$upsertBatchSize = @$this->config['objects'][$objectName]['options']['upsertBatchSize'];

 			while(($data = fgetcsv($fh, 0, ',', '"', chr(0))) !== false) {

				if(count($data) != $fieldCount) {
					print_r($data);
					throw new Exception('error parsing row');
				}

				foreach($fieldsToConvert as $key => $func)
					$data[$key] = $this->$func($data[$key]);

				foreach($fieldsToEscape as $key)
					$data[$key] = $this->db->quote($data[$key]);

				$upsertCsv[] = implode(',', $data);
				$rowCount++;
				if($upsertBatchSize && $rowCount >= $upsertBatchSize) {
					$this->db->upsertCsvValues($objectName, $header, $upsertCsv, $upsertBatchSize);
					$upsertCsv = array();
					$rowCount = 0;
				}
 			}

 			if($rowCount > 0)
				$this->db->upsertCsvValues($objectName, $header, $upsertCsv, $upsertBatchSize);

			$this->db->commitTransaction();
			fclose($fh);

			echo "Finished sync - $objectName".PHP_EOL.PHP_EOL;
		}

		// revert to original timezone
		date_default_timezone_set($originalTz);
	}

	/**
	 * Convert Salesforce object definitions into database tables and create fields as defined by createFieldDefinition().
	 */
	public function syncSchema() {
		// determine which sObjects need to be queried and determine fields that need adding

		$objectsToQuery = array();
		$existingFields = array();
		foreach($this->config['objects'] as $objectName => $object) {
		
			// get existing fields
			if($this->db->tableExists($objectName))
				$existingFields[$objectName] = $this->db->getFields($objectName);
			else
				$existingFields[$objectName] = array();

			$fields = $object['fields'];

			// this gets overwritten later for objects with *
			$this->fieldsToQuery[$objectName] = $fields;
	
			if(in_array('*', $fields)) {
				// add all fields from Salesforce

				$objectsToQuery[$objectName] = true;

			} else if(is_array($existingFields[$objectName])) {
				// some fields already exist, check for new ones

				$diff = array_diff($fields, array_keys($existingFields[$objectName]));
				if(count($diff) > 0)
					$objectsToQuery[$objectName] = $diff;

			} else {
				// add all fields from config

				$objectsToQuery[$objectName] = $fields;

			}

		}

		if(count($objectsToQuery) > 0) {

			$objectUpserts = array();

			// query object schemas from Salesforce
			$describeResult = $this->sf->getSObjectFields(array_keys($objectsToQuery));

			foreach ($describeResult as $objectName => $fields) {

				$objectName = strtolower($objectName);
				$allFields = array_map(function($field) {return strtolower($field->name);}, $fields);
				$newFields = array();

				if(is_array($objectsToQuery[$objectName])) {
					// check if any fields defined in config don't exist in Salesforce		

					$badFields = array_diff($objectsToQuery[$objectName], $allFields);
					if(count($badFields) > 0)
						throw new Exception("Invalid field(s): " . implode(', ', $badFields));

					// get describe data for fields that need to be added
					foreach($fields as $field)
						if(in_array(strtolower($field->name), $objectsToQuery[$objectName]))
							$newFields[] = $field;

				} else if(array_key_exists($objectName, $objectsToQuery)) {
					// add all fields from Salesforce

					$this->fieldsToQuery[$objectName] = $allFields;				
					
					foreach($fields as $field)
						if(!array_key_exists(strtolower($field->name), $existingFields[$objectName]))
							$newFields[] = $field;
				}

				// get mysql definition for each field
				$fieldDefs = array();
				foreach($newFields as $field)
					$objectUpserts[$objectName][$field->name] = $this->createFieldDefinition($field);

			}
			
			// update and create tables
			foreach($objectUpserts as $objectName => $fieldDefs)
				$this->db->upsertObject($objectName, $fieldDefs);

		}

		$this->schemaSynced = true;
	}

	private function loadConfig() {

		$this->config = json_decode(file_get_contents($this::CONFIG_FILE), true);
		if($this->config === null)
			throw new Exception("error reading " . $this::CONFIG_FILE);		

		// validate salesforce config
		if(empty($this->config['salesforce']) || empty($this->config['salesforce']['user']))
			throw new Exception($this::CONFIG_FILE . ' is missing required Salesforce parameters');

		// validate database config
		$db = $this->config['database'];
		if(empty($db) || empty($db['type']) || empty($db['host']) || 
		   empty($db['user']) || !isset($db['pass']) || empty($db['database']))
			throw new Exception($this::CONFIG_FILE . ' is missing required database parameters');

	}

	private function createFieldDefinition($field) {

		$databaseType = $this->config['database']['type'];
		switch($databaseType) {
			case 'mysql':
				switch($field->type) {
					case 'id':
					case 'reference':
					case 'string':
					case 'picklist':
					case 'textarea':
					case 'phone':
					case 'email':
						if($field->length >= 1024)
							return 'TEXT';
						else
							return 'VARCHAR(' . $field->length . ')';
					case 'boolean':
						return 'BIT(1)';
					case 'date':
						return 'DATE';
					case 'datetime':
						return 'DATETIME';
					case 'double':
					case 'currency':
						return 'DECIMAL(' . $field->precision . ',' . $field->scale . ')';
					default:
						throw new Exception("unsupported field type ({$field->type})");
				}
			default:
				throw new Exception("unsupported database type ($databaseType)");
		}
	}

	private function getConvertFunction($fieldType) {

		if($pos = strrpos($fieldType,'('))
			$fieldType = substr($fieldType, 0, $pos);

		switch($fieldType) {
			case 'datetime':
				return 'convertDatetime';
			case 'bit':
				return 'convertBoolean';
			case 'decimal':
				return 'convertDecimal';
			default:
				return null;
		}
	}

	private function isEscapeRequired($fieldType) {

		if($pos = strrpos($fieldType,'('))
			$fieldType = substr($fieldType, 0, $pos);

		switch($fieldType) {
			case 'decimal':
			case 'bit':
				return false;
			default:
				return true;
		}
	}	

	private function convertBoolean($value) {
		if($value == 'true')
			return 1;
		else
			return 0;
	}

	private function convertDatetime($value) {
		return empty($value) ? 'NULL' : date("Y-m-d H:i:s", strtotime($value));
	}

	private function convertDecimal($value) {
		return $value === '' ? 'NULL' : $value;
	}
}

if(!count(debug_backtrace())) { // only run if being called directly
	
	$r = new Replicator();
	$r->syncData();

}