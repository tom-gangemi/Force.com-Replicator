<?php

/**
 * FORCE.COM REPLICATOR
 * @author Tom Gangemi
 *
 * Replicate.php
 * The main class used to perform replication.
 */

require_once('Salesforce.php');
require_once('DB.php');

class Replicator {

	private $sf = null;             // Salesforce interface
	private $db = null;             // database interface
	private $config = null;         // the config file in memory
	private $schemaSynced = false;  // has the schema been synced
	private $fieldTypes = null;     // keep track of field types

	const CONFIG_FILE = 'config.json';

	function __construct() {

		$this->loadConfig();
		
		// initialise Salesforce and database interfaces
		if(isset($this->config['salesforce']['pass']))
			$pass = $this->config['salesforce']['pass'];
		else
			$pass = null;

		$this->sf = new Salesforce($this->config['salesforce']['user'], $pass);
		$this->db = new DB(
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
			foreach(array_keys($this->config['objects']) as $objectName)
				$this->fieldTypes[$objectName] = $this->db->getFields($objectName);

		// start syncing each object
		foreach($this->config['objects'] as $objectName => $object) {

			echo "Starting sync - $objectName".PHP_EOL;

			$thisSync = date("Y-m-d H:i:s");
			$previousSync = date("Y-m-d\TH:i:s\Z", strtotime($this->db->getMostRecentSync($objectName)));

			$result = $this->sf->batchQuery($objectName, $object['fields'], $previousSync);
			$result->header = str_getcsv(strtolower($result->header), ',', '"');

			$fieldsToConvert = array();
			$fieldsToEscape = array();
			// decide which fields need to be treated before inserting into local db
			foreach($result->header as $k => $field) {
				$convertFunction = $this->getCovertFunction($this->fieldTypes[$objectName][$field]);
				if($convertFunction != null)
					$fieldsToConvert[$k] = $convertFunction;

				if($this->isEscapeRequired($this->fieldTypes[$objectName][$field]))
					$fieldsToEscape[] = $k;
			}

			$fieldCount = count($result->header);
			
			// convert and escape data
			echo "Converting data...";
			foreach($result->data as $k => $rowCsv) {
				// use ascii null to avoid escaping
				$rowArr = str_getcsv($rowCsv, ',', '"', chr(0));

				if(count($rowArr) != $fieldCount)
					throw new Exception("error parsing row $k ($rowCsv)");

				foreach($fieldsToConvert as $key => $func)
					$rowArr[$key] = $this->$func($rowArr[$key]);

				foreach($fieldsToEscape as $key)
					$rowArr[$key] = $this->db->quote($rowArr[$key]);

				$result->data[$k] = implode(',', $rowArr);
			}
			echo "ok".PHP_EOL;

			$this->db->upsertCsvValues($objectName, $result->header, $result->data, $thisSync);
		}

		// revert to original timezone
		date_default_timezone_set($originalTz);
	}

	/**
	 * Convert Salesforce object definitions into database tables and create fields as defined by createFieldDefinition().
	 */
	public function syncSchema() {

		// determine which sObjects need to be queried and determine fields that need adding
		$sObjectsToQuery = array();
		foreach($this->config['objects'] as $objectName => $object) {

			// put field names into an array and lowercase them
			$fields = $object['fields'];
			
			if(!$this->db->tableExists($objectName)) {

				// table doesn't exist, add all fields
				$sObjectsToQuery[$objectName] = $fields;

			} else {

				// table exists, add fields that don't already exist
				$diff = array_diff($fields, array_keys($this->db->getFields($objectName)));

				if(count($diff) > 0)
					$sObjectsToQuery[$objectName] = $diff;

			}

		}

		if(count($sObjectsToQuery) > 0) {

			$objectUpserts = array();

			// get all field data for required sObjects
			$describeResult = $this->sf->getSObjectFields(array_keys($sObjectsToQuery));

			// update tables in local db
			foreach ($describeResult as $sObject => $fields) {

				$sObject = strtolower($sObject);

				// check if any fields defined in config don't exist in Salesforce
				$allFields = array_map(function($field) {return strtolower($field->name);}, $fields);
				$badFields = array_diff($sObjectsToQuery[$sObject], $allFields);
				if(count($badFields) > 0)
					throw new Exception("Invalid field(s): " . implode(', ', $badFields));

				// get describe data for fields that need to be added
				$newFields = array();
				foreach($fields as $field) {
					if(in_array(strtolower($field->name), $sObjectsToQuery[$sObject]))
						$newFields[] = $field;
				}

				// get mysql definition for each field
				$fieldDefs = array();
				foreach($newFields as $field)
					$objectUpserts[$sObject][$field->name] = $this->createFieldDefinition($field);

			}

			// update and create tables
			foreach($objectUpserts as $sObject => $fieldDefs)
				$this->db->upsertObject($sObject, $fieldDefs);

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
		   empty($db['user']) || empty($db['pass']) || empty($db['database']))
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

	private function getCovertFunction($fieldType) {

		if($pos = strrpos($fieldType,'('))
			$fieldType = substr($fieldType, 0, $pos);

		switch($fieldType) {
			case 'datetime':
				return 'convertDatetime';
			case 'bit':
				return 'convertBoolean';
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

	private function convertDatetime($value) {
		return date("Y-m-d H:i:s", strtotime($value));
	}

	private function convertBoolean($value) {
		if($value == 'true')
			return 1;
		else
			return 0;
	}

}

if(!count(debug_backtrace())) { // only run if being called directly

	$r = new Replicator();
	$r->syncData();

}