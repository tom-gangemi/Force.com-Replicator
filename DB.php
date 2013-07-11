<?php

/**
 * FORCE.COM REPLICATOR
 * @author Tom Gangemi
 *
 * DB.php
 * Provides and interface to a local datastore as a replication target.
 */

class DB {

	public $batchInsertSize;
	private $db;
	private $transactionDepth;

	const SYNC_HISTORY_TABLE = '_sync_history';

	function __construct($host, $user, $pass, $database, $batchInsertSize=150000) {

		$this->db = new PDO("mysql:host=$host;dbname=$database", $user, $pass);
		$this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION); 

		$this->batchInsertSize = $batchInsertSize;
		$this->transactionDepth = 0;

		if(!$this->isInitialised())
			$this->initialise();
	}

	public function quote($value) {
		return $this->db->quote($value);
	}

	public function beginTransaction($continue=false) {
		if($this->transactionDepth > 0 && !$continue)
			throw new Exception('Transaction already open');
		
		if($this->transactionDepth++ > 0)
			return;

		$this->db->beginTransaction();
		$this->openTransaction = true;
	}

	public function commitTransaction($final=false) {
		if(--$this->transactionDepth > 0 && !$final)
			return;

		$this->transactionDepth = 0;
		$this->db->commit();
		$this->openTransaction = false;
	}

	public function rollbackTransaction() {
		$this->transactionDepth = 0;
		$dbConnect->rollback();
		$this->openTransaction = false;
	}

	public function tableExists($table) {

		return ($this->db->query("SHOW TABLES LIKE '{$table}'")->rowCount() > 0);

	}

	public function getFields($table) {

		$result = $this->db->query("SHOW COLUMNS FROM $table");
		$fields = array();

		foreach($result as $row)
			$fields[$row['Field']] = $row['Type']; 
		
		return $fields;
		
	}	

	public function createObject($table, $fields) {
		
		$this->db->query("CREATE TABLE {$table} (id VARCHAR(18) NOT NULL, PRIMARY KEY(id))");
		$this->addFields($table, $fields);

	}

	public function upsertObject($table, $fields) {

		if($this->tableExists($table)) {
			$this->addFields($table, $fields);
		} else {
			$this->createObject($table, $fields);
		}

	}

	public function addFields($table, $fields) {
	
		$fields = array_change_key_case($fields, CASE_LOWER);	
		unset($fields['id']);
		$fieldDefs = array_map(function($options, $field) {return "ADD COLUMN {$field} {$options}";}, $fields, array_keys($fields));
		$this->db->query("ALTER TABLE {$table} " . implode(', ', $fieldDefs));

	}

	public function deleteFields($table, $fields) {

		$fieldDefs = array_map(function($field) {return "DROP COLUMN {$field}";}, $fields);
		$this->db->query("ALTER TABLE {$table} " . implode(', ', $fieldDefs));
		
	}

	public function getMostRecentSync($table) {

		$result = $this->db->query(
			'SELECT sync_time FROM '. $this::SYNC_HISTORY_TABLE .
			" WHERE object_name='{$table}' ORDER BY sync_time DESC LIMIT 1"
		);

		return $result->fetch(PDO::FETCH_ASSOC)['sync_time'];

	}
	
	public function upsertCsvValues($table, $header, $values, $syncDatetime, $batchSize=null) {

		if($batchSize === null)
			$batchSize = $this->batchInsertSize;

		$fieldNames = '(' . implode(',', $header) . ')';
		$fieldUpdates = array_map(function($field) {return "{$field}=VALUES({$field})";}, $header);
		$fieldUpdates = implode(',', $fieldUpdates);

		$this->beginTransaction();

		$this->db->query(
			'INSERT INTO '. $this::SYNC_HISTORY_TABLE .
			" (object_name, sync_time) VALUES('{$table}', '{$syncDatetime}')"
		);

		foreach(array_chunk($values, $batchSize) as $chunk) {
			// upsertValues batchInsertSize records at a time

			echo "upserting chunk...";

			$values = array();
			foreach($chunk as $k => $record)
				$chunk[$k] = "($record)";

			try {
				$this->db->query(
					"INSERT INTO {$table} {$fieldNames} VALUES" . implode(',', $chunk) . 
					" ON DUPLICATE KEY UPDATE {$fieldUpdates}"
				);
			} catch(Exception $e) {
				file_put_contents('debug.txt', "INSERT INTO {$table} {$fieldNames} VALUES" . implode(',', $chunk) . " ON DUPLICATE KEY UPDATE {$fieldUpdates}");
				throw $e;
			}

			echo 'ok'.PHP_EOL;

		}

		$this->commitTransaction();

	}

	public function upsertArrayValues($table, $header, $values, $syncDatetime, $batchSize=null) {

		if($batchSize === null)
			$batchSize = $this->batchInsertSize;

		$fieldNames = '(' . implode(',', $header) . ')';
		$fieldUpdates = array_map(function($field) {return "{$field}=VALUES({$field})";}, $header);
		$fieldUpdates = implode(',', $fieldUpdates);

		$this->beginTransaction();

		$this->db->query(
			'INSERT INTO '. $this::SYNC_HISTORY_TABLE .
			" (object_name, sync_time) VALUES('{$table}', '{$syncDatetime}')"
		);

		foreach(array_chunk($values, $batchSize) as $chunk) {
			// upsertValues batchInsertSize records at a time

			$values = array();
			foreach($chunk as $record) {
				$recordValues = array_map(function($value) {return "'{$value}'";}, $record);
				$values[] = '(' . implode(',', $recordValues) . ')';
			}

			$this->db->query(
				"INSERT INTO {$table} {$fieldNames} VALUES" . implode(',', $values) . 
				" ON DUPLICATE KEY UPDATE {$fieldUpdates}"			
			);

		}

		$this->commitTransaction();

	}

	private function isInitialised() {

		return $this->tableExists($this::SYNC_HISTORY_TABLE);

	}

	private function initialise() {

		$this->db->query('DROP TABLE IF EXISTS ' . $this::SYNC_HISTORY_TABLE);

		$this->db->query(
			'CREATE TABLE ' . $this::SYNC_HISTORY_TABLE .
			' (id INT NOT NULL AUTO_INCREMENT, object_name VARCHAR(45) NULL, sync_time DATETIME NULL, PRIMARY KEY(id))'
		);

	}	

}
