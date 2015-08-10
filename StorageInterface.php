<?php

/**
 * FORCE.COM REPLICATOR
 * @author Tom Gangemi
 *
 * StorageInterface.php
 * Provides and interface to a local datastore as a replication target.
 */

interface StorageInterface {

	public function quote($value);

	public function beginTransaction($continue=false);

	public function commitTransaction($final=false);

	public function rollbackTransaction();

	public function tableExists($table);

	public function getFields($table);

	public function createObject($table, $fields);

	public function upsertObject($table, $fields);

	public function addFields($table, $fields);

	public function deleteFields($table, $fields);

	public function getMostRecentSync($table);
	
	public function logSyncHistory($table, $syncDatetime=null);

	public function upsertCsvValues($table, $header, $values, $batchSize=null);

	public function upsertArrayValues($table, $header, $values, $syncDatetime, $batchSize=null);

}
