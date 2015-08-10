<?php

/**
 * FORCE.COM REPLICATOR
 * @author Tom Gangemi
 *
 * Salesforce.php
 * Provides an interface to gather data from the Force.com platform.
 */

require_once('soapclient/SforcePartnerClient.php');
require_once('soapclient/SforceHeaderOptions.php');
require_once('bulkclient/BulkApiClient.php');

class Salesforce {
	
	public $sfdc;
	public $session;
	private $user;
	private $pass;

	const WSDL = 'soapclient/partner.wsdl.xml';

	function __construct($user, $pass=null) {

		$this->user = $user;
		$this->pass = $pass;
		$this->sfdc = null;
		$this->session = null;

	}

	public function getSObjectFields($objectNames) {

		if(!is_array($objectNames))
			$objectNames = array($objectNames);

		try {

			if($this->sfdc === null)
				$this->login();

			$result = $this->sfdc->describeSObjects($objectNames);

			if(!is_array($result))
				$result = array($result);

			$sObjects = array();
			foreach($result as $sObject)
				$sObjects[$sObject->name] = $sObject->fields;
			
			return $sObjects;
		} catch (Exception $e) {
			echo $this->sfdc->getLastRequest();
			echo $e->faultstring;
			die;
		}

	}

	public function batchQuery($object, $fields, $startDate=null, $fh=null) {

		$this->initSession();

		$myBulkApiConnection = new BulkApiClient($this->session->serverUrl, $this->session->sessionId);
		$myBulkApiConnection->setLoggingEnabled(false);
		$myBulkApiConnection->setCompressionEnabled(true);

		// create in-memory representation of the job
		$job = new JobInfo();
		$job->setObject($object);
		$job->setOpertion('query');
		$job->setContentType('CSV');
		$job->setConcurrencyMode('Parallel');

		$soql = "SELECT " . implode(',', $fields) . " FROM $object";
		if($startDate != null)
			$soql .= " WHERE LastModifiedDate >= $startDate";

		echo 'Creating job...';
		$job = $myBulkApiConnection->createJob($job);
		echo 'ok'.PHP_EOL;		

		echo 'Creating batch...';
		$batch = $myBulkApiConnection->createBatch($job, $soql);
		echo 'ok'.PHP_EOL;
		
		echo 'Closing job...';
		$myBulkApiConnection->updateJobState($job->getId(), 'Closed');
		echo 'ok'.PHP_EOL;

		$sleepTime = 4;
		echo 'Waiting for job to complete...';
		while($batch->getState() == 'Queued' || $batch->getState() == 'InProgress') {
			// poll Salesforce for the status of the batch
		    sleep($sleepTime *= 1.1);
		    echo ".";
		    $batch = $myBulkApiConnection->getBatchInfo($job->getId(), $batch->getId());    
		}
		echo 'ok'.PHP_EOL;

		// get status of batches
		echo "Retrieving results...";
		$resultList = $myBulkApiConnection->getBatchResultList($job->getId(), $batch->getId());

		// retrieve queried data
		foreach ($resultList as $resultId)
		    $myBulkApiConnection->getBatchResult($job->getId(), $batch->getId(), $resultId, $fh);

		echo 'ok'.PHP_EOL;

		if(isset($fh)) {
			
			$preview = stream_get_contents($fh,32,0);
			rewind($fh);

			if(strcasecmp($preview, 'Records not found for this query') == 0 || trim($preview) == false)
				// return false if no records returned
				return false;
			else
				return true;
		}
	}

	private function login() {

		$this->session = null;
		$interactive = $this->pass === null;

		do {
			try {
				$this->sfdc = new SforcePartnerClient();
				$this->sfdc->createConnection($this::WSDL);
				if($this->pass === null)
					$pass = $this::prompt("Password:");
				else
					$pass = $this->pass;

				$this->session = $this->sfdc->login($this->user, $pass);

			} catch (Exception $e) {
				echo $e->getMessage() . PHP_EOL;
			}
		} while($interactive && $this->session === null);

	}

	private function initSession() {

		if($this->session === null)
			$this->login();

	}	

	private static function prompt($prompt = "Enter Password:") {
		// read password from CLI

		if (preg_match('/^win/i', PHP_OS)) {     // windows
			
			$password = false;
			while($password == false)
				$password = readline($prompt);

			return $password;

		} else {                                 // *nix			

			$command = "/usr/bin/env bash -c 'echo OK'";
			if (rtrim(shell_exec($command)) !== 'OK') {
				trigger_error("Can't invoke bash");
				return;
			}
			$command = "/usr/bin/env bash -c 'read -s -p \""
			           . addslashes($prompt)
			           . "\" mypassword && echo \$mypassword'";
			$password = rtrim(shell_exec($command));
			echo PHP_EOL;
			return $password;

		}
	}
}