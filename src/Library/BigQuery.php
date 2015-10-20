<?php

namespace Mrotundo\DataEngine\Library;

use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Config;


class BigQuery
{
    protected $client;

    protected $service;

    protected $projectID;

    protected $queryTimeout;

    protected $queryTimeoutIncrement;

    public function __construct()
    {
        /* Get config variables */
        $client_id = Config::get('google.client_id');
        $service_account_name = Config::get('google.service_account_name');
        $key_file_location = base_path() . Config::get('google.key_file_location');
        $application_name = base_path() . Config::get('google.application_name');
        $this->projectID = Config::get('google.default_project_id');
        $this->queryTimeout = Config::get('google.query_timeout');
        $this->queryTimeoutIncrement = Config::get('google.query_timeout_increment');

        $this->client = new \Google_Client();
        $this->client->setApplicationName($application_name);
        $this->service = new \Google_Service_Bigquery($this->client);

        //@TODO: determine how the cache works
        if (Cache::has('service_token')) {
            $this->client->setAccessToken(Cache::get('service_token'));
        }

        $key = file_get_contents($key_file_location);
        
        $scopes = array('https://www.googleapis.com/auth/bigquery');
        $cred = new \Google_Auth_AssertionCredentials(
            $service_account_name,
            $scopes,
            $key
        );

        $this->client->setAssertionCredentials($cred);
        if ($this->client->getAuth()->isAccessTokenExpired()) {
            $this->client->getAuth()->refreshTokenWithAssertion($cred);
        }
        Cache::forever('service_token', $this->client->getAccessToken());
    }

    
        /*
         * 
         * $synchronous - if true, wait and return data, if false, return Job ID
         */

    public function query($query=null,$projectID=null,$synchronous=false)
    {
        if(is_null($query))
            {return false;}

        if(is_null($projectID))
            { $projectID = $this->projectID;}

        $queryConfig = new \Google_Service_Bigquery_JobConfigurationQuery();
        
        $queryConfig->setQuery($query);

        $config = new \Google_Service_Bigquery_JobConfiguration();
        $config->setDryRun(false);
        $config->setQuery($queryConfig);

        $job = new \Google_Service_Bigquery_Job();
        $job->setConfiguration($config);

        try {

            $job = $this->service->jobs->insert($projectID, $job);

            $status = $job->getStatus();

            if ($status->count() != 0) {
                $err_res = $status->getErrorResult();
            }
        } catch (Google_Service_Exception $e) {
            echo $e->getMessage();
            exit;
        }

        $jr = $job->getJobReference();

        $jobID = $jr['jobId'];
        
        //if not synchronous, return the job ID to fetch later
        if(!$synchronous)
            {return $jobID;}

        $jobComplete = false;    
        
        $startTime  = time();
        $timeRan    = 0;
        
        while (!$jobComplete&&($timeRan<($this->queryTimeout))) {

            try{
                $data = $this->fetch($jobID,$projectID);
            }
            catch(\Exception $e)
            {
                return false;
            }
            
            if($data!==false)
                {return $data;}

            sleep($this->queryTimeoutIncrement);
            $timeRan = time() - $startTime; 
        }

        return false;

    }
    
    //throw error if jobID not provided or invalid
    //return false if not complete
    public function fetch($jobID=false,$projectID=false,$batchSize=false,&$pageToken=false)
    {
        if(!$projectID||!$jobID)
            {throw new \Exception("Invalid project or job ID provided.");}
        
        $params = ['timeoutMs' => 1000];
        
        if($batchSize)
            {$params['maxResults']=$batchSize;}
        
        if($pageToken)
            {$params['pageToken']=$pageToken;}
            
        $res = $this->service->jobs->getQueryResults($projectID, $jobID, $params);
           
        if(isset($res->pageToken))
            { $pageToken = $res->pageToken; }
        else
            {$pageToken=false;}
        
        if(!isset($res->jobComplete))
            {throw new \Exception("Invalid BigQuery data returned.");}
        
        if(!$res->jobComplete)
            {return false;}

        $columns = $res->schema->fields;
        $rows = $res->getRows();
        $data = array();
        foreach($rows as $row){
            $cells = $row->getF();
            $row_arr = array();

            foreach($cells as $key => $cell)
            {
                $row_arr[$columns[$key]->name] = $cell->getV();
            }

            $data[] = $row_arr;
        }
        
        return $data;
    }


    
    public function insert($projectID=null,$datasetID=null,$tableID=null,$data=null,$schema=null,$synchronous=true)
    {
        if(is_null($projectID)||is_null($tableID)||is_null($schema)||is_null($data)||is_null($datasetID))
            {return false;}
           
        $data = $this->getFormattedData($data,$schema);
         
        $rows = [];
        foreach($data as $current)
        {
            $row = new \Google_Service_Bigquery_TableDataInsertAllRequestRows;
            $row->setJson($current);
            $rows[] = $row;
        }
         
        $insertAllRequest = new \Google_Service_Bigquery_TableDataInsertAllRequest();
        $insertAllRequest->setRows($rows);
        
        $this->createTable($projectID,$datasetID,$tableID,$schema);
        
        $this->service->tabledata->insertAll($projectID, $datasetID, $tableID, $insertAllRequest);
        
        //@TODO: return false if failure occurs
        
        return true;
    }
    
    
    
    public function getFormattedData($data=array(),$schema=array())
    {
        $outputData = []; 
        
        if(!is_array($data)||!is_array($schema))
            { return $outputData;}
            
        foreach($data as $row)
        {
            $outputRow = $this->getFormattedRow($row,$schema);
            
            if(((array)$outputRow)==[])
                {continue;}
                
            $outputData[] = $outputRow;
        }
        
        return $outputData;
    }
    
    //  format the row for BQ based on the provided model
    //          will exclude any data not defined in the model
    //          currently case insensitive
    public function getFormattedRow($row=array(),$schema=array())
    {
        $outputRow = (object) []; 
        
        if(!is_array($row)||!is_array($schema))
            { return $outputRow;}
         
            
        foreach($schema as $schemaField)
        { 
            $fieldFormat    = $schemaField['type'];
            $fieldName      = $schemaField['name'];
            
            if(!isset($row[$fieldName]))
                {$row[$fieldName] = "";}
            
            switch($fieldFormat)
            {
                case 'int':
                    $outputRow->$fieldName = (int)$row[$fieldName];
                    break;
                case 'float':
                    $outputRow->$fieldName = (float)$row[$fieldName];
                    break;
                //@TODO: handle other field formats
                default:
                    $outputRow->$fieldName = $row[$fieldName];
                    break;
            }
            
        }
        
        return $outputRow;
    }
    
    
    protected function createTable($projectID,$datasetID,$tableID,$schema,$deleteIfExists=false){
        
        $tableExists = $this->tableExists($projectID,$datasetID,$tableID);
        
        if($tableExists&&!$deleteIfExists)
            { return true; }
            
        if($tableExists&&$deleteIfExists)
        {
            $del = $this->deleteTable($projectID,$datasetID,$tableID); 
            if(!$del)
                { return false; }
        }
        
        $table_reference = new \Google_Service_Bigquery_TableReference();
        $table_reference->setProjectId($projectID);
        $table_reference->setDatasetId($datasetID);
        $table_reference->setTableId($tableID);
        $bqSchema = new \Google_Service_Bigquery_TableSchema();
        $bqSchema->setFields($schema);
        $table = new \Google_Service_Bigquery_Table();
        $table->setTableReference($table_reference);
        $table->setSchema($bqSchema);

        try {
            return $this->service->tables->insert($projectID, $datasetID, $table);
        } catch (\Google_Service_Exception $ex) {
            //echo $ex->getMessage();
            return false;
        }
        
    }
    
    //check if a BQ table exists
    protected function tableExists($projectID,$datasetID,$tableID){
        
        try{
            $this->service->tables->get($projectID,$datasetID,$tableID);
        } catch (\Exception $ex) {
            return false;
        }
        
        return true;
    }
    
    protected function deleteTable($projectID,$datasetID,$tableID)
    {
        //@TODO: write...  this is very dangerous, need to have serious safeguards in place here
        return false;
    }


}