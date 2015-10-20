<?php
namespace Mrotundo\DataEngine\Processes\Exporters;

abstract class BigQuery extends Base
{
    use \Mrotundo\DataEngine\Processes\Traits\Elasticsearch;
    use \Mrotundo\DataEngine\Processes\Traits\BigQuery;
        
    protected $jobID;
    
    protected $projectID;
    protected $tableName;
    protected $schema;
    
    public function __construct()
    {
        $this->requiredParams[]    = 'mode';
        $this->requiredParams[]    = 'jobid';
        $this->jobID                = false;
        
        $this->loadLibrary('BigQuery','bq');
        $this->setSchema();
        
        return parent::__construct();
    }

    public function execute()
    {
        $this->setSchema();
        
        $this->numRecordsFound = $this->fetchDataSize();
        //@TODO: use input limit if true
        
        $tableName = $this->bqConfig['destination_table'];
        $limit = $this->maxInputBatchSize;
        
        for($offset=0;$offset<$this->numRecordsFound;$offset=($limit+$offset))
        {
            $input = $this->fetchData($limit,$offset);
            $this->setInput($input);
            $processed = $this->processInput();
            $this->setOutput($processed);
            
            //@TODO: handle maxOutputBatchSize
            $output = $this->getOutput();
            $this->bqInsert($tableName,$output,$this->schema);
            
        }
        
        return true;
    }
    
    public function next()
    {
        return true;
    }
    
    public function finalize()
    {
        return true;
    }
    
    protected function setSchema()
    {
        $this->schema = null;
    }
    
    protected function fetchData($limit=null,$offset=null,$namespace=null)
    {
        return [];
    }
    
    

}