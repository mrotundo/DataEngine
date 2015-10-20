<?php
namespace Mrotundo\DataEngine\Processes\Importers;
use Carbon\Carbon;

abstract class BigQuery extends Base
{
    protected $fetchComplete;
    protected $jobID;
    
    protected $projectID;
    
    protected $bqTools;
    
    public function __construct()
    {
        //$this->requiredParams[]    = 'mode';
        //$this->requiredParams[]    = 'jobid';
        $this->fetchComplete        = false;
        $this->jobID                = false;
        $this->minWaitInterval      = 0;
        $this->maxWaitInterval      = 5;
        $this->mode                 = ($this->mode!==false)?$this->mode:'query';
        
        \Eng::loadLibrary('BigQuery','bq');
        
        return parent::__construct();
    }

    public function execute()
    {
        if($this->mode=='fetch')
        {
            $this->import();
            $this->process();    
            $this->save();
        }
        else
        {
            $this->setParam('mode','query');
            return $this->importQuery();
        }
    }
    
    public function next()
    {
        //if the query was just ran, call the connector to fetch the data
        if($this->mode=='query')      //&&!$this->fetchComplete
        {
            $this->setParam('mode','fetch');
            \Eng::queueProcess('Importers\\'.$this->name,$this->queueName,$this->determineWaitInterval(true),$this->getParams());
        }
        //if this was called to fetch, but the data was not available or fully retrieved
        elseif($this->mode=='fetch'&&!$this->fetchComplete)
        {
            $this->setParam('mode','fetch');
            \Eng::queueProcess('Importers\\'.$this->name,$this->queueName,$this->determineWaitInterval(true),$this->getParams());
        }
        //if in fetch mode and all data was returned, schedule next call
        elseif($this->mode=='fetch'&&$this->fetchComplete)
        {
            //@TODO: write
        }
        

    }
    
    public function finalize()
        {   return true; }
    
    protected function importQuery()
    {
        $query = $this->buildQuery();
        
        try
        {
            $this->jobID = \Eng::lib('bq')->query($query,$this->projectID);
        }
        catch(\Exception $e)
        {
            $this->logError($e->getMessage());
            return false;
        }
        
        $this->setParam('jobid',$this->jobID);
        
    }
    
    protected function import()
    {
        //check for the Job ID, otherwise fail and complete
        if(!$this->getParam('jobid'))
        {
            $this->logError("BigQuery job ID not defined."); 
            $this->fetchComplete = true;
            return false;
        }
        
        //attempt tp get the page token
        $pageToken = $this->getParam('pagetoken');
        
        //attempt to get the result, if it does not work, fail and complete
        try {
            $result = \Eng::lib('bq')->fetch($this->getParam('jobid'),$this->projectID,$this->maxInputBatchSize,$pageToken);
        }
        catch(\Exception $e)
        {
            $this->logError($e->getMessage());
            $this->fetchComplete = true;
            return false;
        }
        
        
        //if a result was returned and a page token was not provided (which means this is the last of the data)
        if($result&&!$pageToken)
            {$this->fetchComplete = true;}
        //if there is a result and a page token, that means there is more data
        elseif($result)
        {   $this->setParam('pagetoken',$pageToken);
            $this->fetchComplete = false;
        }
        //the data was not ready, retry
        elseif($result===false)
        {
            $this->fetchComplete = false;
            return true;
        }
        //if there is no result data, that means there is no data to process, exit        
        elseif(!sizeof($result))
        {
            $this->fetchComplete = true;
            return true;
        }
        
        return $this->setInput($result);
    }
    
    public function process($input=null)
    {
        $this->setInput($input);
        $processed = $this->getInput();
        //process the records
        
        return $this->setOutput($processed);
    }
    
    public function save($output=null)
    {
        $this->setOutput($output);
        return \Eng::lib('es')->ingest($this->getOutput());
    }
    
    


}