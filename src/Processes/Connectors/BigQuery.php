<?php
namespace Mrotundo\DataEngine\Processes\Connectors;
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
        $this->bqTools              = new \App\Library\BQTools();
        
        $this->mode                 = ($this->mode!==false)?$this->mode:'query';
        
        return parent::__construct();
    }

    public function execute()
    {
        if($this->mode=='fetch')
        {
            return $this->executeFetch();
        }
        else
        {
            $this->setParam('mode','query');
            return $this->executeQuery();
        }
    }
    
    public function next()
    {
        //if the query was just ran, call the connector to fetch the data
        if($this->mode=='query')      //&&!$this->fetchComplete
        {
            $waitInterval = 1;
            $date = Carbon::now()->addMinutes($waitInterval);
            $this->setParam('mode','fetch');
            \Queue::laterOn($this->queueName,$date, new \App\Commands\QueuedConnection($this->name,$this->params));
        }
        //if this was called to fetch, but the data was not available or fully retrieved
        elseif($this->mode=='fetch'&&!$this->fetchComplete)
        {
            $waitInterval = 0;
            $date = Carbon::now()->addMinutes($waitInterval);
            $this->setParam('mode','fetch');
            \Queue::laterOn($this->queueName,$date, new \App\Commands\QueuedConnection($this->name,$this->params));
            
        }
        //if in fetch mode and all data was returned, schedule next call
        elseif($this->mode=='fetch'&&$this->fetchComplete)
        {
            
        }
        

    }
    
    public function finalize()
        {   return true; }
    
    protected function executeQuery()
    {
        $query = $this->buildQuery();
        
        try
        {
            $this->jobID = $this->bqTools->query($query,$this->projectID);
        }
        catch(\Exception $e)
        {
            $this->logError($e->getMessage());
            return false;
        }
        
        $this->setParam('jobid',$this->jobID);
        
    }
    
    protected function executeFetch()
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
            $result = $this->bqTools->fetch($this->getParam('jobid'),$this->projectID,$this->maxInputBatchSize,$pageToken);
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
        
        //process the records
        $processed = $this->processInput($result);    
        
        $this->
        
        
        $this->elasticsearchIngest($processed);
        
        return true;
    }
    
    
    
    


}