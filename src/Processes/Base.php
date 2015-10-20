<?php

namespace Mrotundo\DataEngine\Processes;
use Carbon\Carbon;

abstract class Base
{
    
    public $name;                       //name of process
    public $description;                //description of process
    public $defaultQueueName;           //default queue to run process on (if any)
    
    public $mode;                       //the mode of the current process (not required)
    
    protected $params;                  //parameters provided to process
    protected $requiredParams;          //array of important parameters
    
    protected $maxInputSize;            //maximum number of records to load in total
    protected $maxInputBatchSize;       //maxinum number of records to load in one batch
    protected $maxOutputBatchSize;      //maximum number of records to output/process in one batch
    
    protected $executionTime;           //time of execution in seconds
    protected $numRecordsFound;         //number of records found that matched the initial query
    protected $numRecordsEffected;      //number of records found that were actually effected by this process
    protected $errorCount;              //number of errors
      
    protected $excpectedRecordsFound;
    protected $excpectedRecordsEffected;//number of records that were expected to be effected... will eventually be replaced
                                            // used for determining delay until next run if process re-schedules itself

    protected $minWaitInterval;         //minimum number of minutes to wait if process re-schedules itself
    protected $maxWaitInterval;         //maximum number of minutes to wait if process re-schedules itself

    protected $input;                   //data loaded in for processing
    protected $output;                  //data after processing is completed
    
    protected $models;                  //models loaded for usage


    public abstract function execute();
    
    public abstract function finalize();
    
    public abstract function next();

    public function __construct()
    {   
        //@TODO: set defaults
        $this->errorCount = 0;
        $this->input    = [];
        $this->output   = [];
    }

    public function run($params=array())
    {
        
        $startTime = microtime(true);

        $this->numRecordsFound     =0;
        $this->numRecordsEffected  =0;
        
        if($this->setParams($params)===false)
        {
            $this->executionTime = round((microtime(true) - $startTime),3);
            $this->log();
            return;
        }
        
        if(!$this->getGlobalSettingsValue('disabled'))
        {
            $this->execute();
            $this->finalize();
            $this->numRecordsFound    = $this->numRecordsFound?$this->numRecordsFound:sizeof($this->input);
            $this->numRecordsEffected = $this->numRecordsEffected?$this->numRecordsEffected:sizeof($this->output);
            $this->next();
        }
        else
        {
            $this->logError('Process Execution Disabled');
        }

        $this->executionTime = round((microtime(true) - $startTime),3);
        $this->log();
        return;

    }
    
    public function setParams($params=array())
    {
        if(!is_array($params))
            {return $this->logError('Invalid parameters provided');}
        
        if(!$this->validateParams($params))
            { return $this->logError('Invalid parameters provided');}
        
        foreach($params as $key=>$value)
            {$this->setParam($key,$value);}
            
        return $this->params;
    }
    
    public function getParam($key=null)
    {
        if(!is_array($this->params))
            {return false;}
        
        if(is_null($key))
            {return $this->params;}
            
        if(!isset($this->params[$key]))
            {return false;}
            
        return $this->params[$key];
    }
    
    public function getParams()
    {   return $this->getParam(); }
    
    public function setParam($key=null,$value=null)
    {
        if(is_null($key))
            { return $this->setParams($value);}
        
        if($key=='mode')
            { $this->mode = $value; }
            
       return $this->params[$key] = $value;
    }
    
    protected function validateParams($params=array(),$valid=true)
    {
        if(!isset($this->requiredParams))
            {return $valid;}
        
        foreach($this->requiredParams as $requiredParam)
        {
            if(!isset($params[$requiredParam]))
            {
                $this->logError('Required parameter ['.$requiredParam.'] not provided.');
                $valid = false;
            }
        }
        return $valid;
    }

    //@TODO: settings functions logic is moved to singleton and all processes should be updated

    public function getGlobalSettingsValue($key=null)
    {
        return $this->getSettingsValue($key,true);
    }

    public function setGlobalSettingsValue($key,$value)
    {
        return $this->setSettingsValue($key,$value,true);
    }

    public function getSettingsValue($key=null,$global=false)
    {
        if(is_null($key)) { return false; }

        $whereAndArray = array();

        if(is_array($key))
        {
            foreach($key as $curKey => $curValue)
            {
                $whereAndArray[] = [$curKey =>$curValue];
            }
        }
        else
        {
            $whereAndArray[] = ['key' =>$key];
        }

        if(!$global)
            {$whereAndArray[] = ['process'=>get_class($this)];}

        $data = \DB::collection('process_settings')->where([ '$and' => $whereAndArray])->first();
        
        if(is_array($data)&&isset($data['value']))
            { return $data['value'];}
        
        return false;
    }

    public function setSettingsValue($key,$value,$global=false)
    {
        if(is_null($key)) { return false; }

        $whereAndArray = array();

        if(is_array($key))
        {
            foreach($key as $curKey => $curValue)
            {
                $whereAndArray[] = [$curKey =>$curValue];
            }
        }
        else
        {
            $whereAndArray[] = ['key' =>$key];
        }

        if(!$global)
            {$whereAndArray[] = ['process'=>get_class($this)];}

        $updateArray = array();
        if(!$global)
            {$updateArray['process']  = get_class($this);}
        $updateArray['value']       = $value;

        if(is_array($key))
        {
            foreach($key as $curKey => $curValue)
            {
                $updateArray[$curKey] = $curValue;
            }
        }
        else
        {
            $updateArray['key'] = $key;
        }

        return \DB::collection('process_settings')->where(['$and' => $whereAndArray])->update($updateArray, array('upsert' => true));
            
    }

    public function delSettingsValue($key,$global=false)
    {

        //@TODO: write delete method
        
    }

    protected function logError($message="",$details="")
    {
        \Log::error('Process Error ['.get_class($this).']: '.$message);
        \DB::collection('process_error_log')->insert(array('message'=>$message,'details'=>$details,'process'=>get_class($this),'occurred_at'=>new \MongoDate()));

        $this->errorCount++;
        
        return false; //always returns false
    }

    protected function logMessage($message="",$outputToBuffer=false)
    {
        $string = 'Info ['.get_class($this).']: '.$message;
        \Log::info('Process '.$string);
        if($outputToBuffer)
            {echo $string."\n";}
    }
    
    protected function log()
    {

        \DB::collection('process_log')->insert(array('exectime'=>$this->executionTime,'records'=>$this->numRecordsEffected,'errors'=>$this->errorCount,'process'=>get_class($this),'executed_at'=>new \MongoDate()));

        $string = '';
        //$string = "Process [".get_class($this)."] | Found [".$this->numRecordsFound."] | Effected [".$this->getRecordsEffected()."] | Exec Time [".$this->getExecutionTime()."] | Errors [".$this->errorCount."]";
        
        
        if($this->mode) 
             {$string .=" (mode : ".$this->mode.") | ";}
            
        if($this->getRecordsFound())
            {$string .=" Found [".$this->getRecordsFound()."] | ";}
        
        if($this->getRecordsEffected())
            {$string .=" Effected [".$this->getRecordsEffected()."] | ";}    
        
        if($this->getErrorCount())
            {$string .=" Error Count [".$this->getErrorCount()."] | ";} 
            
        if($this->getExecutionTime())
            {$string .=" Exec Time [".$this->getExecutionTime()."s] | ";}        
        
        if($string=="")
            {$string = 'ran';}
            
        \Log::info($string);
        echo $string."\n";
    }

    
    
    
    
    
    
    public function getErrorCount()
    {
        return $this->errorCount;
    }
    
    public function getRecordsFound()
    {
        return $this->numRecordsFound;
    }
    
    public function getRecordsEffected()
    {
        return $this->numRecordsEffected;
    }

    public function getExecutionTime()
    {
        return $this->executionTime;
    }
    
    protected function determineWaitInterval($returnDate=false)
    {
        if(!$this->numRecordsFound)
        { return $this->maxWaitInterval;}

        if($this->numRecordsFound>=$this->inputLimit)
        {return $this->minWaitInterval;}

        $proportionateWaitInterval = ($this->maxWaitInterval*(($this->inputLimit-$this->numRecordsFound)/$this->inputLimit));
        
        $waitInterval = ($proportionateWaitInterval<$this->minWaitInterval)?$this->minWaitInterval:$proportionateWaitInterval;
        
        if($returnDate)
        {
            return Carbon::now()->addMinutes($waitInterval);
        }
        
        return $waitInterval;
    }
    
    //@TODO: potentially remove
    protected function processInput($input=null,$clearOutput=true)
    {
        if(!is_null($input))
            {$input = $this->input;}
        if(is_null($input))
            {return false;}
            
        if($clearOutput)
            { $this->clearOutput(); }
            
        $output = [];    
        foreach($input as $key => $record)
        {
            $output[$key] = $this->processInputRecord($record,$key);
        }
        
        return $output;
    }
    
    //@TODO: potentially remove
    protected function processInputRecord($record=array(),$key=null)
    {
        if(!$this->validateRecord($record))
            { return false;}
           
        //$this->output[] = $record;
        
        return $record;
    }
    
    //@TODO: potentially remove
    protected function validateRecord($output=array())
    {
        if(!is_array($output)) {return false;}

        return true;
    }
    
    protected function determineLastOutputRecordField($fieldName=null,$namespace="_default")
    {
        $output = $this->getOutput($namespace);
        
        if(!is_array($output))
            {return null;}
        
        if(is_null($fieldName))
            {return $output[sizeof($output) - 1];}
            
        if(!isset($output[sizeof($output) - 1][$fieldName]))
            {return null;}
            
       return $output[sizeof($output) - 1][$fieldName];
    }
    
    
    ///////// GET + SET + append INPUT AND OUTPUT
    
    public function setOutput($data=null,$namespace=null,$append=false)
    {
        if(is_null($data))
            {return false;}
            
        if(is_null($namespace))
            {$namespace='_default';}
        
        $this->incrementNumRecordsEffected(sizeof($data));    
            
        if(!$append)
            { $this->output[$namespace] = $data;}
        else
            { $this->output[$namespace][] = $data; }
    }
    
    public function appendOutput($data=null,$namespace=null)
    {
        return $this->setOutput($data,$namespace,true);
    }
    
    public function setInput($data=null,$namespace=null,$append=false)
    {
        if(is_null($data))
            {return false;}
            
        if(is_null($namespace))
            {$namespace='_default';}
        
        $this->incrementNumRecordsFound(sizeof($data));    
            
        if(!$append)
            { return $this->input[$namespace] = $data;}
        else
            { return $this->input[$namespace][] = $data; }
    }
    
    public function appendInput($data=[],$namespace=null)
    {
        return $this->appendInput($data,$namespace,true);
    }
    
    public function getOutput($namespace=null)
    {
        if(is_null($namespace))
            {$namespace='_default';}
            
        if(is_array($this->output)&&isset($this->output[$namespace]))
            { return $this->output[$namespace];}
        
        return false;
    }
    
    public function getInput($namespace=null)
    {
        if(is_null($namespace))
            {$namespace='_default';}
            
        if(is_array($this->input)&&isset($this->input[$namespace]))
            { return $this->input[$namespace];}
        
        return false;
    }
    
    public function clearOutput($namespace=null)
    {
        return $this->setOutput(null,$namespace);
    }
    
    public function clearInput($namespace=null)
    {
        return $this->setInput(null,$namespace);
    }
    
    
    
    
    
    
    protected function incrementNumRecordsFound($value=0)
    {
        $this->numRecordsFound += (int)$value;
    }
    
    protected function incrementNumRecordsEffected($value=0)
    {
        $this->numRecordsEffected += (int)$value;
    }
}