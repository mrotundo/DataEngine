<?php

namespace Mrotundo\DataEngine;

use Carbon\Carbon as Carbon;
use Illuminate\Support\Facades\Redis as Redis;


class Engine 
{
    protected $libraries;
    protected $models;
    protected $processes;
    
    protected $redis;
    
    protected $settingsKeyPrefix = 'dataengset_';
    
    public function __construct()
    {
        $this->libraries    = [];
        $this->models       = [];
        $this->processes    = [];
        
        $this->redis = Redis::connection();
        
        $this->loadLibrary('Elasticsearch','es');
    }
    
    public function test()
    {
        echo 'yea';
    }
    
    
    ////////// SETTINGS VALUE ACCESS
    
    public function getGlobalSetting($key=null)
    {
        return $this->getSetting($key,true);
    }

    public function setGlobalSetting($key,$value)
    {
        return $this->setSettingsValue($key,$value,true);
    }

    public function getSetting($key=null,$namespace=false)
    {
        if(is_null($key)) { return false; }
        
        if($namespace)
            {$key = $namespace.'::'.$key; }
        
        try
        {
            $result = $this->redis->get($this->settingsKeyPrefix.$key);   
        }
        catch(\Exception $e)
        {
            return false;
        }   
        return json_decode($result,true);
        
    }

    public function setSetting($key=null,$value=null,$namespace=false)
    {
        if(is_null($key)) { return false; }
        
        if($namespace)
            {$key = $namespace.'::'.$key; }
        
        if(is_null($value))
            {return $this->delSetting($key,$namespace);}
            
        try
        {
            $result = $this->redis->set($this->settingsKeyPrefix.$key,json_encode($value));   
        }
        catch(\Exception $e)
        {
            return false;
        }   
        return $result;
        
    }

    public function delSetting($key,$namespace=false)
    {
        
        //@TODO: write delete method
        
    }

    
    
    //////////////  LOGGING
    
    
    public function logError($message="",$params=[])
    {
        return $this->log($message,$params,'error');
    }

    public function logMessage($message="",$params=[])
    {
        return $this->log($message,$params,'message');
    }
    
    public function log($message="",$params=[],$type='message')
    {
        echo 'LOG:'.$message;
        
        //@TODO: determine based on settings any other logging methods
    }
    
    
    
    /////////////   LOGIC LOADING
    
    //@TODO: fold up model and loadModel so that only model exists
    
    public function model($modelName=null)
    {
        if(is_null($modelName))
            {throw new \Exception('Invalid model provided [null].');}
            
        if(isset($this->models[$modelName]))
            {return $this->models[$modelName];}
        
        return $this->loadModel($modelName);
            
    }
    
    public function loadModel($modelName=null,$alias=null)
    {
        if(is_null($modelName))
            {throw new \Exception('Invalid model provided [null].');}
        
        $modelClassApp =  '\App\Models\\'.$modelName;
        
        if(class_exists($modelClassApp))
        {
           if(!is_null($alias))
            {return $this->models[$alias]= new $modelClassApp;}
        
            return $this->models[$modelName]= new $modelClassApp;
        }
        
        $modelClassPackage =  '\Mrotundo\DataEngine\Models\\'.$modelName;
        
        if(class_exists($modelClassPackage))
        {
           if(!is_null($alias))
            {return $this->models[$alias]= new $modelClassPackage;}
        
            return $this->models[$modelName]= new $modelClassPackage;
        }
        
         throw new \Exception('Model not found ['.$modelName.']');
    }
    
    //@TODO: fold up library and loadLibrary so that only model exists
    
    public function library($libraryName=null)
    {
        if(is_null($libraryName))
            {throw new \Exception('Invalid library provided [null].');}
            
        if(isset($this->libraries[$libraryName]))
            {return $this->libraries[$libraryName];}
        
        return $this->loadLibrary($libraryName);
            
    }
    
    public function loadLibrary($libraryName=null,$alias=null)
    {
        if(is_null($libraryName))
            {throw new \Exception('Invalid library provided [null].');}
        
        $libraryClassApp =  '\App\Library\\'.$libraryName;
        
        if(class_exists($libraryClassApp))
        {
           if(!is_null($alias))
            {return $this->libraries[$alias]= new $libraryClassApp;}
        
            return $this->libraries[$libraryName]= new $libraryClassApp;
        }
        
        $libraryClassPackage =  '\Mrotundo\DataEngine\Library\\'.$libraryName;
        
        if(class_exists($libraryClassPackage))
        {
           if(!is_null($alias))
            {return $this->libraries[$alias]= new $libraryClassPackage;}
        
            return $this->libraries[$libraryName]= new $libraryClassPackage;
        }
        
         throw new \Exception('Library not found ['.$libraryName.']');
    }
    
    public function process($processName=null)
    {
        if(is_null($processName))
            {throw new \Exception('Invalid process provided [null].');}
            
        if(isset($this->processes[$processName]))
            {return $this->processes[$processName];}
        
        return $this->loadProcess($processName);
            
    }
    
    public function loadProcess($processName=null,$alias=null)
    {
        if(is_null($processName))
            {throw new \Exception('Invalid process provided [null].');}
        
        $processClassApp =  '\App\Processes\\'.$processName;
        
        if(class_exists($processClassApp))
        {
           if(!is_null($alias))
            {return $this->processes[$alias]= new $processClassApp;}
        
            return $this->processes[$processName]= new $processClassApp;
        }
        
        $processClassPackage =  '\Mrotundo\DataEngine\Processes\\'.$processName;
        
        if(class_exists($processClassPackage))
        {
           if(!is_null($alias))
            {return $this->libraries[$alias]= new $processClassPackage;}
        
            return $this->libraries[$processName]= new $processClassPackage;
        }
        
         throw new \Exception('Process not found ['.$processName.']');
    }
    
    
    public function queueProcess($processLabel,$queue='normal',$date=null,$params=null)
    {
        
        $process = new \App\Commands\QueuedProcess($processLabel);
        
        if(!is_null($date))
        {
            \Queue::laterOn($this->queue,$date, $process, $params);
        }
        else
        {
            \Queue::pushOn($this->queue, $process, $params);
        }
    }
    
    public function runProcess($processLabel,$params,$executeOnly=false)
    {
        /*
        if($executeOnly)
        {
            $this-> execute();
        }
        else
        {
            run();
        }
         * 
         */
        //@TODO: write
        
    }
    
    
    public function proc($processName=null)
        {return $this->process($processName);}
    
    public function lib($libraryName=null)
        {return $this->library($libraryName);}
    
    public function mdl($modelName=null)
        {return $this->model($modelName);}
    
}