<?php

namespace Mrotundo\DataEngine\Models\Processes\Traits;

trait BigQuery
{

    protected $bqConfig;
    
    protected function bqInsert($tableName,$data,$schema,$projectID=null,$dataset=null)
    {
        if($projectID==null&&!isset($this->bqConfig['project_id']))
            {return false;}
        elseif($projectID==null)
            {$projectID=$this->bqConfig['project_id'];}
        
        if($dataset==null&&!isset($this->bqConfig['dataset']))
            {return false;}
        elseif($dataset==null)
            {$dataset=$this->bqConfig['dataset'];}
            
        return $this->bq->insert($projectID,$dataset,$tableName,$data,$schema);
    
    }
    
}