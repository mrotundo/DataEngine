<?php
namespace Mrotundo\DataEngine\Processes\Traits;

//@TODO: phase out for direct usage of library

trait Elasticsearch
{
    protected $esConfig;
    protected $esConfigFields = ['destination_index','default_type','id_field','date_index_segment','date_field','schema'];
    
    protected function elasticsearchIngest($data,$overrideConfig=[])
    {
        if(!is_array($data))
            {return false;}
        
        if(!sizeof($data))
            {return false;}
        
        $config = $this->getConfig($overrideConfig);
        
        if(isset($this->maxOutputBatchSize)&&$this->maxOutputBatchSize)
            {$chunkedData = array_chunk($data,$this->maxOutputBatchSize);}
        else
            {$chunkedData = [$data];}

        foreach($chunkedData as $chunk)
        {
            
            $ret = $this->es->bulkIndex($chunk,$config['destination_index'],
                                                $config['default_type'],
                                                $config['id_field'],
                                                $config['date_index_segment'],
                                                $config['date_field'],
                                                $config['schema']);

            foreach ($ret['items'] as $item) {
                if (isset($item['index']['status']) && $item['index']['status'] != '201' && $item['index']['status'] != '200') {
                    $this->logError("Object ID: " . $item['index']['_id'] . " failed to index");
                }
            }
        }
    }
    
    protected function getConfig($config=[])
    {
        $configOut = [];
        
        foreach($this->esConfigFields as $field)
        {
            if(isset($config[$field]))
                {$configOut[$field]=$config[$field];}
            elseif(isset($this->esConfig[$field]))
                {$configOut[$field]=$this->esConfig[$field];}
            else
                {$configOut[$field]=false;}
        }
        
        
        return $configOut;
    }
    
    protected function elasticsearchFetch($index=null,$type=null,$limit=null,$offset=null,$query=null,$sort=null,$aggs=null,$returnFullResult=false)
    {
        
        $searchResults = $this->es->search($index,$type,$limit,$offset,$query,$sort,$aggs);
        
        if($returnFullResult)
        {
            return $searchResults;
        }
        else
        {
            return $this->es->getDataFromResult($searchResults);
        }
    }
    
    protected function elasticsearchFetchSize($index=null,$type=null,$limit=null,$offset=null,$query=null,$sort=null,$aggs=null)
    {
        $searchResults =  $this->elasticsearchFetch($index,$type,$limit,$offset,$query,$sort,$aggs,true);
        
        if(!$searchResults)
            {return false;}
            
        if(!isset($searchResults['hits']['total']))
            {return false;}
        
        return $searchResults['hits']['total'];
        
    }
    protected function elasticsearchGetQueryBuilder()
    {
        return $this->loadLibrary('ESQueryBuilder');
    }
    
    protected function elasticearchGetAggregationBuilder()
    {
        return $this->loadLibrary('ESAggregationBuilder');
    }
}


