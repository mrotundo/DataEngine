<?php
namespace Mrotundo\DataEngine\Models\Traits;

trait Elasticsearch
{
    protected $es;
    
    public function getElasticsearchConfig()
    {
        return ['destination_index'=>null,
                'default_type'=>null,
                'id_field'=>null,
                'date_index_segment'=>null,
                'date_field'=>null
                ];
    }
    
    public function getElasticsearchSchema()
    {
        return null;
    }
    
    
    //ingest, but lookup based on fields
    public function upsert($data=[],$indexFields=[],$strength=0)
    {
        if(!sizeof($data))
            {return false;}
        
        $indexFields = $this->processUpsertIndexes($indexFields);    
       
        $query = $this->upsertQuery($data,$indexes);
        
        //@TODO: lookup data
        //@TODO: compare strength
        //@TODO: update only if strength >= current strength
        
        //if(!sizeof())    
    }
    
    
    
    //upserts one record based on the fields provided
    protected function upsertQuery($data=[],$indexes=[])
    {
        //@TODO: write elasticsearch query
    }
    
    protected function processUpsertIndexes($indexFields)
    {
        if(sizeof($indexFields))
        {
            //@TODO: validate indexes are valid
           return $indexFields; 
        }
        elseif(sizeof($this->defaultIndexFields))
        {
            return $this->defaultIndexFields;
        }
        
        return false;
    }
    
    public function ingest($record)
    {
        
    }
    
    public function ingestBulk($records)
    {
        
    }
    
}