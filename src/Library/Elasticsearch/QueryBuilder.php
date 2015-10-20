<?php

namespace Mrotundo\DataEngine\Library\Elasticsearch;

class QueryBuilder
{
    protected $queryObj;
    protected $filters;
    
    public function __construct()
    {
        $this->queryObj = ['filtered'=>['filter'=>[]]];
        $this->filters = [];
    }
    
    public function getQueryObject()
    {
        //@TODO: any cleanup
        if(sizeof($this->filters)>1)
        {
            $this->setFilters($this->filters,'and');
        }
        elseif(sizeof($this->filters)==1)
        {
            $this->setFilters($this->filters[0]);
        }
        
        
        return $this->queryObj;
    }
    
    public function rangeFilter($field,$firstValue,$secondValue,$firstOperator='gte',$secondOperator='lte')
    {
        $obj = ['range'=>['timestamp'=>[$firstOperator=>$firstValue,$secondOperator=>$secondValue]]];
        
        $this->filters[] = $obj;
    }
    
    public function termFilter($field,$value)
    {
        $obj = ['term'=>[$field=>$value]];
        
        $this->filters[] = $obj;
    }
    
    public function fieldExistsFilter($fieldName)
    {
        $obj = ['exists'=>['field'=>$fieldName]];
                
        $this->filters[] = $obj;
    }
    
    
    public function setFilters($obj,$key=null)
    {
        if(is_null($key))
            {$this->queryObj['filtered']['filter'] = $obj;}
        else
        {$this->queryObj['filtered']['filter'][$key]= $obj;}
    }
    
}