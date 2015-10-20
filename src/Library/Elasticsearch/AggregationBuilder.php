<?php

namespace Mrotundo\DataEngine\Library\Elasticsearch;

class AggregationBuilder
{
    protected $aggObj;
    
    public function __construct()
    {
        $this->aggObj = ['filtered'=>['filter'=>'']];
        //$aggs = ['actions_over_time'=>['date_histogram'=>['field'=>'date','interval'=>'5h']]];
    }
    
    public function getAggregationObject()
    {
        $queryObj = $this->aggObj;
        
        //@TODO: any cleanup
        
        return $queryObj;
    }
    
    public function setRangeFilter($field,$firstValue,$secondValue,$firstOperator='gte',$secondOperator='lte')
    {
        $obj = ['range'=>['timestamp'=>[$firstOperator=>$firstValue,$secondOperator=>$secondValue]]];
        
        $this->setFilter($obj);
    }
    
    
    public function setFilter($obj)
    {
        $this->aggObj['filtered']['filter']= $obj;
    }
    
}