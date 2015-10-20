<?php
namespace Mrotundo\DataEngine\Models\Traits;

trait BigQuery
{
    protected $bq;
    
    public function getBigQueryConfig()
    {
        return ['project_id'=>null,
                'dataset'=>null,
                'table'=>null,
                ];
    }
    
    
    public function getBigQuerySchema()
    {
        return null;
    }
    
    protected function export($key=null)
    {
        //@TODO: export to big query
    }
    
}