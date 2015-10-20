<?php
namespace Mrotundo\DataEngine\Models;

class Base
{
    use Traits\Bigquery, Traits\Elasticsearch;
    
    
    public function __construct()
    {
        
    }
    
    
    
}

