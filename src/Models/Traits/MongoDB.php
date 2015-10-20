<?php
namespace Mrotundo\DataEngine\Models\Traits;

trait MongoDB
{
    
    public function getMongoDBConfig()
    {
        return ['project_id'=>null,
                'dataset'=>null,
                'table'=>null,
                ];
    }
    
    
    public function getMongoDBSchema()
    {
        return null;
    }
    
    
    public function fetchDataByLastID($id,$addtlQuery,$limit=50)
    {
        
    }
    
    

    protected function setupQueryByDate($fields=null,$startDateTime=false,$endDateTime=false,$addtlQuery=false)
    {
        if(is_null($fields))
            {return [];}

        if(!$startDateTime)
            {$startDateTime = "1900-01-01 00:00:00";}
            
        //@TODO: set default end date time

        $fieldQueries = ['$or'=>[]];

        foreach($fields as $field)
        {
            $fieldQuery = ['$and'=> [
                                    [$field=>['$gte'=>$startDateTime]],
                                    [$field=>['$lt'=>$endDateTime]]
                                    ]];
            $fieldQueries['$or'][] = $fieldQuery;
        }

        
        $andQuery = [
            ['email' =>['$exists'=>true]],
            $fieldQueries
        ];

        $query = [ '$and' => $andQuery];


        return $query;
    }

    protected function setupQueryByLastID($lastID=false)
    {

        $andQuery = [
            ['email' =>['$exists'=>true]],
        ];

        if($lastID)
        {
            $id = new \MongoId($lastID);
            $andQuery[] = ['_id' => ['$gt' => $id]];
        }

        $query = [ '$and' => $andQuery];

        return $query;

    }
    
}