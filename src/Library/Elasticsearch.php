<?php

namespace Mrotundo\DataEngine\Library;

class Elasticsearch
{
    public function ingest($data,$config=[])
    {
        if(!is_array($data))
            {return false;}
        
        if(!sizeof($data))
            {return false;}
        
        
        //@TODO: validate config
        
        
        if(isset($this->maxOutputBatchSize)&&$this->maxOutputBatchSize)
            {$chunkedData = array_chunk($data,$this->maxOutputBatchSize);}
        else
            {$chunkedData = [$data];}

        foreach($chunkedData as $chunk)
        {
            $ret = $this->bulkIndex($chunk,$config['destination_index'],
                                                $config['default_type'],
                                                $config['id_field'],
                                                isset($config['date_index_segment'])?$config['date_index_segment']:false,
                                                isset($config['date_field'])?$config['date_field']:'_date',
                                                isset($config['schema'])?$config['schema']:null);

            foreach ($ret['items'] as $item) {
                if (isset($item['index']['status']) && $item['index']['status'] != '201' && $item['index']['status'] != '200') {
                    //@TODO: handle error logging for failed items
                    //$this->logError("Object ID: " . $item['index']['_id'] . " failed to index");
                }
            }
        }
    }
    
    
    public function fetch($index=null,$type=null,$limit=null,$offset=null,$query=null,$sort=null,$aggs=null,$returnFullResult=false)
    {
        
        $searchResults = $this->search($index,$type,$limit,$offset,$query,$sort,$aggs);
        
        if($returnFullResult)
        {
            return $searchResults;
        }
        else
        {
            return $this->getDataFromResult($searchResults);
        }
    }
    
    public function fetchSize($index=null,$type=null,$limit=null,$offset=null,$query=null,$sort=null,$aggs=null)
    {
        $searchResults =  $this->elasticsearchFetch($index,$type,$limit,$offset,$query,$sort,$aggs,true);
        
        if(!$searchResults)
            {return false;}
            
        if(!isset($searchResults['hits']['total']))
            {return false;}
        
        return $searchResults['hits']['total'];
        
    }
    protected function getQueryBuilder()
    {
        //return $this->loadLibrary('ESQueryBuilder');
        return new Elasticearch\QueryBuilder();
    }
    
    protected function getAggregationBuilder()
    {
        //return $this->loadLibrary('ESAggregationBuilder');
        return new Elasticearch\AggregationBuilder();
    }
    
    
    
    
    public function index($data=array(),$index='default',$type='default',$idField='_id',$dateIndexSegment=false,$dateField='_date',$schema=null,$bulk=false)
    {
        $params = array();
        $params['body']=array();

        //@TODO: remove this if the index method of ES is used for non-bulk methods
        if(!$bulk)
            {$data = [$data];}
        
        foreach($data as $row) {
                if($schema)
                    {$row = $this->enforceSchema($row,$schema);}

                $obj = array('index' => array(
                    '_index' => $this->determineIndex($row,$index,$dateIndexSegment,$dateField)));

                if(isset($row['_type'])){
                        $obj['index']['_type'] = $row['_type'];
                        unset($row['_type']);
                }
                else {$obj['index']['_type']=$type;}

                if(!is_null($idField)&&isset($row[$idField]))
                                        { $obj['index']['_id'] = $row[$idField];
                                        unset($row[$idField]);
                                        }

                $params['body'][] = $obj;
                $params['body'][] = $row;
        }

        try {
            
            //return;
            $return = \Es::bulk($params);
            //@TODO: use index method for non-bulk requests, also will need to fix the above TODO
            /*
            if($bulk)
                {$return = \Es::bulk($params);}
            else
                {$return = \Es::index();}
             * 
             */
        } catch (Exception $ex) {
            //@TODO: cleanup
                echo "\nFAIL: ";
                echo $ex->getMessage();
                echo "\n\n";
                $return = false;
        }

        //@TODO: set refresh interval back to normal, see TODO at top of method for context
            
        return $return;
    }
    
    

    public function bulkIndex($data=array(),$index='default',$type='default',$idField='_id',$dateIndexSegment=false,$dateField='_date',$schema=null)
    {
        return $this->index($data,$index,$type,$idField,$dateIndexSegment,$dateField,$schema,true);
    }

    public function search($index=null,$type=null,$limit=null,$offset=null,$query=null,$sort=null,$aggs=null)
    {
            $params = array();

            if(!is_null($index))
                    {$params['index'] = $index;}
            if(!is_null($type))
                    {$params['type'] = $type;}
            if(!is_null($offset))
                    {$params['from'] = $offset;}
            if(!is_null($limit))
                    {$params['size'] = $limit;}
            if(!is_null($sort))
                    {$params['sort'] = $sort;}
            if(!is_null($query))
                    {$params['body']['query'] = $query;} 
            if(!is_null($aggs))
                    {$params['body']['aggs'] = $aggs;}        


            try {
                    $results = \Es::search($params);
            }
            catch(\Exception $e)
            {
                    echo $e->getMessage();
                    return array();
            }

            return $results;
    }



    public function fetchByField($fieldName=null,$fieldValue=null,$index=null,$type=null)
    {
            if(is_null($fieldName)||is_null($fieldValue))
                    {return array();}


            $result = $this->search($index,$type,null,null,['filtered'=>['filter'=>['term'=>[$fieldName=>$fieldValue]]]]);

            return $this->getDataFromResult($result);
    }

    public function getFields($index=null,$type=null)
    {
            $params = array();
            $fields = ['_index'=>['type'=>'string'],
                                    '_type'=>['type'=>'string'],
                                    '_id'=>['type'=>'string'],
                                    '_score'=>['type'=>'float']];

            if(!is_null($index))
                    {$params['index'] = $index;}

            if(!is_null($type))
                    {$params['type'] = $type;}

            try {
                    $results = \Es::indices()->getMapping($params);
            }
            catch(\Exception $e)
            {
                    return array();
            }


            foreach($results as $index)
            {
                    if(!isset($index['mappings']))
                            { continue; }

                    foreach($index['mappings'] as $type)
                    {
                            if(!isset($type['properties']))
                             { continue; }

                            $fields = array_merge($fields,$type['properties']);
                    }
            }

            return $fields;
    }

    public function getDataFromResult($result)
    {
            $data = array();

            if(isset($result['hits']['hits']))
            {
                    $data = $result['hits']['hits'];
            }

            if(!is_array($data))
                {return $data;}

            foreach($data as $key => $row)
            {
                    if(!isset($row['_source']))
                            { continue; }
                    unset($data[$key]['_source']);

                    $data[$key] = array_merge($data[$key],$row['_source']);
            }

            return $data;
    }



    public function determineIndex($record=array(),$indexBaseName="",$dateIndexSegment=false,$dateField='_date')
    {

            if(!isset($record[$dateField]))
                            { return $indexBaseName; }

            $dateTime 	= $this->getDateTime($record[$dateField]);
            $indexSuffix 	= $this->generateIndexSuffix($dateTime,$dateIndexSegment);

            return $indexBaseName.$indexSuffix;
    }

    public function getDateIndexes($indexBaseName,$startDate,$endDate,$dateIndexSegment=false)
    {

            $indexes = array();

            if(!$dateIndexSegment)
            {
                            $indexes[] = $indexBaseName;
                            return $indexes;
            }


            $startDateTime 	= $this->getDateTime($startDate);
            $endDateTime 	= $this->getDateTime($endDate);


            $interval = new \DateInterval('P1'.strtoupper($dateIndexSegment[0]));
            $daterange = new \DatePeriod($startDateTime, $interval ,$endDateTime);


            foreach($daterange as $curDate)
            {
                            $indexes[] = $indexBaseName.$this->generateIndexSuffix($curDate,$dateIndexSegment);
            }

                    return $indexes;

    }

    protected function generateIndexSuffix($dateTime,$dateIndexSegment=false)
    {


            switch($dateIndexSegment)
    {
            case 'year':
            $indexSuffix = '-'.$dateTime->format('Y');
            break;

            case 'month':
            $indexSuffix = '-'.$dateTime->format('Y').'.'.$dateTime->format('m');
            break;

            case 'day':
            $indexSuffix = '-'.$dateTime->format('Y').'.'.$dateTime->format('m').'.'.$dateTime->format('d');
            break;

            default:
            $indexSuffix = '';
            break;
    }

    return $indexSuffix;
    }


    protected function getDateTime($originalDateString,$dateFormatted=false)
    {

            $datetime = new \DateTime($originalDateString);

            //@TODO: run conversion on date
            //@TODO: check date format, if not correct throw Exception
                //echo 'date:'.$datetime;

            if($dateFormatted)
                {return $datetime->format($dateFormatted);}
            else
                {return $datetime;}
    }


    public function getIndexNames()
    {
            $mapping = \Es::indices()->getMapping();

            foreach($mapping as $key => $current)
            {
                    echo $key."\r";
            }
    }

    public function getIndexSettings($indexName)
    {
        $params = ['index' => [ $indexName]];

        $data = \Es::indices()->getSettings($params);

        return $data;
    }

    public function getIndexMapping($indexName,$typeName=null)
    {
        $params = ['index' => [ $indexName]];

        if(!is_null($typeName))
        {
            $params['type'] = $typeName;
        }

        $data = \Es::indices()->getMapping($params);

        return $data;
    }



    public function createMapping($indexName,$typeName,$mapping)
    {
        if(!$this->createIndex($indexName))
            {return false;}

        $params = [];
        $params['index'] = $indexName;
        $params['type']  = $typeName;
        $params['body'][$typeName]['_source']['enabled'] = true;
        $params['body'][$typeName]['_source']['dynamic'] = false;

        foreach($mapping as $itemKey => $itemValue)
        {
            $params['body'][$typeName]['_source']['properties'][$itemKey] = $itemValue;
        }

        //echo '<pre>'.print_r($params,true).'</pre>';

        return \Es::indices()->putMapping($params);
    }

    public function createIndex($indexName)
    {
        if(\Es::indices()->exists(['index'=>$indexName]))
            {return true;}

        return \Es::indices()->create(['index'=>$indexName]);
    }

    public function deleteIndex($name)
    {
            //@TODO: write
    }


    protected function enforceSchema($row,$schema=null)
    {
        $outputRow = []; 

        if(!is_array($schema))
            { return $row;}

        if((!is_array($row)))
            {return $outputRow;}

        foreach($schema as $name=> $schemaField)
        {
            $fieldType    = $schemaField['type'];
            $fieldName      = $name;

            if(!isset($row[$fieldName]))
                {$row[$fieldName] = "";}

            switch($fieldType)
            {
                case 'int':
                    $outputRow[$fieldName] = (int)$row[$fieldName];
                    break;
                case 'float':
                    $outputRow[$fieldName] = (float)$row[$fieldName];
                    break;
                case 'date':
                    //@TODO: better handle date format, in the format field.
                    $outputRow[$fieldName] = $this->getDateTime($row[$fieldName],'Y-m-d\TH:i:s');
                    break;

                //@TODO: handle other field formats
                default:
                    $outputRow[$fieldName] = $row[$fieldName];
                    break;
            }

        }

        return $outputRow;
    }


  
}