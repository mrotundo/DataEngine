<?php
namespace Mrotundo\DataEngine\Processes\Importers;

abstract class MongoDB extends Base
{
    protected $mongo;

    public function __construct()
    {
        $this->mongo = \DB::getMongoClient();

        if(!$this->mongo)
            {return $this->logError("Failed to make mongo connection.");}
            
        return parent::__construct();
    }

}