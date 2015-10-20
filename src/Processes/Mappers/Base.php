<?php
namespace Mrotundo\DataEngine\Processes\Mappers;

use Mrotundo\DataEngine\Processes\Base as ProcessesBase;

abstract class Base extends ProcessesBase
{
    public abstract function load();
    
    public abstract function map($data=null);
    
    public abstract function save($data=null);
    
    public function execute()
    {
        $this->load();
        $this->map();
        $this->save();
    }
}