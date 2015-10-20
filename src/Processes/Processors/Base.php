<?php
namespace Mrotundo\DataEngine\Processes\Processors;

use Mrotundo\DataEngine\Processes\Base as ProcessesBase;

abstract class Base extends ProcessesBase
{
    public abstract function load();
    
    public abstract function process($data=null);
    
    public abstract function save($data=null);
    
    public function execute()
    {
        $this->load();
        $this->process();
        $this->save();
    }
}