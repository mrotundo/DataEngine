<?php
namespace Mrotundo\DataEngine\Processes\Connectors;

use Mrotundo\DataEngine\Processes\Base as ProcessesBase;

abstract class Base extends ProcessesBase
{
    public abstract function import();
    
    public abstract function process($data=null);
    
    public abstract function export($data=null);
    
    public function execute()
    {
        $this->import();
        $this->process();
        $this->export();
    }
}