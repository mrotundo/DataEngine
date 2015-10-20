<?php
namespace Mrotundo\DataEngine\Processes\Exporters;

use Mrotundo\DataEngine\Processes\Base as ProcessesBase;

abstract class Base extends ProcessesBase
{
    public abstract function load();
    
    public abstract function process($data=null);
    
    public abstract function export($data=null);
    
    public function execute()
    {
        $this->load();
        $this->process();
        $this->export();
    }

}