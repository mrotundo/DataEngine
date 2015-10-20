<?php
namespace Mrotundo\DataEngine\Processes\Importers;

use Mrotundo\DataEngine\Processes\Base as ProcessesBase;

abstract class Base extends ProcessesBase
{
    public abstract function import();
    
    public abstract function process($data=null);
    
    public abstract function save($data=null);
    
    public function execute()
    {
        $this->import();
        $input = $this->getInput();
        $this->process($input);
        $output = $this->getOutput();
        $this->save($output);
    }
}