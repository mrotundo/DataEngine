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
    
    public abstract function api($data=null,$params=null);
    
    public function executeAPI($data=null,$params=null)
    {
        $this->api($data,$params);
        $input = $this->getInput();
        $this->process($input);
        $output = $this->getOutput();
        $this->save($output);
    }
}