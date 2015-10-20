<?php

namespace Mrotundo\DataEngine;

use Illuminate\Support\ServiceProvider;
use Illuminate\Foundation\AliasLoader;

class DataEngineServiceProvider extends ServiceProvider
{
    /**
     * Perform post-registration booting of services.
     *
     * @return void
     */
    public function boot()
    {
        //
    }

    /**
     * Register any package services.
     *
     * @return void
     */
    public function register()
    {
        //f
        $this->app->singleton('Engine', function($app)
        {
            return new Engine();
        });
        
        $this->app->alias('Engine', 'engine');


        // Shortcut so developers don't need to add an Alias in app/config/app.php
        $this->app->booting(function () {
            $loader = AliasLoader::getInstance();
            $loader->alias('Eng', 'Mrotundo\DataEngine\Facades\Eng');
        });
    }
}