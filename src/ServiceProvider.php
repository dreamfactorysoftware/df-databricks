<?php
namespace DreamFactory\Core\Databricks;

use DreamFactory\Core\Databricks\Models\DatabricksConfig;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Databricks\Services\DatabricksService;
use Illuminate\Routing\Router;

use Route;
use Event;


class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function boot()
    {
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }

    public function register()
    {
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'databricks',
                    'label'           => 'DatabricksService Service',
                    'description'     => 'Service for DatabricksService connections.',
                    'group'           => ServiceTypeGroups::REMOTE,
                    'config_handler'  => DatabricksConfig::class,
                    'factory'         => function ($config) {
                        return new DatabricksService($config);
                    },
                ])
            );
        });
    }
}
