<?php
namespace DreamFactory\Core\Databricks;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Databricks\Database\Connectors\DatabricksConnector;
use DreamFactory\Core\Databricks\Database\Schema\DatabricksSchema;
use DreamFactory\Core\Databricks\Models\DatabricksConfig;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Databricks\Services\DatabricksService;
use Illuminate\Routing\Router;
use Illuminate\Database\DatabaseManager;

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
        $this->app->resolving('df.db.schema', function (DbSchemaExtensions $db){
            $db->extend('databricks', function ($connection){
                return new DatabricksSchema($connection);
            });
        });

        $this->app->resolving('db', function (DatabaseManager $db){
            $db->extend('databricks', function ($config){
                $connector = new DatabricksConnector();
                $connection = $connector->connect($config);

                return new DatabricksConnection($connection, $config['database'], '', $config);
            });
        });

        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'            => 'databricks',
                    'label'           => 'DatabricksService Service',
                    'description'     => 'Service for DatabricksService connections.',
                    'group'           => ServiceTypeGroups::DATABASE,
                    'subscription_required' => LicenseLevel::SILVER,
                    'config_handler'  => DatabricksConfig::class,
                    'factory'         => function ($config) {
                        return new DatabricksService($config);
                    },
                ])
            );
        });
    }
}
