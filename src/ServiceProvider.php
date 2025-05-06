<?php

namespace DreamFactory\Core\Databricks;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Databricks\Database\Connectors\DatabricksConnector;
use DreamFactory\Core\Databricks\Database\Schema\DatabricksSchema;
use DreamFactory\Core\Databricks\Database\DatabricksConnection;
use DreamFactory\Core\Databricks\Config\DatabricksConfig;
use DreamFactory\Core\Databricks\Services\DatabricksService;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Add our database drivers.
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('databricks', function ($config) {
                $connector = new DatabricksConnector();
                $connection = $connector->connect($config);

                return new DatabricksConnection($connection, $config["database"], $config["prefix"], $config);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'                  => 'databricks',
                    'label'                 => 'Databricks',
                    'description'           => 'Database service supporting Databricks connections.',
                    'group'                 => ServiceTypeGroups::DATABASE,
                    'subscription_required' => LicenseLevel::SILVER,
                    'config_handler'        => DatabricksConfig::class,
                    'factory'               => function ($config) {
                        return new DatabricksService($config);
                    },
                ])
            );
        });

        $this->app->resolving('df.db.schema', function ($db) {
            /** @var DatabaseManager $db */
            $db->extend('databricks', function ($connection) {
                return new DatabricksSchema($connection);
            });
        });
    }

    public function boot()
    {
        $this->loadMigrationsFrom(__DIR__ . '/../database/migrations');
    }
}