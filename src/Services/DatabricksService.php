<?php namespace DreamFactory\Core\Databricks\Services;

use DreamFactory\Core\Databricks\Components\DatabricksComponent;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Models\Service;
use DreamFactory\Core\Databricks\Models\DatabricksConfig;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use Illuminate\Support\Arr;
use Log;
use DreamFactory\Core\Databricks\Database\Connectors\DatabricksConnector;
use DreamFactory\Core\Databricks\Database\Schema\DatabricksSchema;
use DreamFactory\Core\Databricks\Database\Query\DatabricksQueryBuilder;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Database\DbSchemaExtensions;
use Illuminate\Support\ServiceProvider as BaseServiceProvider;
use Illuminate\Database\DatabaseManager;

/**
 * Class DatabricksService
 *
 * @package DreamFactory\Core\Databricks\Services
 */
class DatabricksService extends SqlDb
{
    protected $driverName = 'databricks';

    public static function adaptConfig(array &$config)
    {
        \Log::debug('Adapting Databricks config', ['config' => $config]);
        
        // Set default driver path if not provided
        if (!isset($config['options']['driver_path'])) {
            $config['options']['driver_path'] = env('DATABRICKS_ODBC_DRIVER_PATH', '/opt/simba/spark/lib/64/libsparkodbc_sb64.so');
        }
        
        // Set the driver name
        $config['driver'] = 'databricks';
        
        // Move token to options if it's at the root level
        if (!empty($config['token']) && empty($config['options']['token'])) {
            $config['options']['token'] = $config['token'];
            unset($config['token']);
        }
        
        // Move http_path to options if it's at the root level
        if (!empty($config['http_path']) && empty($config['options']['http_path'])) {
            $config['options']['http_path'] = $config['http_path'];
            unset($config['http_path']);
        }
        
        // Validate required fields
        if (empty($config['options']['token'])) {
            \Log::error('=== DATABRICKS CONFIG ERROR ===', [
                'error' => 'Token is missing in configuration',
                'config' => array_merge($config, ['options' => ['token' => '***']])
            ]);
            throw new \Exception('Databricks authentication token is required');
        }
        
        if (empty($config['host'])) {
            \Log::error('=== DATABRICKS CONFIG ERROR ===', [
                'error' => 'Host is missing in configuration',
                'config' => $config
            ]);
            throw new \Exception('Databricks host is required');
        }
        
        if (empty($config['options']['http_path'])) {
            \Log::error('=== DATABRICKS CONFIG ERROR ===', [
                'error' => 'HTTP path is missing in configuration',
                'config' => $config
            ]);
            throw new \Exception('Databricks HTTP path is required');
        }
        
        \Log::debug('Adapted Databricks config', [
            'host' => $config['host'],
            'http_path' => $config['options']['http_path'],
            'token' => isset($config['options']['token']) ? 'set' : 'not set'
        ]);
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();
        
        // Only keep the _table endpoint with GET method
        $paths = [];
        foreach ($base['paths'] as $path => $methods) {
            if (str_contains($path, '_table')) {
                $paths[$path] = [
                    'get' => $methods['get'] ?? null
                ];
            }
        }
        $base['paths'] = $paths;
        
        $base['description'] = 'Databricks service for connecting to Databricks SQL endpoints.';
        return $base;
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();
        
        // Add stored procedure handler
        $handlers['_proc'] = [
            'name'       => 'Stored Procedure',
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedure',
        ];
        
        // Add function handler
        $handlers['_func'] = [
            'name'       => 'Function',
            'class_name' => StoredFunction::class,
            'label'      => 'Function',
        ];
        
        return $handlers;
    }

    public function handleServiceModified($service)
    {
        if (empty($service->name)) {
            $service->name = 'databricks_' . $service->id;
        }
        return parent::handleServiceModified($service);
    }

    public function getConnection()
    {
        $connection = parent::getConnection();
        
        // Log the DSN string
        $config = $this->getConfig();
        $dsn = $this->getDsnString($config);
        
        \Log::debug('=== DATABRICKS REQUEST DSN ===', [
            'dsn' => $dsn,
            'host' => $config['host'] ?? 'not set',
            'http_path' => $config['options']['http_path'] ?? 'not set',
            'token' => isset($config['options']['token']) ? 'set' : 'not set'
        ]);
        
        return $connection;
    }

    protected function getDsnString($config)
    {
        $useOdbc = $config['use_odbc'] ?? true;
        
        if ($useOdbc) {
            // Validate required fields
            if (empty($config['host'])) {
                throw new \Exception('Databricks host is required');
            }
            
            if (empty($config['options']['http_path'])) {
                throw new \Exception('Databricks HTTP path is required');
            }
            
            if (empty($config['options']['token'])) {
                throw new \Exception('Databricks authentication token is required');
            }
            
            $dsn = "odbc:";
            $dsn .= "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so;";
            $dsn .= "Host={$config['host']};";
            $dsn .= "HTTPPath={$config['options']['http_path']};";
            $dsn .= "UID=token;";
            $dsn .= "PWD={$config['options']['token']};";
            $dsn .= "Port=443;";
            $dsn .= "SSL=1;";
            $dsn .= "ThriftTransport=2;";
            $dsn .= "AuthMech=3";
            
            \Log::debug('=== DATABRICKS DSN CONFIGURED ===', [
                'dsn' => $dsn,
                'host' => $config['host'],
                'http_path' => $config['options']['http_path'],
                'driver_path' => '/opt/simba/spark/lib/64/libsparkodbc_sb64.so'
            ]);
        } else {
            $dsn = "databricks:host={$config['host']}";
            $dsn .= !empty($config['port']) ? ":{$config['port']};" : ';';
            $dsn .= "http_path={$config['options']['http_path']};";
            $dsn .= "token={$config['options']['token']};";
            $dsn .= "thrift_transport=2;ssl=1;auth_mech=3";
        }
        
        return $dsn;
    }
}
