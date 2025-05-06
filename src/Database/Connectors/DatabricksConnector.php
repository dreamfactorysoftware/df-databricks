<?php

namespace DreamFactory\Core\Databricks\Database\Connectors;

use DreamFactory\Core\Databricks\Pdo\PdoOdbc;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use Exception;
use PDO;
use Log;

class DatabricksConnector extends Connector implements ConnectorInterface
{
    /**
     * The PDO connection options.
     *
     * @var array
     */
    protected $options = [
        PDO::ATTR_CASE => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => false,
        PDO::ATTR_EMULATE_PREPARES => false,
        PDO::ATTR_TIMEOUT => 30, // 30 seconds timeout
        PDO::ATTR_PERSISTENT => false, // Don't use persistent connections
    ];

    /**
     * Establish a database connection.
     *
     * @param  array $config
     * @return \PDO
     * @throws \Exception
     */
    public function connect(array $config)
    {
        $dsn = $this->getDsn($config);
        \Log::debug('=== DATABRICKS CONNECTOR CONFIG ===', [
            'config' => $config,
            'dsn' => $dsn,
            'host' => $config['host'] ?? 'not set',
            'port' => $config['port'] ?? 'not set',
            'http_path' => $config['options']['http_path'] ?? 'not set',
            'token' => isset($config['options']['token']) ? 'set' : 'not set'
        ]);

        try {
            $options = $this->getOptions($config);
            \Log::debug('=== DATABRICKS CONNECTION OPTIONS ===', ['options' => $options]);

            $connection = $this->createConnection($dsn, $config, $options);
            \Log::debug('=== DATABRICKS CONNECTION ESTABLISHED ===');

            // Test the connection with a simple query
            $testQuery = "SELECT 1";
            \Log::debug('=== DATABRICKS TESTING CONNECTION ===', ['query' => $testQuery]);
            
            $result = $connection->query($testQuery);
            \Log::debug('=== DATABRICKS TEST QUERY RESULT ===', ['result' => $result->fetchAll()]);

            return $connection;
        } catch (\Exception $e) {
            \Log::error('=== DATABRICKS CONNECTION ERROR ===', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'dsn' => $dsn,
                'host' => $config['host'] ?? 'not set'
            ]);
            throw $e;
        }
    }

    /**
     * Create a new PDO connection.
     *
     * @param  string $dsn
     * @param  array  $config
     * @param  array  $options
     * @return \PDO
     * @throws \Exception
     */
    public function createConnection($dsn, array $config, array $options)
    {
        try {
            \Log::debug('=== DATABRICKS CREATING CONNECTION ===', [
                'dsn' => $dsn,
                'config' => array_merge($config, ['options' => ['token' => '***']]), // Hide token in logs
                'driver_path' => $config['options']['driver_path'] ?? '/opt/databricks/lib64/libsparkodbc_sb64.so',
                'use_odbc' => $config['use_odbc'] ?? true,
                'host' => $config['host'] ?? 'not set',
                'http_path' => $config['options']['http_path'] ?? 'not set',
                'odbc_config' => shell_exec('cat /etc/odbcinst.ini'),
                'registered_drivers' => shell_exec('odbcinst -q -d'),
                'driver_dependencies' => shell_exec('ldd ' . ($config['options']['driver_path'] ?? '/opt/databricks/lib64/libsparkodbc_sb64.so'))
            ]);

            // Check if driver file exists
            $driverPath = $config['options']['driver_path'] ?? '/opt/databricks/lib64/libsparkodbc_sb64.so';
            if (!file_exists($driverPath)) {
                \Log::error('=== DATABRICKS DRIVER ERROR ===', [
                    'error' => 'Driver file does not exist',
                    'driver_path' => $driverPath
                ]);
                throw new \Exception("Databricks ODBC driver not found at: {$driverPath}");
            }

            if (!is_readable($driverPath)) {
                \Log::error('=== DATABRICKS DRIVER ERROR ===', [
                    'error' => 'Driver file is not readable',
                    'driver_path' => $driverPath,
                    'permissions' => substr(sprintf('%o', fileperms($driverPath)), -4)
                ]);
                throw new \Exception("Databricks ODBC driver is not readable at: {$driverPath}");
            }

            if (empty($config['options']['token'])) {
                \Log::error('=== DATABRICKS AUTH ERROR ===', [
                    'error' => 'Token is missing in configuration',
                    'config' => array_merge($config, ['options' => ['token' => '***']])
                ]);
                throw new \Exception('Databricks authentication token is required');
            }

            // For Databricks, we use token authentication
            return new PdoOdbc($dsn, 'token', $config['options']['token'], $options);
        } catch (\PDOException $e) {
            \Log::error('=== DATABRICKS CONNECTION ERROR ===', [
                'error' => $e->getMessage(),
                'code' => $e->getCode(),
                'dsn' => $dsn,
                'config' => array_merge($config, ['options' => ['token' => '***']]),
                'driver_path' => $config['options']['driver_path'] ?? '/opt/databricks/lib64/libsparkodbc_sb64.so',
                'use_odbc' => $config['use_odbc'] ?? true,
                'odbc_config' => shell_exec('cat /etc/odbcinst.ini'),
                'registered_drivers' => shell_exec('odbcinst -q -d')
            ]);
            throw $e;
        }
    }

    /**
     * Create a DSN string from configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        // Check if we're using ODBC or direct connection
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
            
            $dsn = 'odbc:';
            
            // Use the correct driver path
            $driverPath = '/opt/simba/spark/lib/64/libsparkodbc_sb64.so';
            
            // Check if driver file exists and is readable
            if (!file_exists($driverPath)) {
                throw new \Exception("Databricks ODBC driver not found at: {$driverPath}");
            }
            
            if (!is_readable($driverPath)) {
                throw new \Exception("Databricks ODBC driver is not readable at: {$driverPath}");
            }
            
            $dsn .= "Driver={$driverPath};";
            $dsn .= "Host={$config['host']};";
            $dsn .= "HTTPPath={$config['options']['http_path']};";
            
            if (!empty($config['database'])) {
                $dsn .= "Database={$config['database']};";
            }

            // For Databricks, we use token authentication
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
                'driver_path' => $driverPath
            ]);
        } else {
            $dsn = 'databricks:';
            $dsn .= "host={$config['host']}";
            $dsn .= !empty($config['port']) ? ":{$config['port']};" : ';';
            $dsn .= "http_path={$config['options']['http_path']};";
            $dsn .= "token={$config['options']['token']};";
            $dsn .= "thrift_transport=2;ssl=1;auth_mech=3";
        }
        
        return $dsn;
    }

    /**
     * Get the PDO options based on the configuration.
     *
     * @param  array  $config
     * @return array
     */
    public function getOptions(array $config)
    {
        $options = $this->options;
        
        // Override timeout if specified in config
        if (isset($config['options']['timeout'])) {
            $options[PDO::ATTR_TIMEOUT] = (int) $config['options']['timeout'];
        }
        
        // Add any additional options from config
        if (isset($config['options']['pdo_options']) && is_array($config['options']['pdo_options'])) {
            $options = array_merge($options, $config['options']['pdo_options']);
        }
        
        return $options;
    }
}
