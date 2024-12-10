<?php

namespace DreamFactory\Core\Databricks\Database\Connectors;

use DreamFactory\Core\Databricks\Database\Schema\DatabricksSchema;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use PDO;

class DatabricksConnector extends Connector implements ConnectorInterface
{
    /**
     * The PDO connection options.
     *
     * @var array
     */
    protected $options = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ];

    public function connect(array $config)
    {
        $options = array_merge($this->getOptions($config), $this->options);
        $dsn = $this->getDsn($config);
        return $this->createConnection($dsn, $config, $options);
    }

    /**
     * Create a new PDO connection instance.
     *
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array $options
     * @return \PDO
     */
    protected function createPdoConnection($dsn, $username, $password, $options)
    {
        $pdo = new PDO($dsn, $username, $password);
        foreach ($options as $key => $value) {
            $pdo->setAttribute($key, $value);
        }

        return $pdo;
    }


    /**
     * Create a DSN string from a configuration.
     *
     * @param array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        $dsn  = "Driver={$driver_path};";
        $dsn .= "Host={$host};";
        $dsn .= "Port=443;";
        $dsn .= "HTTPPath={$http_path};";
        $dsn .= "AuthMech=3;";
        $dsn .= "ThriftTransport=2;";
        $dsn .= "UID=token;";
        $dsn .= "PWD={$token};";
        $dsn .= "SSL=1;";

        return $dsn;
    }
}
