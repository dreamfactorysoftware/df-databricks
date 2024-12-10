<?php

namespace DreamFactory\Core\Databricks\Database;

use DreamFactory\Core\Databricks\Database\Query;
use DreamFactory\Core\Databricks\Database\Query\Grammars\DatabricksGrammar;
use DreamFactory\Core\Databricks\Database\Query\Processors\DatabricksProcessor;
use DreamFactory\Core\Databricks\Database\Schema\Grammars\DatabricksGrammar as SchemaGrammar;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use PDO;

class DatabricksConnection extends Connection
{
    /**
     * The Databricks connection handler.
     *
     * @var PDO
     */
    protected $connection;

    /**
     * The name of the default schema.
     *
     * @var string
     */
    protected $defaultSchema;

    /**
     * Create a new database connection instance.
     *
     * @param  array $config
     */
    public function __construct(PDO $pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        parent::__construct($pdo, $database, $tablePrefix, $config);

        if (isset($config['schema'])) {
            $this->currentSchema = $this->defaultSchema = strtoupper($config['schema']);
        }
    }

    /**
     * Get the name of the default schema.
     *
     * @return string
     */
    public function getDefaultSchema()
    {
        return $this->defaultSchema;
    }

    /**
     * Reset to default the current schema.
     *
     * @return string
     */
    public function resetCurrentSchema()
    {
        $this->setCurrentSchema($this->getDefaultSchema());
    }

    /**
     * Set the name of the current schema.
     *
     * @param $schema
     *
     * @return string
     */
    public function setCurrentSchema($schema)
    {
        $this->statement('SET SCHEMA ?', [strtoupper($schema)]);
    }

    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $statement = $this->getPdo()->prepare($query);

            $this->bindValues($statement, $this->prepareBindings($bindings));

            $this->recordsHaveBeenModified();

            $result = $statement->execute();
            if (false === $result) {
                $errorInfo = $statement->errorInfo();
                $errorMessage = $errorInfo[2];
                if ($errorMessage !== null) {
                    $errorCode = $errorInfo[1];
                    throw new \Exception($errorMessage, $errorCode);
                }
            }
            return $result;
        });
    }

    /**
     * Get the default query grammar instance
     *
     * @return Query\Grammars\DatabricksGrammar
     */
    protected function getDefaultQueryGrammar()
    {
        return new DatabricksGrammar;
    }

    /**
     * Get the default post processor instance.
     *
     * @return Query\Processors\DatabricksProcessor
     */
    protected function getDefaultPostProcessor()
    {
        return new DatabricksProcessor;
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return SchemaGrammar
     */
    protected function getDefaultSchemaGrammar()
    {
        return $this->withTablePrefix(new SchemaGrammar());
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param  string $table
     * @return Builder
     */
    public function table($table, $as = null)
    {
        $processor = $this->getPostProcessor();

        $query = new Builder($this, $this->getQueryGrammar(), $processor);

        return $query->from($table, $as);
    }
}