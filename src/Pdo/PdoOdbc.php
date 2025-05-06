<?php

namespace DreamFactory\Core\Databricks\Pdo;

use PDO;
use PDOStatement;

class PdoOdbc extends PDO
{
    /**
     * Create a new PDO instance.
     *
     * @param string $dsn
     * @param string|null $username
     * @param string|null $password
     * @param array $options
     */
    public function __construct($dsn, $username = null, $password = null, array $options = [])
    {
        \Log::info('Constructing PDO instance with DSN: ' . $dsn);
        parent::__construct($dsn, $username, $password, $options);
    }

    /**
     * Prepare a new statement.
     *
     * @param string $query
     * @param array $options
     * @return PDOStatement
     */
    public function prepare($query, $options = [])
    {
        \Log::info('Preparing query: ' . $query);
        $statement = parent::prepare($query, $options);

        if ($statement instanceof PDOStatement) {
            $statement->setFetchMode(PDO::FETCH_ASSOC);
        }

        return $statement;
    }
} 