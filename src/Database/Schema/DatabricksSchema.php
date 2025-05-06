<?php

namespace DreamFactory\Core\Databricks\Database\Schema;

use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use Arr;
use Log;

/**
 * Schema is the class for retrieving metadata information from a Databricks database.
 */
class DatabricksSchema extends SqlSchema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '"';
    const RIGHT_QUOTE_CHARACTER = '"';

    /**
     * @inheritdoc
     */
    public function getDefaultSchema()
    {
        Log::debug('DatabricksSchema::getDefaultSchema()');
        return $this->getUserName();
    }

    /**
     * @inheritdoc
     */
    protected function translateSimpleColumnTypes(array &$info)
    {
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'integer';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'integer';
                $info['is_foreign_key'] = true;
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = ['expression' => 'CURRENT_TIMESTAMP'];
                }
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = ['expression' => 'CURRENT_TIMESTAMP'];
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_BOOLEAN:
                $info['type'] = 'boolean';
                break;

            case DbSimpleTypes::TYPE_INTEGER:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_DOUBLE:
                $info['type'] = 'double';
                break;

            case DbSimpleTypes::TYPE_FLOAT:
                $info['type'] = 'float';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $info['type'] = 'string';
                break;

            case DbSimpleTypes::TYPE_TEXT:
                $info['type'] = 'text';
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $info['type'] = 'binary';
                break;

            case DbSimpleTypes::TYPE_DATE:
                $info['type'] = 'date';
                break;

            case DbSimpleTypes::TYPE_TIME:
                $info['type'] = 'time';
                break;

            case DbSimpleTypes::TYPE_DATETIME:
            case DbSimpleTypes::TYPE_TIMESTAMP:
                $info['type'] = 'timestamp';
                break;
        }
    }

    /**
     * @inheritdoc
     */
    protected function validateColumnSettings(array &$info)
    {
        // Validate column settings
        if (isset($info['type'])) {
            $type = $info['type'];
            switch ($type) {
                case 'string':
                case 'varchar':
                    if (!isset($info['length'])) {
                        $info['length'] = 255;
                    }
                    break;

                case 'decimal':
                case 'numeric':
                    if (!isset($info['precision'])) {
                        $info['precision'] = 18;
                    }
                    if (!isset($info['scale'])) {
                        $info['scale'] = 0;
                    }
                    break;
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function buildColumnDefinition(array $info)
    {
        $type = (isset($info['type'])) ? $info['type'] : null;
        $definition = '';

        switch ($type) {
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $definition .= 'INTEGER IDENTITY(1,1) PRIMARY KEY';
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $definition .= 'INTEGER';
                break;

            case 'string':
            case 'varchar':
                $length = (isset($info['length'])) ? $info['length'] : 255;
                $definition .= "VARCHAR($length)";
                break;

            case 'text':
                $definition .= 'TEXT';
                break;

            case 'integer':
                $definition .= 'INTEGER';
                break;

            case 'double':
                $definition .= 'DOUBLE';
                break;

            case 'float':
                $definition .= 'FLOAT';
                break;

            case 'decimal':
            case 'numeric':
                $precision = (isset($info['precision'])) ? $info['precision'] : 18;
                $scale = (isset($info['scale'])) ? $info['scale'] : 0;
                $definition .= "DECIMAL($precision,$scale)";
                break;

            case 'boolean':
                $definition .= 'BOOLEAN';
                break;

            case 'date':
                $definition .= 'DATE';
                break;

            case 'time':
                $definition .= 'TIME';
                break;

            case 'timestamp':
                $definition .= 'TIMESTAMP';
                break;

            case 'binary':
                $definition .= 'BINARY';
                break;
        }

        if (isset($info['allow_null']) && !$info['allow_null']) {
            $definition .= ' NOT NULL';
        }

        if (isset($info['default'])) {
            $default = $info['default'];
            if (is_array($default) && isset($default['expression'])) {
                $definition .= ' DEFAULT ' . $default['expression'];
            } else {
                $definition .= ' DEFAULT ' . $this->quoteValue($default);
            }
        }

        return $definition;
    }

    /**
     * @inheritdoc
     */
    public function compareTableNames($name1, $name2)
    {
        return strcasecmp($name1, $name2);
    }

    /**
     * @inheritdoc
     */
    public function resetSequence($table, $value = null)
    {
        // Not supported in Databricks
        return false;
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{$table->schemaName}' AND TABLE_NAME = '{$table->name}'";
        $columns = $this->connection->select($sql);

        foreach ($columns as $column) {
            $columnSchema = new ColumnSchema();
            $columnSchema->name = $column['COLUMN_NAME'];
            $columnSchema->rawName = $this->quoteColumnName($column['COLUMN_NAME']);
            $columnSchema->allowNull = ($column['IS_NULLABLE'] === 'YES');
            $columnSchema->dbType = $column['DATA_TYPE'];
            $columnSchema->defaultValue = $column['COLUMN_DEFAULT'];
            $columnSchema->size = $column['CHARACTER_MAXIMUM_LENGTH'];
            $columnSchema->precision = $column['NUMERIC_PRECISION'];
            $columnSchema->scale = $column['NUMERIC_SCALE'];

            $this->extractType($columnSchema, $column['DATA_TYPE']);
            $this->extractDefault($columnSchema, $column['COLUMN_DEFAULT']);
            $this->extractLimit($columnSchema, $column['DATA_TYPE']);

            $table->addColumn($columnSchema);
        }
    }

    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '$schema'";
        return $this->connection->select($sql);
    }

    /**
     * @inheritdoc
     */
    public function getSchemas()
    {
        $sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA";
        return $this->connection->select($sql);
    }

    /**
     * @inheritdoc
     */
    public function getTableNames($schema = null)
    {
        \Log::debug('=== DATABRICKS GETTING TABLE NAMES ===', [
            'schema' => $schema ?? $this->schema,
            'connection' => $this->connection->getConfig()
        ]);

        try {
            // First try a simple query to test the connection
            $testSql = "SHOW TABLES";
            \Log::debug('=== DATABRICKS EXECUTING TEST QUERY ===', [
                'sql' => $testSql
            ]);

            $startTime = microtime(true);
            $testResult = $this->connection->select($testSql);
            $endTime = microtime(true);
            
            \Log::debug('=== DATABRICKS TEST QUERY COMPLETED ===', [
                'execution_time' => ($endTime - $startTime) . ' seconds',
                'result_count' => count($testResult)
            ]);

            if (empty($testResult)) {
                \Log::debug('=== DATABRICKS NO TABLES FOUND ===');
                return [];
            }

            // Extract table names from the result
            $tables = array_column($testResult, 'tableName');
            \Log::debug('=== DATABRICKS TABLES FOUND ===', ['tables' => $tables]);
            
            return $tables;
        } catch (\Exception $e) {
            \Log::error('=== DATABRICKS TABLE QUERY ERROR ===', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'sql' => $testSql ?? 'not set',
                'schema' => $schema ?? $this->schema
            ]);
            throw $e;
        }
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames($schema = '')
    {
        $sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$schema' AND TABLE_TYPE = 'VIEW'";
        return $this->connection->select($sql);
    }

    /**
     * @inheritdoc
     */
    public function getProcedureNames($schema = '')
    {
        $sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '$schema' AND ROUTINE_TYPE = 'PROCEDURE'";
        return $this->connection->select($sql);
    }

    /**
     * @inheritdoc
     */
    public function getFunctionNames($schema = '')
    {
        $sql = "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '$schema' AND ROUTINE_TYPE = 'FUNCTION'";
        return $this->connection->select($sql);
    }

    /**
     * @inheritdoc
     */
    protected function loadParameters(RoutineSchema $holder)
    {
        $sql = "SELECT * FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_SCHEMA = '{$holder->schemaName}' AND SPECIFIC_NAME = '{$holder->name}'";
        $parameters = $this->connection->select($sql);

        foreach ($parameters as $parameter) {
            $paramSchema = new ParameterSchema();
            $paramSchema->name = $parameter['PARAMETER_NAME'];
            $paramSchema->position = $parameter['ORDINAL_POSITION'];
            $paramSchema->paramType = $parameter['PARAMETER_MODE'];
            $paramSchema->type = $parameter['DATA_TYPE'];
            $paramSchema->dbType = $parameter['DATA_TYPE'];
            $paramSchema->length = $parameter['CHARACTER_MAXIMUM_LENGTH'];
            $paramSchema->precision = $parameter['NUMERIC_PRECISION'];
            $paramSchema->scale = $parameter['NUMERIC_SCALE'];

            $holder->addParameter($paramSchema);
        }
    }

    /**
     * @inheritdoc
     */
    public function renameTable($table, $newName)
    {
        $sql = "ALTER TABLE {$this->quoteTableName($table)} RENAME TO {$this->quoteTableName($newName)}";
        return $this->connection->statement($sql);
    }

    /**
     * @inheritdoc
     */
    public function renameColumn($table, $name, $newName)
    {
        $sql = "ALTER TABLE {$this->quoteTableName($table)} RENAME COLUMN {$this->quoteColumnName($name)} TO {$this->quoteColumnName($newName)}";
        return $this->connection->statement($sql);
    }

    /**
     * @inheritdoc
     */
    public function alterColumn($table, $column, $definition)
    {
        $sql = "ALTER TABLE {$this->quoteTableName($table)} ALTER COLUMN {$this->quoteColumnName($column)} {$this->buildColumnDefinition($definition)}";
        return $this->connection->statement($sql);
    }

    /**
     * @inheritdoc
     */
    public function typecastToClient($value, $field_info, $allow_null = true)
    {
        if (is_null($value)) {
            return null;
        }

        $type = (isset($field_info['type'])) ? $field_info['type'] : null;
        switch ($type) {
            case 'boolean':
                return (bool)$value;

            case 'integer':
                return (int)$value;

            case 'double':
                return (double)$value;

            case 'float':
                return (float)$value;

            case 'string':
            case 'text':
                return (string)$value;

            case 'date':
            case 'time':
            case 'timestamp':
                return $value;

            case 'binary':
                return $value;

            default:
                return $value;
        }
    }

    /**
     * @inheritdoc
     */
    public function extractType(ColumnSchema $column, $dbType)
    {
        $type = strtolower($dbType);
        if (strpos($type, 'int') !== false) {
            $column->type = 'integer';
        } elseif (strpos($type, 'char') !== false || strpos($type, 'text') !== false) {
            $column->type = 'string';
        } elseif (strpos($type, 'float') !== false || strpos($type, 'double') !== false || strpos($type, 'decimal') !== false) {
            $column->type = 'double';
        } elseif (strpos($type, 'bool') !== false) {
            $column->type = 'boolean';
        } elseif (strpos($type, 'date') !== false || strpos($type, 'time') !== false) {
            $column->type = $type;
        } elseif (strpos($type, 'binary') !== false) {
            $column->type = 'binary';
        } else {
            $column->type = 'string';
        }
    }

    /**
     * @inheritdoc
     */
    public function extractDefault(ColumnSchema $field, $defaultValue)
    {
        if ($defaultValue === null) {
            return;
        }

        $field->defaultValue = $defaultValue;
    }

    /**
     * @inheritdoc
     */
    public function extractLimit(ColumnSchema $field, $dbType)
    {
        if (strpos($dbType, '(') !== false) {
            if (preg_match('/^([^(]+)\((\d+)\)/', $dbType, $matches)) {
                $field->size = (int)$matches[2];
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function getProcedureStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = '';
        foreach ($param_schemas as $key => $param) {
            if ($key > 0) {
                $paramStr .= ',';
            }
            $paramStr .= '?';
        }

        return "CALL {$this->quoteTableName($routine->name)}($paramStr)";
    }

    /**
     * @inheritdoc
     */
    protected function doRoutineBinding($statement, array $paramSchemas, array &$values)
    {
        foreach ($paramSchemas as $key => $param) {
            $value = $values[$key];
            $this->bindValue($statement, $key + 1, $value, $param->type);
        }
    }
}