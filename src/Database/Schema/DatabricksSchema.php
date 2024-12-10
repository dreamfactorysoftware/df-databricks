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

class DatabricksSchema extends SqlSchema
{
    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $sql = 'SHOW TABLES ';

        if (!empty($schema)) {
            $sql .= ' IN ' . $this->quoteTableName($schema);
        }

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $schemaName = $schema;
            $resourceName = $row[1];
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

}