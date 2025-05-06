<?php

namespace DreamFactory\Core\Databricks\Config;

use DreamFactory\Core\Models\BaseServiceConfigModel;

class DatabricksConfig extends BaseServiceConfigModel
{
    /** @var string */
    protected $table = 'databricks_config';

    /** @var array */
    protected $fillable = [
        'service_id',
        'label',
        'description',
        'host',
        'port',
        'http_path',
        'token',
        'use_odbc',
        'driver_path'
    ];

    /** @var array */
    protected $casts = [
        'service_id' => 'integer',
        'port' => 'integer',
        'use_odbc' => 'boolean'
    ];

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $schema = [
            [
                'name' => 'host',
                'type' => 'text',
                'label' => 'Host',
                'description' => 'Your Databricks Host URL',
                'required' => true
            ],
            [
                'name' => 'http_path',
                'type' => 'text',
                'label' => 'HTTP Path',
                'description' => 'The HTTP path for your Databricks cluster',
                'required' => true
            ],
            [
                'name' => 'token',
                'type' => 'password',
                'label' => 'Access Token',
                'description' => 'Your Databricks access token',
                'required' => true
            ],
            [
                'name' => 'driver_path',
                'type' => 'text',
                'label' => 'ODBC Driver Path',
                'description' => 'The path to the Databricks ODBC driver',
                'required' => true,
                'default' => '/opt/databricks/lib64/libsparkodbc_sb64.so'
            ]
        ];

        return $schema;
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'label':
                $schema['label'] = 'Simple label';
                $schema['type'] = 'text';
                $schema['description'] = 'This is just a simple label';
                break;

            case 'description':
                $schema['label'] = 'Description';
                $schema['type'] = 'text';
                $schema['description'] = 'This is just a description';
                break;

            case 'host':
                $schema['label'] = 'Databricks Host';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] = 'Your Databricks Host URL';
                break;

            case 'port':
                $schema['label'] = 'Port';
                $schema['type'] = 'integer';
                $schema['default'] = 443;
                $schema['description'] = 'The port number for the Databricks connection';
                break;

            case 'http_path':
                $schema['label'] = 'HTTP Path';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] = 'The HTTP path for your Databricks cluster';
                break;

            case 'token':
                $schema['label'] = 'Access Token';
                $schema['type'] = 'password';
                $schema['required'] = true;
                $schema['description'] = 'Your Databricks access token';
                break;

            case 'use_odbc':
                $schema['label'] = 'Use ODBC';
                $schema['type'] = 'boolean';
                $schema['default'] = true;
                $schema['description'] = 'Whether to use ODBC for the connection';
                break;

            case 'driver_path':
                $schema['label'] = 'ODBC Driver Path';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] = 'The path to the Databricks ODBC driver';
                break;
        }
    }
} 