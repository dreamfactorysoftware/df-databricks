<?php

namespace DreamFactory\Core\Databricks\Models;

use DreamFactory\Core\Models\BaseServiceConfigModel;
use Illuminate\Support\Arr;

/**
 * Class DatabricksService
 *
 * @package DreamFactory\Core\DatabricksService\Models
 */
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
        'token',
        'http_path',
        'driver_path',
    ];

    /** @var array */
    protected $casts = [
        'service_id' => 'integer',
    ];

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
                $schema['description'] =
                    'This is just a description';
                break;
            case 'host':
                $schema['label'] = 'Databricks Host';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your DatabricksService Host URL';
                break;
            case 'token':
                $schema['label'] = 'Databricks API Token';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your Databricks API Token';
                break;
            case 'http_path':
                $schema['label'] = 'Databricks HTTP Path';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your Databricks HTTP Path';
                break;
            case 'driver_path':
                $schema['label'] = 'Databricks ODBC Driver Path';
                $schema['type'] = 'text';
                $schema['required'] = true;
                $schema['description'] =
                    'Your ODBC Driver Path';
                break;
        }
    }


}
