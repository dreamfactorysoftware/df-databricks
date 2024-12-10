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

class DatabricksService extends SqlDb
{
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $prefix = parent::getConfigBasedCachePrefix();
        $this->setConfigBasedCachePrefix($prefix);
    }

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'databricks';
        parent::adaptConfig($config);
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();
//        $paths = (array)Arr::get($base, 'paths');
//        foreach ($paths as $pkey => $path) {
//            foreach ($path as $rkey => $resource) {
//                if ($rkey === 'patch' || $rkey === 'put') {
//                    unset($paths[$pkey][$rkey]);
//                    continue;
//                }
//            }
//        }
//        foreach ($paths as $pkey => $path) {
//            if ($pkey !== '/' && isset($path['get']) && isset($path['get']['parameters'])) {
//                $newParams = [
//                    $this->getHeaderPram('hostname'),
//                    $this->getHeaderPram('account'),
//                    $this->getHeaderPram('username'),
//                    $this->getHeaderPram('password'),
//                    $this->getHeaderPram('role'),
//                    $this->getHeaderPram('database'),
//                    $this->getHeaderPram('warehouse'),
//                    $this->getHeaderPram('schema')
//                ];
//                $paths[$pkey]['get']['parameters'] = array_merge($paths[$pkey]['get']['parameters'], $newParams);
//            }
//        }
//        $base['paths'] = $paths;

        return $base;
    }

    public static function getDriverName()
    {
        return 'databricks';
    }

    private function getHeaderPram($name): array
    {
        return [
            "name" => $name,
            "description" => ucfirst($name) . " for database connection.",
            "schema" => [
                "type" => "string"
            ],
            "in" => "header",
            "required" => false
        ];
    }
}
