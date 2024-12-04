<?php namespace DreamFactory\Core\Databricks\Services;

use DreamFactory\Core\Databricks\Components\DatabricksComponent;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Models\Service;
use DreamFactory\Core\Services\BaseRestService;
use DreamFactory\Core\Databricks\Components\ExampleComponent;
use DreamFactory\Core\Databricks\Models\DatabricksConfig;
use DreamFactory\Core\Databricks\Resources\ExampleResource;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;

class DatabricksService extends BaseRestService
{
    /**
     * @var \DreamFactory\Core\Databricks\Models\DatabricksConfig
     */
    protected $exampleModel = null;


    //*************************************************************************
    //* Methods
    //*************************************************************************

    /**
     * Create a new DatabricksService
     *
     * Create your methods, properties or override ones from the parent
     *
     * @param array $settings settings array
     *
     * @throws \InvalidArgumentException
     */
    public function __construct($settings)
    {
        $this->exampleModel = new DatabricksConfig();
        parent::__construct($settings);
    }

    /**
     * Fetches example.
     *
     * @return array
     * @throws UnauthorizedException
     */
    protected function handleGET()
    {
        $databricks = new DatabricksComponent($this->config);

        $content = $databricks->get('/clusters/get');

        return $content;
    }
}
