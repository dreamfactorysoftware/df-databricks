<?php

namespace DreamFactory\Core\Databricks\Components;

use Exception;

class DatabricksComponent
{
    /**
     * @var string Databricks API token
     */
    private $apiToken;

    /**
     * @var string Databricks instance URL
     */
    private $baseUrl;

    public function __construct($config)
    {
        $databricksHost = $config['host'];
        $this->apiToken = $config['token'];
        $this->baseUrl = "https://{$databricksHost}/api/2.0";
    }

    /**
     * Make a GET request to the Databricks API.
     *
     * @param string $endpoint
     * @param array $params
     * @return array
     * @throws Exception
     */
    public function get($endpoint, $params = [])
    {
        $url = $this->baseUrl . $endpoint . '?' . http_build_query($params);

        $response = $this->makeRequest('GET', $url);

        return json_decode($response, true);
    }

    /**
     * Make a POST request to the Databricks API.
     *
     * @param string $endpoint
     * @param array $data
     * @return array
     * @throws Exception
     */
    public function post($endpoint, $data = [])
    {
        $url = $this->baseUrl . $endpoint;

        $response = $this->makeRequest('POST', $url, $data);

        return json_decode($response, true);
    }

    /**
     * Generic method to handle HTTP requests.
     *
     * @param string $method
     * @param string $url
     * @param array|null $data
     * @return string
     * @throws Exception
     */
    private function makeRequest($method, $url, $data = null)
    {
        $ch = curl_init();

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HTTPHEADER, [
            "Authorization: Bearer {$this->apiToken}",
            "Content-Type: application/json",
        ]);

        if ($method === 'POST') {
            curl_setopt($ch, CURLOPT_POST, true);
            if (!empty($data)) {
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
            }
        }

        $response = curl_exec($ch);

        if (curl_errno($ch)) {
            $error = curl_error($ch);
            curl_close($ch);
            throw new Exception("cURL Error: $error");
        }

        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        if ($httpCode >= 400) {
            throw new Exception("HTTP Error: $httpCode Response: $response");
        }

        return $response;
    }
}
