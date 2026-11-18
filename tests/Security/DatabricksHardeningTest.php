<?php

namespace DreamFactory\Core\Databricks\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: df-databricks must not allow shell-injection via the
 * driver_path config and must parameterize information-schema lookups.
 *
 * Phase 2 audit found:
 *
 * 1. Command injection in DatabricksConnector.php:95
 *      'driver_dependencies' => shell_exec('ldd ' . $config['options']['driver_path'] ?? ...)
 *    A malicious config value like "/tmp/x; rm -rf /; #.so" would be
 *    concatenated raw into the shell command.
 *
 * 2. SQL injection in DatabricksSchema.php — six methods interpolated
 *    $schema/$table/$name into INFORMATION_SCHEMA queries:
 *      - loadTableColumns()  (TABLE_SCHEMA + TABLE_NAME)
 *      - getTableConstraints (TABLE_SCHEMA)
 *      - getViewNames        (TABLE_SCHEMA)
 *      - getProcedureNames   (ROUTINE_SCHEMA)
 *      - getFunctionNames    (ROUTINE_SCHEMA)
 *      - loadParameters      (SPECIFIC_SCHEMA + SPECIFIC_NAME)
 *
 * Fix:
 *  - escapeshellarg the driver_path before passing to shell_exec
 *  - replace each interpolated SQL site with :name / :schema bindings
 */
class DatabricksHardeningTest extends TestCase
{
    public function testConnectorEscapesDriverPath(): void
    {
        $sourcePath = __DIR__ . '/../../src/Database/Connectors/DatabricksConnector.php';
        $this->assertFileExists($sourcePath);
        $contents = file_get_contents($sourcePath);

        // The vulnerable shape was:
        //   shell_exec('ldd ' . $config['options']['driver_path'] ?? ...)
        $this->assertDoesNotMatchRegularExpression(
            "/shell_exec\s*\(\s*'ldd '\s*\.\s*\(\s*\\\$config\[/",
            $contents,
            'Connector must not concatenate driver_path raw into shell_exec'
        );
        $this->assertMatchesRegularExpression(
            "/shell_exec\s*\(\s*'ldd '\s*\.\s*escapeshellarg/",
            $contents,
            'Connector must escapeshellarg the driver_path before shell_exec'
        );
    }

    /**
     * @dataProvider sqlMethodProvider
     */
    public function testSchemaMethodIsParameterized(string $methodName): void
    {
        $sourcePath = __DIR__ . '/../../src/Database/Schema/DatabricksSchema.php';
        $contents = file_get_contents($sourcePath);

        $start = strpos($contents, "function {$methodName}");
        $this->assertNotFalse($start, "method {$methodName} must exist");
        $next = strpos($contents, "\n    /**", $start + 10);
        $body = substr($contents, $start, $next === false ? null : ($next - $start));

        // Forbid interpolated single-quoted PHP-var SQL fragments.
        $this->assertDoesNotMatchRegularExpression(
            "/=\s*'\{?\\\$\w+(?:->\w+)?\}?'/",
            $body,
            "{$methodName}() must not interpolate \$variable into SQL string"
        );
        // Require named placeholder bindings.
        $this->assertMatchesRegularExpression(
            '/:(name|schema)\b/',
            $body,
            "{$methodName}() must use :name / :schema named placeholders"
        );
    }

    public static function sqlMethodProvider(): array
    {
        return [
            'loadTableColumns'    => ['loadTableColumns'],
            'getTableConstraints' => ['getTableConstraints'],
            'getViewNames'        => ['getViewNames'],
            'getProcedureNames'   => ['getProcedureNames'],
            'getFunctionNames'    => ['getFunctionNames'],
            'loadParameters'      => ['loadParameters'],
        ];
    }
}
