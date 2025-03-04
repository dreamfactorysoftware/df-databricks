<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Migrations\Migration;
use Symfony\Component\Console\Output\ConsoleOutput;

class CreateDatabricksConfigTable extends Migration
{
    /**
     * Run the migrations. To create example database table.
     *
     * @return void
     */
    public function up()
    {
        $driver = Schema::getConnection()->getDriverName();
        // Even though we take care of this scenario in the code,
        // SQL Server does not allow potential cascading loops,
        // so set the default no action and clear out created/modified by another user when deleting a user.
        $onDelete = (('sqlsrv' === $driver) ? 'no action' : 'set null');

        $output = new ConsoleOutput();
        $output->writeln("Migration driver used: $driver");

        Schema::create(
            'databricks_config',
            function (Blueprint $t) use ($onDelete) {
                $t->integer('service_id')->unsigned();
                $t->foreign('service_id')->references('id')->on('service')->onDelete('cascade');
                $t->string('host');
                $t->string('http_path');
                $t->string('token');
                $t->string('driver_path');
            }
        );
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        // Drop created tables in reverse order

        // Database Extras
        Schema::dropIfExists('databricks_config');
    }
}
