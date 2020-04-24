<?php

require_once("dbcomp.php");

class CompareOdoo extends DBCompare {
    public $config = [ 
        "databases" => [
            "restore" => [
                "type" => "postgres",
                "host" => "localhost",
                "port" => 2345,
                "dbname" => "odoo-testpoint-a",
                "user" => "odoo",
                "password" => "odoo",
            ],
            "test" => [
                "type" => "postgres",
                "host" => "localhost",
                "port" => 2345,
                "dbname" => "odoo-testpoint-b",
                "user" => "odoo",
                "password" => "odoo",
            ],
        ],
        "prettyJSON" => true,
        "includeColumnNames" => false,
        "includeColumnCount" => true,
        "includeTables" => ["account%"],
        "excludeTables" => ["account_invoice_report"],
        "hasColumns" => ["write_date", "id"]
    ];



    function testTable($tableInfo) {
        $this->println("Checking transaction history: ". $tableInfo['table_name']);
        $diff = $this->pgNewerThanComparison($tableInfo['table_name'], "restore", "test");
        if($diff) {
            $this->printResults($diff);
        }
    }
}

$comp = new CompareOdoo();
$comp->compare();

