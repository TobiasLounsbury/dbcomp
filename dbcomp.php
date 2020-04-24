<?php

abstract class DBCompare {

    abstract function testTable($tableInfo);

    protected $jsonEncodeParam = 0;
    protected $dbLinks = [];
    protected $defaultDatabase;
    protected $allTables;

    function __construct($config = null) {
        
        if ($config) {
            $this->config = $config;
        }

        if(!$this->config) {
            throw new Exception("\$config is undefined: Please supply a config variable");
        }

        $this->setup();
    }

    /**
     * This is the default entry point for comparing
     */
    function compare() {
        $this->_run();
    }


    /**
     * Setup function 
     */
    function setup() {
        foreach($this->config['databases'] as $name => $db) {
            $this->dbLinks[$name] = $this->makeDBConnection($db);
        }
        $this->defaultDatabase = $this->conf("defaultDBConnection", array_keys($this->config['databases'])[0]);
        $this->jsonEncodeParam = ($this->conf("prettyJSON")) ?  JSON_PRETTY_PRINT : 0;
        $q = $this->makeAllTablesQuery();
        $this->allTables = $this->queryAllValues($this->defaultDatabase, $q);

        if($this->conf("includeColumnNames")) {
            $this->fetchAllColumnNames();
        }
    }


    protected function _run() {
        foreach($this->allTables as $table) {
            $this->testTable($table);
        }
        
    }
    

    /**
     * Compose a query to fetch all of the tables to be 
     * searched with relevant column data
     */
    function makeAllTablesQuery($db = null) {

        $columns = ["t.table_name"];
        

        if($this->conf("includeColumnCount")) {
            $columns[] = "cc.column_count";
        }


        $hasColumns = $this->conf("hasColumns");
        if($hasColumns) {
            foreach ($hasColumns as $index => $columnName) {
                $columns[] = "hc$index.has_$columnName";
            }
        }
        
        $tablesQuery = "SELECT " . implode(", ", $columns) . " FROM information_schema.tables as t";
        

        if($this->conf("includeColumnCount")) {
            $tablesQuery .= " LEFT JOIN (SELECT table_name, count(*) as column_count FROM information_schema.columns GROUP BY table_name) as cc ON cc.table_name=t.table_name";
        }

        if($hasColumns) {
            foreach ($hasColumns as $index => $columnName) {
                $columns[] = ".has_$columnName";
                $tablesQuery .= " LEFT JOIN (SELECT table_name, (1) as has_$columnName FROM information_schema.columns WHERE column_name = '$columnName') as hc$index ON hc$index.table_name=t.table_name";
            }
        }

        $wheres = [];

        //Not sure we need to do this for mysql
        if($this->db($db, 'type') == "postgres") {
            $wheres[] = "t.table_schema='public'";    
        } else {
            $wheres[] = "t.table_schema='".$this->db($db, 'schema')."'";
        }

        //White list
        if(array_key_exists("includeTables", $this->config)) {
            $tableFiltersWhere = [];
            foreach($this->config['includeTables'] as $q) {
                $tableFiltersWhere[] = "t.table_name LIKE '${q}'";
            }
            if(!empty($tableFiltersWhere)) {
                $wheres[] = "(". implode(" OR ", $tableFiltersWhere) .")";
            }
        }

        //Black list
        if(array_key_exists("excludeTables", $this->config)) {
            $tableFiltersWhere = [];
            foreach($this->config['excludeTables'] as $q) {
                $tableFiltersWhere[] = "t.table_name NOT LIKE '${q}'";
            }
            if(!empty($tableFiltersWhere)) {
                $wheres[] = "(". implode(" OR ", $tableFiltersWhere) .")";
            }
        }

        //Add all where clauses
        if(!empty($wheres)) {
            $tablesQuery .= " WHERE ". implode(" AND ", $wheres);    
        }

        //Add Sort
        $tablesQuery .= " ORDER BY t.table_name ASC";
        
        return $tablesQuery;
    }


    /**
     * Query for all column names for a given table
     */
    function fetchAllColumnNames($db = null) {
        
        foreach($this->allTables as $index => $table) {
            $q = "SELECT column_name FROM information_schema.columns WHERE table_name = '{$table['table_name']}'";

            if ($this->db($db, 'type') == "postgres") {
                $q .= " AND table_schema='public'";
            } else {
                $q .= " AND table_schema='{$table['schema']}'";
            }

            $q .= " ORDER BY column_name ASC";
            $this->allTables[$index]['column_names'] = $this->convertSingleColumnToValues($this->queryAllValues($db, $q));
        }
    }

    


    


    /**** [ Comparison Functions ] ****/
    

    /**
     * Comparison based on adding and removal of values in a 
     * unique indexed column
     */
    function uniqueColumnComparison($tableName, $db1, $db2, $columnName="id", $wrap=false) {
        $list1 = $this->convertSingleColumnToValues($this->queryAllValues($db1, "SELECT $columnName FROM $tableName LIMIT 0"));
        $list2 = $this->convertSingleColumnToValues($this->queryAllValues($db2, "SELECT $columnName FROM $tableName LIMIT 0"));

        $intersection = array_intersect($list1, $list2);
        $deletedValues = array_diff($list1, $intersection);
        $newValues = array_diff($list2, $list1);

        $result = [];

        if (!empty($deletedValues)) {
            if ($wrap) {
                $list = "'". implode("', '", $deletedValues) ."'";
            } else {
                $list = implode(",", $deletedValues);
            }
            $result["deleted"] = $this->queryAllValues($db1, "SELECT * FROM $tableName WHERE $columnName in ($list) LIMIT 0");
        }

        if(!empty($newValues)) {
            if ($wrap) {
                $list = "'". implode("', '", $newValues) ."'";
            } else {
                $list = implode(",", $newValues);
            }
            $result["new"] = $this->queryAllValues($db1, "SELECT * FROM $tableName WHERE $columnName in ($list) LIMIT 0");
        }
    
        return (!empty($result)) ? $result : false;
    }


    /**
     * Comparison based on the number of rows in the table
     */
    function offsetComparison($tableName, $db1, $db2) {
        $offset = $this->querySingleValue($db1, "SELECT count(*) FROM $tableName");
        return $this->queryAllValues($db2, "SELECT * FROM $tableName OFFSET $offset");
    }


    /**
     * Comparison based on a maximum value
     */
    function maxComparison($columnName, $tableName, $db1, $db2, $wrap = false) {
        $max = $this->querySingleValue($db1, "SELECT max($columnName) FROM $tableName");
        if($wrap) {
            $max = "'$max'";
        }
        return $this->queryAllValues($db2, "SELECT * FROM $tableName WHERE $columnName > $max");
    }

    /**
     * Comparison based on a timestamp column
     */
    function pgNewerThanComparison($tableName, $db1, $db2) {
        $last = $this->querySingleValue($db1, "select max(pg_xact_commit_timestamp(xmin)) from $tableName");
        if(!$last) {
            return false;
        }
        return $this->queryAllValues($db2, "SELECT * FROM $tableName WHERE  pg_xact_commit_timestamp(xmin) > '$last'");
    }
    

    
    
    /**** [ Helper Functions ] ****/
    

    function convertSingleColumnToValues($data, $columnName = false) {
        if (!$data) {
            return [];
        }
        $filt = function($i) use ($columnName) {
            $columnName = ($columnName) ? $columnName : array_keys($i)[0];
            return $i[$columnName];
        };
        return array_map($filt, $data);
    }

    /**
     * Just prints with a new line appended
     */
    function println($m) {
        print $m."\n";
    }

    /**
     * a print results function that currently just delegates to printJson
     * but can be expanded later to do more or delegate to other forms
     * of encoded output as they are written
     */
    function printResults($res) {
        $this->printJson($res);
    }

    /**
     * Print out json
     */
    function printJson($res) {
        $this->println(json_encode($res, $this->jsonEncodeParam));
    }


    function conf($key, $default = false) {
        return (array_key_exists($key, $this->config)) ? $this->config[$key] : $default;
    }

    /**
     * Fetch a database config
     */
    function db($db = null, $key = null) {
        if($db) {
            if($key) {
                return $this->config['databases'][$db][$key];    
            }
            return $this->config['databases'][$db];
        }

        if($key) {
            return $this->config['databases'][ $this->defaultDatabase ][$key];
        }
        return $this->config['databases'][ $this->defaultDatabase ];
    }


    /**
     * Fetch a database link
     */
    function dbLink($db = null) {
        if($db) {
            return $this->dbLinks[$db];
        }
        return $this->dbLinks[ $this->defaultDatabase ];
    }




    /**** [ Abstract Database Functions ] ****/
    

    /**
     * Delegates the creation of a single DB Connection
     * to the appropriate db engine
     */
    function makeDBConnection($def) {
        return call_user_func_array([$this, $def['type']."MakeDBConnection"], [$def]);
    }
    
    //Single Query
    function query($db, $q, $params = null) {
        return call_user_func_array([$this, $this->db($db, "type")."Query"], [$db, $q, $params]);
    }

    //Fetch a row as a numeric array
    function fetch($db, $res) {
        return call_user_func_array([$this, $this->db($db, "type")."Fetch"], [$db, $res]);
    }

    //Fetch a row as an Object
    function fetchObj($db, $res) {
        return call_user_func_array([$this, $this->db($db, "type")."FetchObj"], [$db, $res]);
    }

    //Fetch a row as an assoc array
    function fetchAssoc($db, $res) {
        return call_user_func_array([$this, $this->db($db, "type")."FetchAssoc"], [$db, $res]);
    }

    //Return a single value from a query
    function querySingleValue($db, $q, $params = null) {
        return call_user_func_array([$this, $this->db($db, "type")."QuerySingleValue"], [$db, $q, $params]);
    }

    //Return all the data from a query
    function queryAllValues($db, $q, $params = null) {
        return call_user_func_array([$this, $this->db($db, "type")."QueryAllValues"], [$db, $q, $params]);
    }



    /**** [ PostgreSQL Functions ] ****/


    /**
     * Makes a PostgreSQL db link
     */
    function postgresMakeDBConnection($def) {
        if (array_key_exists("connectionString", $def)) {
            $conString = $def['connectionString'];
        } else {
            $conString = "";

            foreach($def as $key => $value) {
                if (!in_array($key, ["type", "db_link_object"])) {
                    $conString .= " $key=$value";
                }
            }
        }
        return pg_connect($conString);
    }

    /**
     * Execute a PostgreSQL query
     */
    function postgresQuery($db, $q, $params = null) {
        $params = ($params) ? $params : [];
        return pg_query_params($this->dbLink($db), $q, $params);
    }

    function postgresFetch($db, $res) {
        return pg_fetch_array($res);
    }

    function postgresFetchObj($db, $res) {
        return pg_fetch_object($res);
    }

    function postgresFetchAssoc($db, $res) {
        return pg_fetch_assoc($res);
    }

    function postgresQuerySingleValue($db, $q, $params = null) {
        $res = $this->postgresQuery($db, $q, $params);
        return ($res) ? pg_fetch_result($res, 0, 0) : false;
    }

    function postgresQueryAllValues($db, $q, $params = null) {
        $res = $this->postgresQuery($db, $q, $params);
        return ($res) ? pg_fetch_all($res) : false;
    }





    /**** [ MySQL Functions ] ****/


    /**
     * Makes a MySQLi Procedural style db link
     */
    function mysqlMakeDBConnection($def) {
        $host = (array_key_exists("host", $def)) ? $def['host'] : '';
        $user = (array_key_exists("user", $def)) ? $def['user'] : '';
        $user = (array_key_exists("username", $def)) ? $def['username'] : $user;
        $password = (array_key_exists("password", $def)) ? $def['password'] : '';
        $password = (array_key_exists("pass", $def)) ? $def['pass'] : $password;
        $database = (array_key_exists("schema", $def)) ? $def['schema'] : '';
        $port = (array_key_exists("port", $def)) ? $def['port'] : '';
        $socket = (array_key_exists("socket", $def)) ? $def['socket'] : '';
        return mysqli_connect($host, $user, $password, $database, $port, $socket);
    }


    /**
     * Execute a PostgreSQL query
     */
    function mysqlQuery($db, $q, $params = null) {

        if($params && is_array($params)) {
            $keys = array_flip(array_keys($params));
            $find = [];
            $replace = [];
            foreach($keys as $key) {
                $find[] = "\$$key";
                $replace[] = $params[$key];
            }
            $q = str_replace($find, $replace, $q);
        }

        return mysqli_query($this->dbLink($db), $q);
    }

    function mysqlQuerySingleValue($db, $q, $params = null) {
        $r = $this->mysqlQuery($db, $q, $params);
        return ($r) ? mysqli_fetch_array($r)[0] : false;
    }

    function mysqlFetch($db, $res) {
        return mysqli_fetch_array($res);
    }
    
    function mysqlFetchAssoc($db, $res) {
        return mysqli_fetch_assoc($res);
    }
    function mysqlFetchObj($db, $res) {
        return mysqli_fetch_object($res);
    }

    function mysqlQueryAllValues($db, $q, $params = null) {
        $res = $this->mysqlQuery($db, $q, $params);
        return ($res) ? mysqli_fetch_all($res, MYSQLI_ASSOC) : false;
    }

}