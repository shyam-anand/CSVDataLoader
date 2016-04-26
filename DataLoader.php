<?php

class DataLoader {

    private $inserted_rows_count = 0;
    private $invalid_rows = array();
    private $fields = array();
    private $source_fh;
    private $invalid_rows_fh;
    private $dbh = null;
    private $dry_run = false;
    private $verbosity = 0;

    /**
     * DataLoader constructor.
     * @param $table
     * @param $dbname
     * @param $user
     * @param $password
     * @param $dbhost
     * @param bool $dry_run
     * @param int $verbosity
     */
    function __construct($table, $dbname, $user, $password, $dbhost, $dry_run = false, $verbosity = 0) {
        $this->table = $table;
        $this->dry_run = $dry_run;
        $this->verbosity =$verbosity;
        
        try {
            $this->dbh = new PDO("mysql:host=$dbhost;dbname=$dbname", $user, $password);
        } catch (PDOException $e) {
            die("Failed to connect DB: " . $e->getMessage() . "\n");
        }

        $desc_tables = $this->dbh->query("DESC $table");
        foreach ($desc_tables as $row) {
            $this->fields[] = $row['Field'];
        }
        $this->value_placeholders = implode(',', array_fill(0, count($this->fields), '?'));

    }

    function load($source_file, $invalid_rows_file) {

        if ($this->verbosity && $this->dry_run) {
            echo " -- DRY RUN --\n";
        }

        $this->source_fh = fopen($source_file, "r");
        if (!$this->source_fh) {
            trigger_error("Unable to open {$source_file} for reading\n");
            die;
        }

        $this->invalid_rows_fh = fopen($invalid_rows_file, "w");
        if (!$this->invalid_rows_fh) {
            trigger_error("Unable to open {$invalid_rows_file} for writing\n");
        }

        $insert_stmt = "INSERT INTO {$this->table} " . implode(',', $this->fields) . " VALUES $this->value_placeholders";
        $stmt = $this->dbh->prepare($insert_stmt);

        $row = 1;
        $this->inserted_rows_count = 0;
        $fields_count = count($this->fields);

        while ( ($line = fgets($this->source_fh) ) !== false ) {
            $csv = str_getcsv($line);
            if (count($csv) != $fields_count ) {
                if ($this->verbosity > 0) {
                    echo "Invalid row $row, " . count($csv) . " fields instead of $fields_count\n";
                    echo implode(" | ", $csv);
                    echo "\n";
                }
                $this->invalid_rows[] = $row;
                fwrite($this->invalid_rows_fh, $line);
            } else {
                if (!$this->dry_run) {
                    $stmt->execute($csv);
                }
                $this->inserted_rows_count++;
            }
            $row++;
        }
    }

    function get_inserted_rows_count() {
        return $this->inserted_rows_count;
    }

    function get_invalid_rows() {
        return $this->invalid_rows;
    }

    function __destruct() {
        fclose($this->invalid_rows_fh);
        fclose($this->source_fh);
        $this->dbh = null;
    }
}