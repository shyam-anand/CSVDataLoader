<?php

date_default_timezone_set('Asia/Calcutta');
require __DIR__ . '/vendor/autoload.php';


class DataLoader
{

    private $inserted_rows_count = 0;
    private $invalid_rows = array();
    private $fields = array();
    private $source_fh;
    private $invalid_rows_fh;
    private $dbh = null;
    private $dry_run = false;

    private $logger;

    /**
     * DataLoader constructor.
     * @param String $table Table name to which the data is to be inserted
     * @param String $dbname Database name
     * @param String $user Database user
     * @param String $password Database password
     * @param String $dbhost Database hostname
     * @param bool $dry_run Call to load() does not insert to tables if set to TRUE
     */

    function __construct($table, $dbname, $user, $password, $dbhost, $dry_run = false)
    {

        $this->logger = new Monolog\Logger("CSVDataLoader");

        $this->table = $table;
        $this->dry_run = $dry_run;

        try {
            $this->dbh = new PDO("mysql:host=$dbhost;dbname=$dbname", $user, $password);
        } catch (PDOException $e) {
            $this->logger->err("Failed to connect DB: {$e->getMessage()}");
            die;
        }

        $desc_tables = $this->dbh->query("DESC $table");
        foreach ($desc_tables as $row) {
            $this->fields[] = $row['Field'];
        }
        $this->value_placeholders = implode(', ', array_fill(0, count($this->fields), '?'));

    }

    function load($source_file, $invalid_rows_file)
    {

        $this->logger->info(" Dry run - " .  var_export($this->dry_run, true));

        $this->source_fh = fopen($source_file, "r");
        if (!$this->source_fh) {
            $this->logger->err("Unable to open {$source_file} for reading");
            die;
        }

        $this->invalid_rows_fh = fopen($invalid_rows_file, "w");
        if (!$this->invalid_rows_fh) {
            $this->logger->err("Unable to open {$invalid_rows_file} for writing");
            die;
        }

        $insert_stmt = "INSERT INTO {$this->table} (" . implode(', ', $this->fields) . ") VALUES ($this->value_placeholders)";
        $this->logger->debug("Preparing insert statement - $insert_stmt");
        $stmt = $this->dbh->prepare($insert_stmt);

        $row = 1;
        $this->inserted_rows_count = 0;
        $fields_count = count($this->fields);

        while (($line = fgets($this->source_fh)) !== false) {
            $csv = str_getcsv($line);
            if (count($csv) != $fields_count) {
                $this->logger->debug("Invalid row $row, " . count($csv) . " fields instead of $fields_count");
                $this->logger->debug(implode(" | ", $csv));

                $this->invalid_rows[] = $row;
                fwrite($this->invalid_rows_fh, $line);
            } else {
                if ($this->dry_run === false) {
                    if (!$stmt->execute($csv)) {
                        $error_info = $stmt->errorInfo();
                        trigger_error("Insert failed: [{$error_info[0]}] {$error_info[2]} for values " . implode(", ", $csv), E_USER_ERROR);
                        die;
                    }
                }
                $this->inserted_rows_count++;
            }
            $row++;
        }
    }

    function set_log_file($log_file_path)
    {
        $rotating_file_handler = new \Monolog\Handler\RotatingFileHandler($log_file_path);
        $date_format = "Y-m-d H:i:s";
        $output = "[%datetime%] %channel%.%level_name%: %message%\n";
        $formatter = new \Monolog\Formatter\LineFormatter($output, $date_format);
        $rotating_file_handler->setFormatter($formatter);
        $this->logger->pushHandler($rotating_file_handler);
    }

    function get_inserted_rows_count()
    {
        return $this->inserted_rows_count;
    }

    function get_invalid_rows()
    {
        return $this->invalid_rows;
    }

    function __destruct()
    {
        $this->logger->debug("Closing files and connections");
        fclose($this->invalid_rows_fh);
        fclose($this->source_fh);
        $this->dbh = null;
    }
}