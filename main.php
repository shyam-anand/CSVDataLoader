<?php

/**
 * This is a command line script which uses DataLoader class to write data from a specified CSV to file the specified table.
 * Usage: php main.php -f source_file -t table_name -d db_name -h db_host -u db_user -p [db_password] [--invalid-rows-file invalid_rows_filename] [--dry dry_run]
 * -f source_file The CSV file to read from
 * -t table_name The table to which the data is to be written
 * -d db_name Database name
 * -h db_host Database host
 * -u db_user Database username
 * -p db_password Database password. Can be either given directly in the command line (not recommended), or at the prompt
 * --invalid-rows-file invalid_rows_filename The file to which invalid rows are to be written
 * --dry dry_run If specified, data won't be inserted to the table. Useful for sanitizing data.
 */

require 'DataLoader.php';
$cmd_opts = getopt("f:t:s:d:u:p::h:v", array("invalid-rows-file:","dry"));

$mandatory_opts = array('f', 't', 'd', 'u', 'p');
$cmd_opts_keys = array_keys($cmd_opts);

if (count(array_intersect($cmd_opts_keys, $mandatory_opts)) != count($mandatory_opts)) {
    die("Usage: php main.php -f source_file -t table_name -d db_name -h db_host -u db_user -p [db_password] [--invalid-rows-file invalid_rows_filename] [--dry dry_run]");
}

$source = $cmd_opts['f'];
$table = $cmd_opts['t'];
$dbname = $cmd_opts['d'];
$user = $cmd_opts['u'];
$pswd = '';
if ($cmd_opts['p'] == '') {
    $pswd = readline("Password: ");
} else {
    trigger_error("Using password in command line is not recommended", E_USER_WARNING);
}
if (array_key_exists('h', $cmd_opts)) {
    $host = $cmd_opts['h'];
} else {
    $host = 'localhost';
}

if (array_key_exists('invalid-rows-file', $cmd_opts)) {
    $invalid_rows_fname = $cmd_opts['invalid-rows-file'];
} else {
    $invalid_rows_fname = "invalid-rows.csv";
}

$verbosity = array_key_exists('v', $cmd_opts) ? 1 : 0;
$dry_run = array_key_exists('dry', $cmd_opts);


// -- //

$loader = new DataLoader($table, $dbname, $user, $pswd, $host, $dry_run, $verbosity);
$loader->set_log_file("log/CSVDataLoader.log");
$loader->load($source, $invalid_rows_fname);

echo $loader->get_inserted_rows_count() . " rows inserted\n";
echo count($loader->get_invalid_rows()) . " invalid rows";
echo "\n\n";
