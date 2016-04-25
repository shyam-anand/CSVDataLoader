<?php
$cmd_opts = getopt("f:t:s:d:u:p::h:", array("invalid-rows-file:","dry"));

$source = $cmd_opts['f'];
$table = $cmd_opts['t'];
$dbname = $cmd_opts['d'];
$user = $cmd_opts['u'];
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

$dry_run = array_key_exists('dry', $cmd_opts);


$loader = new DataLoader($table, $dbname, $user, $pswd, $host, $dry_run);
$loader->load($source, $invalid_rows_fname);

echo $loader->get_inserted_rows_count() . " rows inserted";
echo count($loader->get_invalid_rows()) . " invalid rows";
echo "\n\n";
