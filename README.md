# CSVDataLoader
Loads data from a CSV file to MySQL table

## Dependencies
[monolog](https://github.com/Seldaek/monolog) managed by [Composer](https://getcomposer.org)

## Usage
`php main.php -f source_file -t table_name -d db_name -h db_host -u db_user -p [db_password] [--invalid-rows-file filename] [--dry dry_run]`

```
-f source_file Source CSV file to load data from
-t table_name Target table name
-d db_name Target database
-h db_host Database hostname
-u db_user Database username
-p [db_password] Database password. Optional to give the password here. If -p is set without value, will be prompted for the password at run-time.
--invalid-rows-file filename File name to which the rows which could not be processed will be written. Usefull if there are lines with incorrect formatting
--dry If set, data will not be inserted to the table
```
