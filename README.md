# CSVDataLoader
Loads data from a CSV file to MySQL table

## Dependencies
[monolog](https://github.com/Seldaek/monolog) managed by [Composer](https://getcomposer.org)

## Usage
php main.php -f source_file -t table_name -d db_name -h db_host -u db_user -p [db_password] [--invalid-rows-file invalid_rows_filename] [--dry dry_run]
