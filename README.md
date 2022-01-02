## Data Validation After Migration from MySQL to Snowflake

The major challenge after data migration is to validate the data. Migrated data has to be 100% identical to the original data unless one performs transformations. This project demonstrates 
*	data migration
*	basic validation
*	advanced validation using hashing
*	and multithreading for performance

Apart from basic validation techniques like comparison of number of rows and columns, etc., hashing is used to perform validation. We get the hash value of each tuple in the MySQL table and compare it with the hash value of each tuple in the Snowflake table.

When the migrated table is large in size, that is, with multi-million records, multithreading will be useful to achieve performance. In this example, I have used two threads to simultaneously get the hash values of both the MySQL table and the Snowflake table. Based on the needs, we can adopt more threads.

### Built With
*	Python & SQL
*	MySQL
*	Snowflake


## Getting Started
### Prerequisites
1.	MySQL Account
* Table that needs to be migrated.
* For sample table, the following is included
   - a Python Script to create the sample table (sales_table) and 
   - a sample CSV file for uploading on to MySQL for migrating exercise
2.	Snowflake Account
### Installation
1.	Clone the repo.
```sh
   gh repo clone shlokavidi/data-validation
```
2.	Enter following details in in the `config/dv_config.ini` file.
*	MySQL credentials
*	Snowflake credentials 
*	Database name that is used on both MySQL and Snowflake
*	Name of the table migrated
3.	For creating sample table and uploading data on MySQL
*	Run `python create_mysql_table.py` 
*	NOTE: This is a standalone program which you need not run if you already have a table in MySQL that you can migrate to Snowflake.

4.	If you are using your own data, change the SQL query in `create_sf_table.py` to suit your table.
```python
    sql_cmd = ("CREATE OR REPLACE TABLE " + tab_name +
                """ (your table attribute names and their data types);""")
```
5.	Confirm that the table for migrating is ready and credentials are working correctly.
```sh
mysql -u <user_id> -p
```
```sh
select count(*) from <table_name>
```
6.	The functionality of the main program
* Establishes connection with MySQL
* Establishes connection with Snowflake
* Creates the required table in Snowflake with expected columns and column types
* Migrates the data
* Performs basic data validation
* Performs hashing data validation (using single thread)
* Calculates the time taken for hashing data validation
* Creates two threads
   - One thread to get the hash values in MySQL
   - Another thread to get the hash values in Snowflake
* Compares the time improvements with multithreads
   - Finally, run main.py to migrate data from MySQL to Snowflake, and validate it.



