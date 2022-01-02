## About The Project
Data Validation After Migration from MySQL to Snowflake

Apart from basic validation techniques like comparison of number of rows and columns, etc, hashing has been used to perform validation. 
We get the hash value of each tuple in the MySQL table and compare it with the hash value of each tuple in the Snowflake table. 

Multithreading has been used as a basic approach to speed up the process - two threads have been used to simultaneously get the hash values of both the MySQL table and the Snowflake table.
### Built With
* Python
* MySQL
* Snowflake

## Getting Started
### Prerequisites
* [MySQL Workbench](https://dev.mysql.com/downloads/workbench/)
* [Snowflake Account](https://signup.snowflake.com/?_ga=2.222608927.563174558.1641092602-130385872.1639210032)

### Installation
1. Clone the repo.
```sh
   git clone https://github.com/shlokavidi/data-validation
   ```
2. Enter your MySQL and Snowflake credentials in the `dv_config.ini` file.
3. Run `create_mysql_table.py` to create and populate your SQL table with the sample data. This is a standalone module which you need not run if you already have a table in MySQL that you want to migrate.
4. If you are using your own data, change the SQL query in `create_sf_table.py` to suit your table.
```sh
    sql_cmd = ("CREATE OR REPLACE TABLE " + tab_name +
                """ (your table attribute names and their data types);""")
```
5. Finally, run `main.py` to migrate data from MySQL to Snowflake, and validate it.
