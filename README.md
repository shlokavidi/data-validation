Data Validation After Migration from MySQL to Snowflake

Apart from basic validation techniques like comparison of number of rows and columns (reconciliation test), comparison of attribute names, hashing has been used to perform validation. 
We get the hash value of each tuple in the MySQL table and compare it with the hash value of each tuple in the Snowflake table. This process can be followed when no operations are performed on the data before migration (example - deleting/adding rows or columns, transforming data to an alternative form, etc.).

Multithreading has been used as a basic approach to speed up the process - two threads have been used to simultaneously get the hash values of both the MySQL table and the Snowflake table.
