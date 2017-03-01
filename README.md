# store-upc-loader
Consumes Gap Inc. Product Catalog API response and populates a MySQL database with the mapping of (Store) UPC values and online SKUs.

In order to run successfully on a given server with Python 2.7 installed...
- The MySQLdb Python module must be present (1.2.5)
- A 'config.py' file must be created in the same location as the 'load-upc-data.py' script containing the MySQL connection information and a valid Product Catalog API key (see 'EMPTY-config.py' as an example template)
- The MySQL instance running on the target server must be configured to accept traffic from the machine executing the script
- If necessary, Update MySQL table in SQL statement to ensure INSERTS are committed to the right destination (and ensure table is configured with expected columns & definitions)
