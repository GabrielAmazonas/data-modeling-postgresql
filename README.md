# Data Modeling with PostgreSQL

### Goal: 
Extract, Transform and Load the correct information for the songplays into a Star Schema (Fact and Dimension tables) data extracted from the provided json files.

### Running the project:
  - Install and run a postgresql instance locally
  - Install Python 3
  - Run create_tables.py. This will drop the tables if they already exists, and create the needed Dimension & Fact tables.

Afther the above step, run etl.py. This will populate the new created tables.

You can also run test.ipynb to check the results.
