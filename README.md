# Database Migration Toolkit

[![forthebadge](https://forthebadge.com/images/badges/made-with-java.svg)](https://en.wikipedia.org/wiki/Java)
[![forthebadge](https://forthebadge.com/images/badges/powered-by-coffee.svg)](https://www.buymeacoffee.com/elomagic)
[![forthebadge](https://forthebadge.com/images/badges/compatibility-betamax.svg)](https://en.wikipedia.org/wiki/Betamax)
[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)

Prototype of an toolkit for translating SQLAnywhere "reload" SQL script into Postgres SQL

## Table Of Contents

- [Preparation](#Preparation)
- [Let's do the migration](#Lets-do-the-migration)
- [Useful Links](#Useful-Links)

# Preparation

* Accessible PSQL Postgres application. An unzipped Postgres ZIP archive is sufficient
* JDBC driver for the source database. (For SQLAnywhere, the original ODBC/JDBC driver is strongly recommended. 
  Don't use JConnect driver.)
* Installed Docker
  * Volume ```db_unloaded``` must be mapped to the local unloaded database files.
* Create a configuration file like ```application-dev.properties```` in your working folder.


# Let's do the migration

1. Unload SQLAnywhere schema and data
   1. By using dbunload tool
      Check and if required update path for database access and unload!
   
       ```powershell
       dbunload -y -v -c "UID=dba;PWD=***;CHARSET=utf-8;DBF=C:\db\database.db" -ss -l -r "C:\projects\db\ris-unloaded-example\reload.sql" -ii -up "C:\projects\db\ris-unloaded-example\unload"
       ```
   2. By using JDBC unload
      In this case, the database table unload will also be done by step 2.
    
2. Translate SQL with Java application
    ```powershell
    // TODO java ...
    ```
3. Reload into Postgres
    
    Check and if required update port and/or volume mappings!     

    ```powershell
    docker run --rm --name postgres-migration -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -p 45432:5432 -v "C:/projects/java/dbmigrationtk/target:/db_unloaded" -d postgres:15.1
    psql -p 45432 -U postgres -f .\reload-postgres.sql
    ```

## Useful Links

* https://www.postgresql.org/download/ - Postgres Download Links
* https://www.sqlines.com/sybase-asa-to-postgresql
* https://regex101.com/ - Creating and testing regular expressions
* https://dcx.sap.com/index.html#sqla170/en/html/822e707dc8624445a615b7180321d900.html - SAP SQLAnywhere 17 Documentation 
