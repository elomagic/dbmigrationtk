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
  Supported properties are
  ```properties
    # JDBC database connection URL
    de.elomagic.dbtk.source.database.url=jdbc:sqlanywhere:CommLinks=tcpip(HOST=localhost;VerifyServerName=NO)
    #de.elomagic.dbtk.source.database.url=jdbc:sybase:Tds:localhost:2638
    # JDBC database user name
    de.elomagic.dbtk.source.username=dba
    # JDBC database password
    de.elomagic.dbtk.source.password=secret
    # Used by the "Reload..." classes. Database properties will be ignored
    de.elomagic.dbtk.source.file=c:\\projects\\db\\db-unloaded-example\\reload.sql
    de.elomagic.dbtk.source.export.path=c:\\projects\\db\\db-unloaded-example
    # Encoding of the reload script and table content files.
    de.elomagic.dbtk.source.encoding=UTF-8
    # Translator class (Currently, the JdbcSqlAnyImporter is recommended)
    de.elomagic.dbtk.source.translator=de.elomagic.importer.ReloadV2SqlImporter
    
    # Name of the target database
    de.elomagic.dbtk.target.databaseName=MigratedDatabase
    # Encoding of the target database
    de.elomagic.dbtk.target.encoding=UTF8
    de.elomagic.dbtk.target.ctype=en_US.utf8
    de.elomagic.dbtk.target.collate=en_US.utf8
    # May be, we don't need an dedicated administrator because we have an postgres administrator
    de.elomagic.dbtk.target.adminRole=adminUser
    de.elomagic.dbtk.target.userRole=user
    de.elomagic.dbtk.target.backupRole=backupUser
    de.elomagic.dbtk.target.output.path=.\\target
    # How to interpret NULL table content files. 
    de.elomagic.dbtk.target.output.value.null=
    ```

# Let's do the migration

1. Unload SQLAnywhere schema and data
   1. By using dbunload tool
      Check and if required update path for database access and unload!
   
       ```powershell
       dbunload -y -v -c "UID=dba;PWD=***;CHARSET=utf-8;DBF=C:\db\database.db" -ss -l -r "C:\projects\db\db-unloaded-example\reload.sql" -ii -up "C:\projects\db\db-unloaded-example\unload"
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
