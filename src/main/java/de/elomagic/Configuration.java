package de.elomagic;

import org.jetbrains.annotations.NotNull;

public enum Configuration {

    SOURCE_DATABASE_URL("de.elomagic.dbtk.source.database.url", "jdbc:sybase:Tds:localhost:2638"),
    SOURCE_USERNAME("de.elomagic.dbtk.source.username", "dba"),
    SOURCE_PASSWORD("de.elomagic.dbtk.source.password", "secret"),
    SOURCE_FILE("de.elomagic.dbtk.source.file", null),
    SOURCE_EXPORT_PATH("de.elomagic.dbtk.source.export.path", null),
    SOURCE_ENCODING("de.elomagic.dbtk.source.encoding", "UTF-8"),
    SOURCE_TRANSLATOR("de.elomagic.dbtk.source.translator", "de.elomagic.importer.JdbcSqlAnyImporter"),

    TARGET_DATABASE_NAME("de.elomagic.dbtk.target.databaseName", "MigratedDatabase"),
    TARGET_ENCODING("de.elomagic.dbtk.target.encoding", "UTF8"),
    TARGET_CTYPE("de.elomagic.dbtk.target.ctype", "en_US.utf8"),
    TARGET_COLLATE("de.elomagic.dbtk.target.collate", "en_US.utf8"),
    TARGET_ADMIN_ROLE("de.elomagic.dbtk.target.adminRole", "admin"),
    TARGET_USER_ROLE("de.elomagic.dbtk.target.userRole", "user"),
    TARGET_BACKUP_ROLE("de.elomagic.dbtk.target.backupRole", "backup"),
    TARGET_OUTPUT_PATH("de.elomagic.dbtk.target.output.path", ".\\target"),
    TARGET_OUTPUT_TABLER_FILTER("de.elomagic.dbtk.target.output.table.filter", null),
    TARGET_OUTPUT_VALUE_NULL("de.elomagic.dbtk.target.output.value.null", "\\N");

    private final String key;
    private final String defaultValue;

    Configuration(String key, String defaultValue) {

        this.key = key;
        this.defaultValue = defaultValue;
    }

    public static String getString(@NotNull Configuration c) {
        return System.getProperty(c.key, c.defaultValue);
    }

}
