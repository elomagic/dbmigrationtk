package de.elomagic.loader;

import de.elomagic.AppRuntimeException;
import de.elomagic.Configuration;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbDataType;
import de.elomagic.dto.DbForeignKey;
import de.elomagic.dto.DbIndex;
import de.elomagic.dto.DbIndexComment;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTable;
import de.elomagic.dto.DbTableConstraint;
import de.elomagic.dto.DbTableContent;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * TODO's
 * - Columns value defaults (To be checked)
 * - Strange column definition like "Feld2" char(20) NULL INLINE 20 PREFIX 8 (To be checked)
 * - DB Constraints
 * - Set Timestamp format
 *
 */
public class PostgresLoader implements SchemaLoader {

    private static final Logger LOGGER = LogManager.getLogger(PostgresLoader.class);

    @Override
    public void export(@NotNull DbSystem system, @NotNull Writer writer) throws AppRuntimeException {
        writeDatabase(writer);
        // TODO Create db spaces ???
        // TODO Create users ???
        // TODO Create roles ???
        // TODO Create dbspace permissions ???
        // TODO Create sequences
        writeTables(writer, system);
        writeLoadTables(writer, system);
        writeForeignKeys(writer, system);
        writeIndexes(writer, system);
        // TODO Create functions (skeletons ?)
        // TODO Create views
        // TODO Create procedures (skeletons ?)
        // TODO Create triggers (skeletons ?)
        // TODO Create Events ???

        LOGGER.debug("Writing SQL done");
    }

    private void writeSectionDescription(@NotNull Writer writer, @NotNull String text) throws IOException {
        writer.append("""
            
            %s
            -- %s
            %s
                            
            """.formatted(
                StringUtils.leftPad("", 49, "-"),
                text,
                StringUtils.leftPad("", 49, "-")));
    }

    private void writeDatabase(@NotNull Writer writer) throws AppRuntimeException {
        LOGGER.info("Writing database init SQL");

       final String SQL =
                """
                -- Create Admin user role
                CREATE ROLE "%s" WITH LOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION ENCRYPTED PASSWORD  'SCRAM-SHA-256$4096:ABr/j0LV6omVjLpGJy7vrA==$5nwquL9wWK4hq17VYPs28scvQY1ylc9zcNoLI55q6xE=:wMDJ11tD/yXtH+8ya/kTvMO/yRltaKVybHX2mLqa0Ic=';
                -- Create default user role
                CREATE ROLE "%s" WITH LOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION ENCRYPTED PASSWORD  'SCRAM-SHA-256$4096:ABr/j0LV6omVjLpGJy7vrA==$5nwquL9wWK4hq17VYPs28scvQY1ylc9zcNoLI55q6xE=:wMDJ11tD/yXtH+8ya/kTvMO/yRltaKVybHX2mLqa0Ic=';
                -- Create backup user role
                CREATE ROLE "%s" WITH NOLOGIN NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE REPLICATION ENCRYPTED PASSWORD  'SCRAM-SHA-256$4096:ABr/j0LV6omVjLpGJy7vrA==$5nwquL9wWK4hq17VYPs28scvQY1ylc9zcNoLI55q6xE=:wMDJ11tD/yXtH+8ya/kTvMO/yRltaKVybHX2mLqa0Ic=';
                                                
                -- TODO Must be configurable
                DROP DATABASE IF EXISTS "%s";
                
                CREATE DATABASE "%s"
                    OWNER "%s"
                    ENCODING %s
                    LC_COLLATE '%s'
                    LC_CTYPE '%s'
                    TABLESPACE = pg_default
                    CONNECTION LIMIT = -1;
                    
                \\connect "%s"
                """.formatted(
                        Configuration.getString(Configuration.TARGET_ADMIN_ROLE),
                        Configuration.getString(Configuration.TARGET_USER_ROLE),
                        Configuration.getString(Configuration.TARGET_BACKUP_ROLE),
                        Configuration.getString(Configuration.TARGET_DATABASE_NAME),
                        Configuration.getString(Configuration.TARGET_DATABASE_NAME),
                        Configuration.getString(Configuration.TARGET_USER_ROLE),
                        Configuration.getString(Configuration.TARGET_ENCODING),
                        Configuration.getString(Configuration.TARGET_COLLATE),
                        Configuration.getString(Configuration.TARGET_CTYPE),
                        Configuration.getString(Configuration.TARGET_DATABASE_NAME)
                        );
        try {
            writer.append(SQL);
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void writeTables(@NotNull Writer writer, @NotNull DbSystem system) throws AppRuntimeException {
        LOGGER.info("Writing tables SQL");
        try {
            writeSectionDescription(writer,"Create tables");

            system.tables.values()
                    .stream()
                    .sorted(Comparator.comparing(DbTable::getId))
                    .forEach(t -> writeTable(writer, t));
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void writeTable(@NotNull Writer writer, @NotNull DbTable table) throws AppRuntimeException {
        final String CREATE_TABLE_PATTERN = "\nCREATE TABLE %s (\n%s\n);\n";

        LOGGER.debug("Writing table SQL '{}'", table.name);

        try {
            String columns = writeTableColumns(table);

            writer.append("\n----------------------------------------------------------\n\n");
            table.columns
                    .values()
                    .stream()
                    .sorted(Comparator.comparing(DbColumn::getIndex))
                    .filter(c -> c.autoinc)
                    .forEach(c -> writeSequencerSql(writer, table, c));

            writer.append(String.format(CREATE_TABLE_PATTERN, table.name, columns));

            writer.append("\n");
            writeTableConstraints(writer, table);
            writeTableComment(writer, table);
            writer.append("\n");
            writeColumnsComments(writer, table);
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private String createSequencerName(@NotNull DbTable table, @NotNull DbColumn column) {
        return "SEQ_%s__%s".formatted(table.name, column.name);
    }

    private void writeSequencerSql(@NotNull Writer writer, @NotNull DbTable table, @NotNull DbColumn column) {
        try {
            writer.append("CREATE SEQUENCE %s AS %s START %d;%n".formatted(
                    createSequencerName(table, column),
                    "bigint",
                    column.nextValue == null ? 1 : column.nextValue));
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    @NotNull
    private String writeTableColumns(@NotNull DbTable table) throws AppRuntimeException {
        try {
            return table.columns
                    .values()
                    .stream()
                    .sorted(Comparator.comparing(DbColumn::getIndex))
                    .map(c -> convertDbColumnToSql(table, c))
                    .collect(Collectors.joining(",\n"));
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private String convertDbColumnToSql(@NotNull DbTable table, @NotNull DbColumn column) {
        StringBuilder sb = new StringBuilder("\t");

        // Wrapped reserved words
        String name = wrapReservedWords(column.name);

        sb.append("%1$-24s".formatted(name));
        sb.append("\t");

        sb.append(mapToPSqlDataType(column));
        sb.append(column.nullable ? " NULL" : " NOT NULL");

        // TODO Uniqueness

        if (column.primaryKey) {
            sb.append(" PRIMARY KEY");
        }

        if (column.autoinc) {
            sb.append(" DEFAULT nextval('%s')".formatted(createSequencerName(table, column)));
        } else if (column.defaultValue != null) {
            // TODO Validate default value
            String defaultValue = switch (column.defaultValue.toUpperCase(Locale.ROOT)) {
                case "CURRENT USER" -> "current_user";
                // timestamp on record update wird so nicht von Postgres unterstützt > Solution Idea > Trigger?
                case "CURRENT TIMESTAMP",
                        "TIMESTAMP",
                        "CURRENT SERVER TIMESTAMP" -> "current_timestamp";
                case "CURRENT DATE" -> "current_date";
                case "\"NOW\"()" -> "now()";
                // Not pretty but it works
                case "\"DATEFORMAT\"(\"NOW\"(),'YYYY.MM.DD')" -> "to_char(current_timestamp, 'YYYY.MM.dd')";
                case "\"DATEFORMAT\"(\"NOW\"(),'YYYY.MM.DD HH:NN:SS')" -> "to_char(current_timestamp, 'YYYY.MM.dd HH24:MI:SS')";
                case "\"DATEFORMAT\"(\"NOW\"(),'YYYY-MM-DD HH:NN:SS')" -> "to_char(current_timestamp, 'YYYY-MM-dd HH24:MI:SS')";
                case "\"DATEFORMAT\"(\"NOW\"(),'YYYY.MM.DD HH:NN:SS.SSS')" -> "to_char(current_timestamp, 'YYYY.MM.dd HH24:MI:SS.MS')";
                case "\"DATEFORMAT\"(\"NOW\"(),'YYYY-MM-DD HH:NN:SS.SSS')" -> "to_char(current_timestamp, 'YYYY-MM-dd HH24:MI:SS.MS')";
                // case "(\"FinancialTransactionSequence\".\"nextval\")" ->
                case "\"DATEFORMAT\"(\"NOW\"(),'HH:NN:SS')" -> "to_char(current_timestamp, 'HH24:MI:SS')";
                default -> column.defaultValue;
            };

            // Workaround for Mandantensystem :-(
            if ("current_user".equals(defaultValue) && column.datatype == DbDataType.INTEGER) {
                defaultValue = "CAST(current_user as INTEGER)";
            }

            sb.append(" DEFAULT %s".formatted(defaultValue));
        }

        return sb.toString();
    }

    private String wrapReservedWords(String word) {
        return switch (word) {
            case "LIMIT", "Limit" -> "\"Limit\"";
            default -> word;
        };
    }

    private void writeTableComment(@NotNull Writer writer, @NotNull DbTable table) throws IOException {
        final String SQL = "\nCOMMENT ON TABLE %s IS '%s';\n";

        writer.append(String.format(SQL,
                table.name,
                escapeString(table.comment)));
    }

    private String escapeString(@Nullable String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }

        return value
                .replace("'", "''")
                .replace("\n", "\\n");
    }

    private void writeColumnsComments(@NotNull Writer writer, @NotNull DbTable table) throws IOException {
        final String SQL = "COMMENT ON COLUMN %s.%s IS '%s';%n";

        for (DbColumn column : table.columns.values()) {
            if (column.comment != null) {
                writer.append(String.format(SQL,
                        table.name,
                        wrapReservedWords(column.name),
                        escapeString(column.comment)));
            }
        }
    }

    private void writeTableConstraints(@NotNull Writer writer, @NotNull DbTable table) throws IOException {
        final String SQL = "ALTER TABLE %s ADD %s%s;%n";

        for (DbTableConstraint constraint : table.constraints) {
            writer.append(String.format(SQL,
                    table.name,
                    constraint.name == null ? "" : "CONSTRAINT \"%s\" ".formatted(constraint.name),
                    constraint.columns.isEmpty() ? "" : "UNIQUE ( %s )".formatted(constraint
                            .columns
                            .stream()
                            // TODO Sorting order missing
                            .map(this::wrapReservedWords)
                            .collect(Collectors.joining(", ")))));
        }
    }

    private void writeForeignKeys(@NotNull Writer writer, @NotNull DbSystem system) {
        try {
            writeSectionDescription(writer,"Create foreign keys");

            final String SQL = """
                    ALTER TABLE %s
                        ADD CONSTRAINT "%s"
                        FOREIGN KEY ( %s )
                        REFERENCES %s ( %s )
                        %s %s
                    """;
            for (DbForeignKey fk : system.foreignKeys) {
                writer.append(String.format(SQL,
                        fk.tableName,
                        fk.name,
                        String.join(", ", fk
                                .fkColumns
                                .stream()
                                .map(c -> "%s %s".formatted(c.getKey(), c.getValue() ? "DESC" : "ASC"))
                                .toList()),
                        fk.referenceTable,
                        String.join(", ", fk
                                .referenceColumns
                                .stream()
                                .toList()),
                        mapRefAction("ON DELETE", fk.actionOnDelete),
                        mapRefAction("ON UPDATE", fk.actionOnUpdate)
                ).trim());

                writer.append(";\n\n");
            }
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void writeIndexes(@NotNull Writer writer, @NotNull DbSystem system) {
        try {
            writeSectionDescription(writer,"Create indexes");

            final String SQL = "CREATE %sINDEX \"%s\" ON %s ( %s );\n";
            final String SQL_COMMENT = "COMMENT ON INDEX %s IS '%s';\n";

            for (DbIndex index : system.indexes.values()) {
                writer.append(String.format(SQL,
                        index.unique ? "UNIQUE " : "",
                        index.indexName,
                        index.tableName,
                        String.join(", ", index
                                .columns
                                .stream()
                                .map(c -> "%s %s".formatted(c.getKey(), c.getValue() ? "DESC" : "ASC"))
                                .collect(Collectors.toSet()))));

                if (system.indexComments.containsKey(index.indexName)) {
                    DbIndexComment indexComment = system.indexComments.get(index.indexName);
                    writer.append((String.format(SQL_COMMENT,
                            index.indexName,
                            indexComment.comment)));
                }
            }
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void writeLoadTables(@NotNull Writer writer, @NotNull DbSystem system) {
        try {
            writeSectionDescription(writer,"Reload data");

            final String SQL = """
                
                COPY %s ( %s )
                    FROM '%s'
                    ( FORMAT TEXT, DELIMITER ',', ENCODING '%s' );
                """;

            for (DbTable table : system.tables.values()) {
                DbTableContent content = table.content;
                if (content == null) {
                    continue;
                }

                List<String> columns = content.columns.stream().map(this::wrapReservedWords).toList();

                writer.append(String.format(SQL,
                        table.name,
                        String.join(",", columns),
                        Paths.get("/db_unloaded", content.file.getParent().getFileName().toString(), content.file.getFileName().toString()).toString().replace("\\", "/"),
                        Configuration.getString(Configuration.SOURCE_ENCODING)));
            }
        } catch (Exception ex) {
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    @NotNull
    private String mapToPSqlDataType(@NotNull DbColumn column) {
        return switch (column.datatype) {
            case BIGINT -> "BIGINT";
            case BIT -> "BOOLEAN";
            case BINARY, IMAGE, LONG_BINARY -> "BYTEA";
            case CHAR, VARCHAR -> "VARCHAR(%d)".formatted(column.width);
            case DATETIME_OFFSET -> "TIMESTAMP WITH TIME ZONE";
            case DECIMAL -> "DECIMAL";
            case DOUBLE,FLOAT -> "DOUBLE PRECISION";
            case INTEGER -> "INTEGER";
            case LONG_VARCHAR -> "text";
            case NUMERIC -> "NUMERIC(%d, %d)".formatted(column.width, column.scale);
            case DATE -> "DATE";
            case SMALLINT, TINYINT -> "SMALLINT";
            case TIMESTAMP, DATETIME -> "TIMESTAMP";
            case UNSIGNED_INT -> "NUMERIC(10)";
            case UNSIGNED_SMALLINT -> "NUMERIC(5)";
            case XML -> "XML";
        };
    }

    @NotNull
    private String mapRefAction(@NotNull String prefix, @NotNull DbForeignKey.RefAction action) {
        return switch (action) {
            case NO_ACTION -> "";
            case CASCADE -> prefix + " CASCADE";
            case RESTRICT -> prefix + " RESTRICT";
            case SET_NULL -> prefix + " SET NULL";
        };
    }

    @Nullable
    public String denormalizeValue(@Nullable String rawValue, @NotNull DbColumn column) {
        if (rawValue == null) {
            return "\\N";
        }

        if ("".equals(rawValue)) {
            return "";
        }

        // TODO Support any datatype
        /*
        String rawValue = switch (column.datatype) {
            case CHAR, VARCHAR, LONG_VARCHAR -> rawValue ;
            case INTEGER, NUMERIC, SMALLINT, BIGINT, TINYINT -> value;
            // TODO Check date time format
            case TIMESTAMP, DATETIME -> value;
            case BINARY, LONG_BINARY -> "x" + HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
            default -> throw new AppRuntimeException("Datatype " + column.datatype + " currently not support yet.");
        };

         */

        /*
         * Backslash characters (\) can be used in the COPY data to quote data characters that might otherwise be taken
         * as row or column delimiters. In particular, the following characters must be preceded by a backslash if they
         * appear as part of a column value: backslash itself, newline, carriage return, and the current delimiter
         * character.
         */
        return switch (column.datatype) {
            case BINARY, LONG_BINARY -> ("\\\\x" + HexFormat.of().formatHex(rawValue.getBytes(StandardCharsets.UTF_8)));
            case INTEGER, NUMERIC, SMALLINT, BIGINT, TINYINT -> rawValue;
            default -> rawValue
                    .replace("\\", "\\\\")
                    .replace("\u0000", "")
                    .replace("\"", "\\\"")
                    .replace("\b", "\\b")
                    .replace("\f", "\\f")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t")
                    .replace(",", "\\,");
        };
    }

}
