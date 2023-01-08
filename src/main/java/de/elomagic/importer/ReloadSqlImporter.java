package de.elomagic.importer;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Doesn't work well because of an RegEx issue?
 */
public class ReloadSqlImporter implements SqlAnyImporter {

    private static final Logger LOGGER = LogManager.getLogger(ReloadSqlImporter.class);

    static final String REGEX_TABLE_SQL = "^(?<sql>CREATE TABLE.*?\\))(\\sIN\\s\\\".*\\\")?\\ngo";
    private static final Pattern PATTERN_TABLE_SQL = Pattern.compile(REGEX_TABLE_SQL, Pattern.MULTILINE | Pattern.DOTALL);
    static final String REGEX_TABLE_NAME = "^(CREATE TABLE \\\".*\\\"\\.\\\")(?<tablename>.*)(\\\")";
    private static final Pattern PATTERN_TABLE_NAME = Pattern.compile(REGEX_TABLE_NAME, Pattern.MULTILINE);
    // ^   [\s|,]\"(?<column>\S*?)\"\s+(?<datatype>.*?)\s(?<nullable>NOT NULL?|NULL)(?<rest>.*?)$
    static final String REGEX_TABLE_COLUMNS = "^   [\\s|,]\\\"(?<columnname>\\S*?)\\\"\\s+(?<datatype>.*?)\\s(?<nullable>NOT NULL?|NULL)(?<rest>.*?)$";
    private static final Pattern PATTERN_TABLE_COLUMNS = Pattern.compile(REGEX_TABLE_COLUMNS, Pattern.MULTILINE);
    static final String REGEX_TABLE_PK = "^   ,PRIMARY KEY \\(\\\"(?<column>.*)\\\"\\s+(?<order>ASC|DESC)\\)";
    private static final Pattern PATTERN_TABLE_PK = Pattern.compile(REGEX_TABLE_PK, Pattern.MULTILINE);
    static final String REGEX_TABLE_COMMENT = "^COMMENT ON TABLE \\\"(?<owner>.*?)\\\"\\.\\\"(?<tablename>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    private static final Pattern PATTERN_TABLE_COMMENT = Pattern.compile(REGEX_TABLE_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);
    static final String REGEX_COLUMN_COMMENT = "^COMMENT ON COLUMN \\\"(?<owner>.*?)\\\"\\.\\\"(?<tablename>.*?)\\\"\\.\\\"(?<columnname>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    private static final Pattern PATTERN_COLUMN_COMMENT = Pattern.compile(REGEX_COLUMN_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);

    @Override
    @NotNull
    public DbSystem importDatabase(String[] args) throws AppRuntimeException {
        try {
            Path file = Paths.get(args.length != 0 ? args[0] : "c:\\projects\\db\\ris-unloaded-example\\reload.sql");

            DbSystem system = new DbSystem();

            LOGGER.info("Reading file ''{}'...", file);
            String reloadScript = Files
                    .readString(file, StandardCharsets.ISO_8859_1)
                    .replace("\r\n", "\n");

            streamTableSQLs(reloadScript)
                    .forEach(sql -> parseCreateTableSQL(system, sql));

            parseTableComments(system, reloadScript);
            parseColumnComments(system, reloadScript);

            return system;
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AppRuntimeException("args=" + Arrays.toString(args), ex);
        }
    }

    private Optional<String> getCreateTablesSection(@NotNull String script) {
        final String REGEX_CREATE_TABLES = ".*(--\\s\\s\\sCreate tables\\n-{49}\\n)(?<sql>.*?)(-{49}.*)";

        Pattern pattern = Pattern.compile(REGEX_CREATE_TABLES, Pattern.MULTILINE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(script);

        LOGGER.info("Parsing for section 'Create tables'...");
        if(matcher.find()) {
            LOGGER.info("Tables found");
            String createTables = matcher.group("sql");

            return Optional.ofNullable(createTables);
        }

        LOGGER.info("Not tables found");
        return Optional.empty();
    }

    private void parseTableComments(@NotNull DbSystem system, @NotNull String reloadScript) {
        Matcher matcher = PATTERN_TABLE_COMMENT.matcher(reloadScript);
        while (matcher.find()) {
            String tableName = matcher.group("tablename");
            try {
                LOGGER.info("Getting comment of table {}", tableName);

                String comment = matcher.group("comment");

                DbTable table = system.tables.get(tableName);
                table.comment = comment;
            } catch (Exception ex) {
                LOGGER.error("Unable to parse comment for table '{}'", tableName);
                throw ex;
            }
        }
    }

    private void parseColumnComments(@NotNull DbSystem system, @NotNull String reloadScript) {
        Matcher matcher = PATTERN_COLUMN_COMMENT.matcher(reloadScript);
        while (matcher.find()) {
            String tableName = matcher.group("tablename");
            String columnName = matcher.group("columnname");
            String comment = matcher.group("comment");
            LOGGER.info("Getting comment of column {}.{}", tableName, columnName);

            DbColumn column = system.tables.get(tableName).columns.get(columnName);
            column.comment = comment;
        }
    }

    /**
     * Returns ONLY table creation SQLs.
     *
     * @param tablesSql
     * @return
     */
    private Stream<String> streamTableSQLs(@NotNull String tablesSql) {
        Matcher matcher = PATTERN_TABLE_SQL.matcher(tablesSql);

        List<String> tableList = new ArrayList<>();

        LOGGER.info("Parsing for tables...");
        while (matcher.find()) {
            String tableSql = matcher.group("sql");
            tableList.add(tableSql);
        }

        LOGGER.info("{} tables found.", tableList.size());

        return tableList.stream();
    }

    /**
     * Parse the "create table" statement of one table.
     *
     * @param createTableSQL
     */
    private void parseCreateTableSQL(@NotNull DbSystem system, @NotNull String createTableSQL) {
        // TODO column UNIQUENESS

        DbTable table = new DbTable();

        Matcher matcher = PATTERN_TABLE_NAME.matcher(createTableSQL);
        if (matcher.find()) {
            table.name = matcher.group("tablename");
            LOGGER.debug("Table '{}' found", table.name);
        } else {
            throw new AppRuntimeException("Table name not found: " + createTableSQL);
        }

        system.tables.put(table.name, table);

        parseColumnsOfTable(table, createTableSQL);

        findPrimaryKey(createTableSQL).ifPresent(s -> table.columns.get(s).primaryKey = true);
    }

    private void parseColumnsOfTable(@NotNull DbTable table, @NotNull String createTableSql) {
        Matcher matcher = PATTERN_TABLE_COLUMNS.matcher(createTableSql);
        while (matcher.find()) {
            String datatype = matcher.group("datatype");
            DbColumn column = new DbColumn();
            column.name = matcher.group("columnname");
            column.datatype = mapToDbDataType(datatype);
            column.nullable = "NOT NULL".equalsIgnoreCase(matcher.group("nullable"));

            table.columns.put(column.name, column);
        }

        Matcher m1 = PATTERN_TABLE_PK.matcher(createTableSql);
        if (m1.find()) {
            Optional.ofNullable(table.columns.get(m1.group(1))).ifPresent(c -> c.primaryKey = true);
        }
    }

    private Optional<String> findPrimaryKey(@NotNull String createTable) {
        Matcher m1 = PATTERN_TABLE_PK.matcher(createTable);
        return m1.find() ? Optional.ofNullable(m1.group(1)) : Optional.empty();
    }

}
