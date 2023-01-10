package de.elomagic.unloader;

import de.elomagic.AppRuntimeException;
import de.elomagic.Configuration;
import de.elomagic.DbUtils;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbForeignKey;
import de.elomagic.dto.DbIndex;
import de.elomagic.dto.DbIndexComment;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTable;
import de.elomagic.dto.DbTableContent;
import de.elomagic.loader.SchemaLoader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

/**
 * TODO
 * - Foreign Keys
 * - Indexes
 * - DB Constraint
 * - DB Functions (Incl. comments)
 * - DB Views
 * - DB Procedures (Incl. comments)
 * - DB Events
 * - Set Timestamp format
 * Ignores:
 * - DBSpaces
 * - Users / Roles
 */
public class SqlAnyJdbcUnloader implements SqlAnyUnloader {

    private static final Logger LOGGER = LogManager.getLogger(SqlAnyJdbcUnloader.class);

    @Override
    @NotNull
    public DbSystem importDatabase(@NotNull SchemaLoader targetLoader) throws AppRuntimeException {
        DbSystem system = new DbSystem();

        try (Connection con = DbUtils.createConnection()) {
            LOGGER.info(("Importing database schema"));
            importTables(system, con);
            importColumns(system, con);
            importForeignKeys(system, con);
            importIndexes(system,con);

            unloadTables(system, con);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }

        LOGGER.info("Export of database schema and content finished");

        return system;
    }

    void importTables(@NotNull DbSystem system, @NotNull Connection con) throws SQLException {
        String sql = """
                SELECT u.user_name as creator_name, r.remarks AS remarks, t.* FROM systab AS t
                    JOIN sysuser AS u ON t.creator = u.user_id
                    LEFT OUTER JOIN sysremark AS r ON t.object_ID = r.object_id
                    WHERE t.table_type = 1
                    AND user_name in ('dba')
                    ORDER BY table_id
                    """.replace("\n", " ");

        LOGGER.info("Reading database tables schema");

        try (PreparedStatement statement = DbUtils.createPrepareStatement(con, sql, List.of()); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                DbTable table = new DbTable();

                table.id = rs.getInt("table_id");
                table.owner = rs.getString("creator_name");
                table.name = rs.getString("table_name");
                table.comment = rs.getString("remarks");
                // TODO

                system.tables.put(table.name, table);
            }
        }
    }

    void importColumns(@NotNull DbSystem system, @NotNull Connection con) throws SQLException {
        String sql = """
                SELECT NUMBER() as idx, if i.sequence IS NULL THEN 'N' ELSE 'Y' ENDIF AS pk, d.domain_name AS data_type, r.remarks as remarks, t.table_name, c.* FROM SYSTABCOL AS c
                    JOIN systab AS t ON c.table_id = t.table_id
                    LEFT OUTER JOIN sysremark AS r ON c.object_ID = r.object_id
                    LEFT OUTER JOIN sysidxcol AS i ON c.table_id = i.table_id AND c.column_id = i.column_id AND i.index_id = 0
                    JOIN sysuser AS u ON t.creator = u.user_id
                    JOIN sysdomain AS d ON d.domain_id = c.domain_id
                    WHERE t.table_type = 1 and u.user_name in ('dba')
                    ORDER BY t.table_name, c.column_id
                """.replace("\n", " ");

        try (PreparedStatement statement = DbUtils.createPrepareStatement(con, sql, List.of()); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                DbTable table = system.tables.get(tableName);

                DbColumn column = new DbColumn();

                column.name = rs.getString("column_name");
                column.comment = rs.getString("remarks");
                column.nullable = "Y".equalsIgnoreCase(rs.getString("nulls"));
                column.width = rs.getInt("width");
                column.scale = rs.getInt("scale");
                column.nextValue = rs.getLong("max_identity") + 1;
                column.autoinc = "autoincrement".equalsIgnoreCase(rs.getString("default"));
                column.primaryKey = "Y".equalsIgnoreCase(rs.getString("pk"));
                column.defaultValue = "autoincrement".equalsIgnoreCase(rs.getString("default")) ? null : rs.getString("default");
                column.datatype = mapToDbDataType(rs.getString("data_type"));

                column.index = table.columns.size() + 1;

                table.columns.put(column.name, column);
            }
        }
    }

    private void unloadTables(@NotNull DbSystem system, @NotNull Connection con) throws ExecutionException, InterruptedException {
        String filter = Configuration.getString(Configuration.TARGET_OUTPUT_TABLER_FILTER);
        List<String> filterTableNames = filter == null ? List.of() : List.of(filter.split(","));

        ForkJoinPool customThreadPool = new ForkJoinPool(20);
        customThreadPool.submit(() ->
                system.tables
                        .values()
                        .parallelStream()
                        .filter(t -> filterTableNames.isEmpty() || filterTableNames.contains(t.name))
                        .forEach(t -> unloadTable(t, con))).get();
    }

    private void unloadTable(@NotNull DbTable table, @NotNull Connection con) {
        try {
            Path file = Path.of(
                    Configuration.getString(Configuration.TARGET_OUTPUT_PATH),
                    "unloaded",
                    table.name + ".dat");

            LOGGER.info("Unloading table data '{}' into '{}'", table.name, file);

            table.content = new DbTableContent();
            table.content.file = file;
            table.content.encoding = Charset.forName(Configuration.getString(Configuration.SOURCE_ENCODING));
            table.content.columns.addAll(
                    table.columns
                            .values()
                            .stream()
                            .sorted(Comparator.comparing(DbColumn::getIndex))
                            .map(c -> c.name).toList());

            Files.createDirectories(table.content.file.getParent());

            Map<Integer, DbColumn> indexedColumns = new HashMap<>();
            table.columns.values().forEach(c -> indexedColumns.put(c.index, c));

            try (BufferedWriter writer = Files.newBufferedWriter(file, table.content.encoding)) {
                String sql = "SELECT %s FROM \"%s\"".formatted(
                        String.join(",", table.content.columns),
                        table.name
                );

                try (PreparedStatement stmt = DbUtils.createPrepareStatement(con, sql, List.of())) {
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        int columnCount = rs.getMetaData().getColumnCount();
                        List<String> buffer = new ArrayList<>();

                        // LOGGER.debug("Column count {}. DBColumn.count={}", columnCount, table.columns.size());
                        for (int i = 0; i < columnCount; i++) {
                            int index = i + 1;
                            DbColumn column = indexedColumns.get(index);

                            String value = rs.getString(index);

                            if (value == null) {
                                value = Configuration.getString(Configuration.TARGET_OUTPUT_VALUE_NULL);

                                if (!column.nullable) {
                                    LOGGER.warn("Value of source database table '{}', column '{}' is NULL but must be NOT NULL by schema definition", table.name, column.name);
                                }
                            } else {
                                value = normalizeValue(value);
                                value = wrapConditionalValue(value, column);
                            }

                            buffer.add(value);
                        }

                        writer.write(String.join(",", buffer));
                        writer.write("\n");
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    @Nullable
    private String wrapConditionalValue(@Nullable String value, @NotNull DbColumn column) {
        if (value == null) {
            return null;
        }

        // TODO Support any datatype
        return switch (column.datatype) {
            case CHAR, VARCHAR, LONG_VARCHAR -> "\"" + value + "\"";
            case INTEGER, NUMERIC, SMALLINT, BIGINT, TINYINT -> value;
            // TODO Check date time format
            case TIMESTAMP -> value;
            case LONG_BINARY -> "x" + HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
            default -> throw new AppRuntimeException("Datatype " + column.datatype + " currently not support yet.");
        };
    }

    @NotNull
    private String normalizeValue(@NotNull String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\u0000", "")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace(",", "\\,")
                .replace("\r", "\\r");
    }

    void importForeignKeys(@NotNull DbSystem system, @NotNull Connection con) throws SQLException {
        /*
         String sql = """
         SELECT index_name, table_name, user_name FROM sysidx AS i
         JOIN systab AS t ON i.table_id = t.table_id
         JOIN sysuser AS u ON t.creator = u.user_id
         WHERE u.user_name in ('dba') AND index_category = 2
         """.replace("\n", " ");
         */

        String sql = """              
                SELECT u.user_name, p.table_name AS p_table_name, fi.index_name AS fk_name, f.table_name AS fk_table_name, LIST(pcc.column_name || ' ' || pic."order") AS p_cols, LIST(fcc.column_name || ' ' || fic."order") AS f_cols FROM sysfkey AS fk
                    JOIN systab AS f ON f.table_id = fk.primary_table_id
                    JOIN sysuser AS u ON f.creator = u.user_id
                    JOIN sysidx AS fi ON fk.foreign_table_id = fi.table_id AND fk.foreign_index_id = fi.index_id
                    JOIN systab AS p ON p.table_id = fk.foreign_table_id
                    JOIN sysidxcol AS pic ON pic.table_id = fk.foreign_table_id AND pic.index_id = fk.foreign_index_id
                    JOIN systabcol AS pcc ON pcc.table_id = pic.table_id AND pcc.column_id = pic.column_id                
                    JOIN sysidxcol AS fic ON fic.table_id = fk.primary_table_id AND fic.index_id = fk.primary_index_id
                    JOIN systabcol AS fcc ON fcc.table_id = fic.table_id AND fcc.column_id = fic.column_id                
                    WHERE u.user_name in ('dba') AND fi.index_category = 2
                    GROUP BY u.user_name, p_table_name, fk_table_name, fk_name
                    ORDER BY u.user_name, fk_table_name, fk_name
                """.replace("\n", " ");

        LOGGER.info("Reading database foreign keys");

        try (PreparedStatement statement = DbUtils.createPrepareStatement(con, sql, List.of()); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                DbForeignKey fk = new DbForeignKey();
                fk.fkName = rs.getString("fk_name");
                fk.owner = rs.getString("user_name");
                fk.tableName = rs.getString("p_table_name");
                fk.referenceTable = rs.getString("fk_table_name");

                String pcols = rs.getString("p_cols");
                List<Pair<String, Boolean>> columnNames = Arrays
                        .stream(pcols.split(","))
                        .map(c -> c.replace("\"", "").trim())
                        .map(c -> Pair.of(c.split(" ")[0].trim(), c.endsWith(" D")))
                        .toList();
                fk.fkColumns = columnNames;

                String fcols = rs.getString("f_cols");
                List<String> fcolumnNames = Arrays
                        .stream(fcols.split(","))
                        .map(c -> c.replace("\"", "").trim())
                        //.map(c -> Pair.of(c.split(" ")[0].trim(), c.endsWith(" D")))
                        .toList();
                fk.referenceColumns = fcolumnNames;

                // TODO match mode and cascade. Looks that database handles "cascade" with triggers

                system.foreignKeys.add(fk);
            }
        }
    }

    void importIndexes(@NotNull DbSystem system, @NotNull Connection con) throws SQLException {
        String sql = """
                SELECT index_name, table_name, user_name, IF "unique" = 2 THEN 'Y' ELSE 'N' ENDIF AS "unique", remarks, LIST(c.column_name || ' ' || ic."order") AS cols FROM sysidx AS i
                    JOIN systab AS t ON i.table_id = t.table_id
                    JOIN sysuser AS u ON t.creator = u.user_id
                    JOIN sysidxcol as ic ON ic.index_id = i.index_id and ic.table_id = i.table_id
                    JOIN systabcol as c ON c.column_id = ic.column_id AND c.table_id = ic.table_id
                    LEFT JOIN sysremark AS r ON i.object_id = r.object_id
                    WHERE u.user_name in ('dba') AND index_category = 3
                    GROUP BY index_name, table_name, user_name, "unique", remarks
                    ORDER BY table_name, index_name
                """.replace("\n", " ");

        LOGGER.info("Reading database indexes");

        try (PreparedStatement statement = DbUtils.createPrepareStatement(con, sql, List.of()); ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String cols = rs.getString("cols");
                List<Pair<String, Boolean>> columnNames = Arrays
                        .stream(cols.split(","))
                        .map(c -> c.replace("\"", "").trim())
                        .map(c -> Pair.of(c.split(" ")[0].trim(), c.endsWith(" D")))
                        .toList();

                DbIndex index = new DbIndex();
                index.indexName = rs.getString("index_name");
                index.owner = rs.getString("user_name");
                index.tableName = rs.getString("table_name");
                index.unique = "y".equalsIgnoreCase(rs.getString("unique"));
                index.columns.addAll(columnNames);

                system.indexes.put(index.indexName, index);

                String comment = rs.getString("remarks");
                if (StringUtils.isNotEmpty(comment)) {
                    DbIndexComment indexComment = new DbIndexComment();
                    indexComment.tableName = rs.getString("table_name");
                    indexComment.indexName = rs.getString("index_name");
                    indexComment.owner = rs.getString("user_name");
                    indexComment.comment = rs.getString("remarks");

                    system.indexComments.put(indexComment.indexName, indexComment);
                }
            }
        }
    }

}
