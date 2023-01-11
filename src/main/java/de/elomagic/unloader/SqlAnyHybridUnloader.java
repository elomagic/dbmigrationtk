package de.elomagic.unloader;

import de.elomagic.AppRuntimeException;
import de.elomagic.Configuration;
import de.elomagic.DbUtils;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTable;
import de.elomagic.dto.DbTableContent;
import de.elomagic.loader.SchemaLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;

/**
 *
 * Open issue
 * - Table export includes different, non documented value formats
 *
 * TODO's
 * - Sequences 
 * - Strange column definition like "Feld2" char(20) NULL INLINE 20 PREFIX 8
 * - DB Constraints
 * - DB Functions (Incl. comments)
 * - DB Views
 * - DB Procedures (Incl. comments)
 * - DB Events
 * - Set Timestamp format
 *
 * Ignores:
 * - DBSpaces
 * - Users / Roles
 */
public class SqlAnyHybridUnloader extends SqlAnyReloadUnloader {

    private static final Logger LOGGER = LogManager.getLogger(SqlAnyHybridUnloader.class);

    @Override
    @NotNull
    public DbSystem importDatabase(@NotNull SchemaLoader targetLoader) throws AppRuntimeException {
        try {
            Path file = Paths.get(Configuration.getString(Configuration.SOURCE_FILE));

            DbSystem system = new DbSystem();

            Charset encoding = Charset.forName(Configuration.getString(Configuration.SOURCE_ENCODING));

            LOGGER.info("Reading {} encoded file '{}'...", encoding, file);
            String reloadScript = Files
                    .readString(file, encoding)
                    .replace("\r\n", "\n");

            LOGGER.debug("Length of reload script: {} chars", reloadScript.length());

            try (Connection con = DbUtils.createConnection()) {
                streamGoSections(reloadScript).forEach(s -> processSection(s, system, con, targetLoader));
            }

            return system;
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void processSection(@NotNull String section, @NotNull DbSystem system, @NotNull Connection con, @NotNull SchemaLoader targetLoader) {
        if (PATTERN_TABLE_NAME.matcher(section).find()) {
            processCreateTable(section, system);
        } else if (PATTERN_COLUMN_COMMENT.matcher(section).find()) {
            processColumnComment(section, system);
        } else if (PATTERN_TABLE_COMMENT.matcher(section).find()) {
            processTableComment(section, system);
        } else if (PATTERN_ALTER_COLUMN_UNIQUE.matcher(section).find()) {
            processTableConstraint(section, system);
        } else if (PATTERN_SEQUENCE.matcher(section).find()) {
            processSequence(section, system);
        } else if (PATTERN_CREATE_INDEX.matcher(section).find()) {
            processCreateIndex(section, system);
        } else if (PATTERN_CREATE_FK.matcher(section).find()) {
            processCreateForeignKey(section, system);
        } else if (PATTERN_COMMENT_INDEX.matcher(section).find()) {
            processIndexComment(section, system);
        } else if (PATTERN_LOAD_TABLE.matcher(section).find()) {
            processLoadTable(section, system, con, targetLoader);
        } else if (PATTERN_RESET_IDENTITY.matcher(section).find()) {
            processResetIdentity(section, system);
        } else {
            LOGGER.debug("Ignoring following section:\n{}", section);
        }
    }

    private void processLoadTable(@NotNull String section, @NotNull DbSystem system, @NotNull Connection con, @NotNull SchemaLoader targetLoader) {
        String filter = Configuration.getString(Configuration.TARGET_OUTPUT_TABLER_FILTER);
        List<String> filterTableNames = filter == null ? List.of() : List.of(filter.split(","));

        Matcher matcher = PATTERN_LOAD_TABLE.matcher(section);
        if (matcher.find()) {
            String tableName = matcher.group("tn");
            try {
                if (filterTableNames.isEmpty() || filterTableNames.contains(tableName)) {
                    unloadTable(system.tables.get(tableName), con, targetLoader);
                }
            } catch (Exception ex) {
                LOGGER.error("Unable to parse load table '{}'", tableName);
                throw ex;
            }
        }
    }

    private void unloadTable(@NotNull DbTable table, @NotNull Connection con, @NotNull SchemaLoader targetLoader) {
        try {
            Path file = Path.of(
                    Configuration.getString(Configuration.TARGET_OUTPUT_PATH),
                    "unloaded",
                    table.name + ".dat");

            LOGGER.info("Unloading table data '{}' into '{}'", table.name, file);

            // Order columns as created in original database
            List<DbColumn> indexedColumns = table
                    .columns
                    .values()
                    .stream()
                    .sorted(Comparator.comparing(DbColumn::getIndex))
                    .toList();

            table.content = new DbTableContent();
            table.content.file = file;
            table.content.encoding = Charset.forName(Configuration.getString(Configuration.SOURCE_ENCODING));
            table.content.columns.addAll(indexedColumns.stream().map(c -> c.name).toList());

            Files.createDirectories(table.content.file.getParent());

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
                            DbColumn column = indexedColumns.get(index-1);

                            String value = rs.getString(index);

                            if (rs.wasNull()) {
                                value = null;

                                if (!column.nullable) {
                                    LOGGER.warn("Value of source database table '{}', column '{}' is NULL but must be NOT NULL by schema definition", table.name, column.name);
                                }
                            }

                            value = targetLoader.denormalizeValue(value, column);

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

}
