package de.elomagic.unloader;

import de.elomagic.AppRuntimeException;
import de.elomagic.Configuration;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTableContent;
import de.elomagic.loader.SchemaLoader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Open issue
 * - Table export includes different, non documented value formats
 *
 * TODO's
 * - Sequences 
 * - Strange column definition like "Feld2" char(20) NULL INLINE 20 PREFIX 8
 * - DB Foreign keys
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
public class SqlAnyReloadV2Unloader extends SqlAnyReloadUnloader {

    private static final Logger LOGGER = LogManager.getLogger(SqlAnyReloadV2Unloader.class);

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

            streamGoSections(reloadScript).forEach(s -> processSection(s, system));

            return system;
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new AppRuntimeException(ex.getMessage(), ex);
        }
    }

    private void processSection(@NotNull String section, @NotNull DbSystem system) {
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
        } else if (PATTERN_COMMENT_INDEX.matcher(section).find()) {
            processIndexComment(section, system);
        } else if (PATTERN_LOAD_TABLE.matcher(section).find()) {
            processLoadTable(section, system);
        } else if (PATTERN_RESET_IDENTITY.matcher(section).find()) {
            processResetIdentity(section, system);
        } else {
            LOGGER.debug("Ignoring following section:\n{}", section);
        }
    }

    private void processLoadTable(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_LOAD_TABLE.matcher(section);
        if (matcher.find()) {
            String tableName = matcher.group("tn");
            List<String> columnNames = Arrays
                    .stream(matcher.group("cols").split(","))
                    .map(c -> c.replace("\"", ""))
                    .toList();
            Path file = Paths.get(matcher.group("file"));
            try {
                LOGGER.trace("Processing load content SQL from database table {}", tableName);
                DbTableContent content = new DbTableContent();
                content.file = file;
                content.columns.addAll(columnNames);

                system.tables.get(tableName).content = content;
            } catch (Exception ex) {
                LOGGER.error("Unable to parse load table '{}'", tableName);
                throw ex;
            }
        }
    }

}
