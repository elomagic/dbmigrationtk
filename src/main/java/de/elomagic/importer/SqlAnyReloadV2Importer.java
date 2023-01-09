package de.elomagic.importer;

import de.elomagic.AppRuntimeException;
import de.elomagic.Configuration;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbIndex;
import de.elomagic.dto.DbIndexComment;
import de.elomagic.dto.DbSequence;
import de.elomagic.dto.DbSystem;
import de.elomagic.dto.DbTable;
import de.elomagic.dto.DbTableConstraint;
import de.elomagic.dto.DbTableContent;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class SqlAnyReloadV2Importer implements SqlAnyImporter {

    private static final Logger LOGGER = LogManager.getLogger(SqlAnyReloadV2Importer.class);

    // (-{49}\n--\s{3}.*?\n-{49}\n)?(^.*?go\n\n)
    static final String REGEX_GO_SECTIONS = "(^\\w.*?go\\n\\n)";
    private static final Pattern PATTERN_GO_SECTIONS = Pattern.compile(REGEX_GO_SECTIONS, Pattern.MULTILINE | Pattern.DOTALL);

    static final String REGEX_TABLE_SQL = "^(?<sql>CREATE TABLE.*?\\))(\\sIN\\s\\\".*\\\")?\\ngo";
    private static final Pattern PATTERN_TABLE_SQL = Pattern.compile(REGEX_TABLE_SQL, Pattern.MULTILINE | Pattern.DOTALL);
    static final String REGEX_TABLE_NAME = "(CREATE TABLE \\\"(?<o>.*)\\\"\\.\\\")(?<tn>.*)(\\\")";
    private static final Pattern PATTERN_TABLE_NAME = Pattern.compile(REGEX_TABLE_NAME, Pattern.MULTILINE);
    // ^   [\s|,]\"(?<column>\S*?)\"\s+(?<datatype>.*?)\s(?<nullable>NOT NULL?|NULL)(?<rest>.*?)$
    static final String REGEX_TABLE_COLUMNS = "^   [\\s|,]\\\"(?<cn>\\S*?)\\\"\\s+(?<datatype>.*?)\\s(?<nullable>NOT NULL?|NULL)(?<def> DEFAULT )?(?<dv>.*?)$";
    private static final Pattern PATTERN_TABLE_COLUMNS = Pattern.compile(REGEX_TABLE_COLUMNS, Pattern.MULTILINE);
    static final String REGEX_TABLE_PK = "^   ,PRIMARY KEY \\(\\\"(?<column>.*)\\\"\\s+(?<order>ASC|DESC)\\)";
    private static final Pattern PATTERN_TABLE_PK = Pattern.compile(REGEX_TABLE_PK, Pattern.MULTILINE);
    static final String REGEX_TABLE_COMMENT = "^COMMENT ON TABLE \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    private static final Pattern PATTERN_TABLE_COMMENT = Pattern.compile(REGEX_TABLE_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);
    static final String REGEX_COLUMN_COMMENT = "^COMMENT ON COLUMN \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\.\\\"(?<cn>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    private static final Pattern PATTERN_COLUMN_COMMENT = Pattern.compile(REGEX_COLUMN_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);
    // ^ALTER TABLE \"(?<o>.*)\".\"(?<tn>.*)\"\n\s{4}ADD( CONSTRAINT )?\"?(?<n>.*)\"? UNIQUE \(\s(?<cn>\".*\")\s\)
    // ALTER TABLE \"(?<o>.*)\".\"(?<tn>.*)\"\n\s{4}ADD UNIQUE \(\s\"(?<cn>.*)\"\s\)
    private static final String REGEX_ALTER_COLUMN_UNIQUE = "^ALTER TABLE \\\"(?<o>.*)\\\".\\\"(?<tn>.*)\\\"\\n\\s{4}ADD( CONSTRAINT )?\\\"?(?<name>.*)\\\"? UNIQUE \\(\\s(?<cn>\\\".*\\\")\\s\\)";
    private static final Pattern PATTERN_ALTER_COLUMN_UNIQUE = Pattern.compile(REGEX_ALTER_COLUMN_UNIQUE);//, Pattern.MULTILINE | Pattern.DOTALL);

    private static final String REGEX_SEQUENCE = "^CREATE SEQUENCE \\\"(?<owner>.*?)\\\"\\.\\\"(?<n>.*?)\\\"\\sMINVALUE\\s(?<min>\\d+) MAXVALUE (?<max>\\d+) INCREMENT BY (?<step>\\d+) START WITH (?<start>\\d+)";
    private static final Pattern PATTERN_SEQUENCE = Pattern.compile(REGEX_SEQUENCE);//, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * <pre>
     * LOAD TABLE "dba"."TABLE_NAME" ("COL_NAME_1","COL_NAME_2","COL_NAME_3","COL_NAME_4","COL_NAME_5")
     *     FROM 'C:/projects/db/unloaded-example/unload/3037.dat'
     *     FORMAT 'TEXT' QUOTES ON
     *     ORDER OFF ESCAPES ON
     *     CHECK CONSTRAINTS OFF COMPUTES OFF
     *     STRIP OFF DELIMITED BY ','
     *     ENCODING 'windows-1252'
     * go
     * </pre>
     */
    private static final String REGEX_LOAD_TABLE = "^LOAD TABLE \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\" \\n?\\((?<cols>.*?)\\)\\n?\\s{4}FROM\\s\\'(?<file>.*?)\\'";
    private static final Pattern PATTERN_LOAD_TABLE = Pattern.compile(REGEX_LOAD_TABLE, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * <pre>
     * CREATE INDEX "Idx_NAME" ON "dba"."TABLE_NAME"
     *     ( "FT_Created" DESC,"FT_TransactionId" DESC )
     * go
     * </pre>
     *
     * ...or...
     *
     * <pre>
     * CREATE UNIQUE INDEX "INDEX_NAME" ON "dba"."TABLE_NAME"
     *     ( "COL_NAME" )
     * go
     * </pre>
     */
    private static final String REGEX_CREATE_INDEX = "^CREATE (?<u>UNIQUE)?\\s?INDEX \\\"(?<n>.*?)\\\" ON \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\n\\s{4}\\((?<cols>.*?)\\)$";
    private static final Pattern PATTERN_CREATE_INDEX = Pattern.compile(REGEX_CREATE_INDEX, Pattern.MULTILINE | Pattern.DOTALL);
    /**
     * COMMENT ON INDEX "dba"."TABLE_NAME"."Idx_NAME" IS
     * 	'Some nice text'
     * go
     */
    private static final String REGEX_COMMENT_INDEX = "^COMMENT ON INDEX \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\".\\\"(?<n>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    private static final Pattern PATTERN_COMMENT_INDEX = Pattern.compile(REGEX_COMMENT_INDEX, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * ALTER TABLE "dba"."Table_Name"
     *     ADD FOREIGN KEY "FK_NAME" ("BFCS_Id" ASC)
     *     REFERENCES "dba"."REF_TABLE_NAME" ("BFCS_Id")
     *
     * go
     */
    private static final String REGEX_CREATE_FK = "";
    private static final Pattern PATTERN_CREATE_FK = Pattern.compile(REGEX_CREATE_FK, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * <pre>
     * call dbo.sa_reset_identity('TABLE_NAME', 'dba', 10001);
     * go
     * </pre>
     */
    private static final String REGEX_RESET_IDENTITY = "^call dbo.sa_reset_identity\\('(?<tn>.*?)', '(?<owner>.*?)', (?<nv>\\d*?)\\);";
    private static final Pattern PATTERN_RESET_IDENTITY = Pattern.compile(REGEX_RESET_IDENTITY);

    @Override
    @NotNull
    public DbSystem importDatabase(String[] args) throws AppRuntimeException {
        try {
            Path file = Paths.get(args.length != 0 ? args[0] : Configuration.getString(Configuration.SOURCE_FILE));

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
            throw new AppRuntimeException("args=" + Arrays.toString(args), ex);
        }
    }

    private Stream<String> streamGoSections(@NotNull String reloadScript) {
        Matcher matcher = PATTERN_GO_SECTIONS.matcher(reloadScript);

        LOGGER.info("Parsing reload script for GO section...");
        List<String> gos = new ArrayList<>();

        while (matcher.find()) {
            String section = matcher.group(0);
            LOGGER.trace("GO found: {}", section);
            gos.add(section);
        }

        LOGGER.debug("{} GO sections found.", gos.size());

        return gos.stream();
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

    private void processSequence(@NotNull String section, @NotNull DbSystem system) {
        LOGGER.trace("Processing sequence: {}", section);


        Matcher matcher = PATTERN_SEQUENCE.matcher(section);
        if (matcher.find()) {
            DbSequence sequence = new DbSequence();

            sequence.owner = matcher.group("owner");
            sequence.name = matcher.group("n");
            sequence.minimum = Long.getLong(matcher.group("min"));
            sequence.maximum = Long.getLong(matcher.group("max"));
            sequence.increment = Integer.getInteger(matcher.group("step"));
            sequence.startWith = Long.getLong(matcher.group("start"));
            // TODO NO Cycle and Cache size

            LOGGER.debug("Sequence '{}' found", sequence.name);
            system.sequences.put(sequence.name, sequence);
        }
    }

    private void processCreateTable(@NotNull String section, @NotNull DbSystem system) {
        LOGGER.trace("Processing create table: {}", section);

        DbTable table = new DbTable();

        Matcher matcher = PATTERN_TABLE_NAME.matcher(section);
        if (matcher.find()) {
            table.owner = matcher.group("o");
            table.name = matcher.group("tn");
            LOGGER.debug("Table '{}' found", table.name);
        } else {
            throw new AppRuntimeException("Table name not found: " + section);
        }

        system.tables.put(table.name, table);

        processCreateColumnsOfTable(table, section);

        findPrimaryKey(section).ifPresent(s -> table.columns.get(s).primaryKey = true);
    }

    private void processCreateColumnsOfTable(@NotNull DbTable table, @NotNull String createTableSql) {
        Pattern widthScalePattern = Pattern.compile("\\((?<w>\\d*)(\\sCHAR)?,?(?<s>\\d*)?\\)$");

        int index = 0;

        Matcher matcher = PATTERN_TABLE_COLUMNS.matcher(createTableSql);
        while (matcher.find()) {
            String datatype = matcher.group("datatype");
            DbColumn column = new DbColumn();
            column.index = index++;
            column.name = matcher.group("cn");
            column.datatype = mapToDbDataType(datatype);
            column.nullable = !"NOT NULL".equalsIgnoreCase(matcher.group("nullable"));
            column.defaultValue = StringUtils.isBlank(matcher.group("def")) || "autoincrement".equalsIgnoreCase(matcher.group("dv")) ? null : matcher.group("dv");
            column.autoinc = "autoincrement".equalsIgnoreCase(matcher.group("dv"));

            Matcher m2 = widthScalePattern.matcher(datatype);
            if (m2.find()) {
                column.width = Integer.parseInt(m2.group("w"));
                column.scale = "".equals(m2.group("s")) ? null : Integer.parseInt(m2.group("s"));
            }

            table.columns.put(column.name, column);
        }

        Matcher m1 = PATTERN_TABLE_PK.matcher(createTableSql);
        if (m1.find()) {
            Optional.ofNullable(table.columns.get(m1.group(1))).ifPresent(c -> c.primaryKey = true);
        }
    }

    private void processColumnComment(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_COLUMN_COMMENT.matcher(section);
        while (matcher.find()) {
            String tableName = matcher.group("tn");
            String columnName = matcher.group("cn");
            String comment = matcher.group("comment");
            LOGGER.trace("Getting comment of column {}.{}", tableName, columnName);

            DbColumn column = system.tables.get(tableName).columns.get(columnName);
            column.comment = comment;
        }
    }

    private void processTableComment(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_TABLE_COMMENT.matcher(section);
        while (matcher.find()) {
            String tableName = matcher.group("tn");
            try {
                LOGGER.trace("Getting comment of table {}", tableName);

                String comment = matcher.group("comment");

                DbTable table = system.tables.get(tableName);
                table.comment = comment;
            } catch (Exception ex) {
                LOGGER.error("Unable to parse comment for table '{}'", tableName);
                throw ex;
            }
        }
    }

    private void processCreateIndex(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_CREATE_INDEX.matcher(section);
        if (matcher.find()) {
            String owner = matcher.group("owner");
            String tableName = matcher.group("tn");
            String indexName = matcher.group("n");
            String cols = matcher.group("cols");
            boolean unique = "UNIQUE".equalsIgnoreCase(matcher.group("u"));
            List<Pair<String, Boolean>> columnNames = Arrays
                    .stream(cols.split(","))
                    .map(c -> c.replace("\"", "").trim())
                    .map(c -> Pair.of(c.split(" ")[0].trim(), c.endsWith(" DESC")))
                    .toList();
            try {
                LOGGER.trace("Getting comment of table index {}.{}", tableName, indexName);

                DbIndex index = new DbIndex();
                index.indexName = indexName;
                index.owner = owner;
                index.tableName = tableName;
                index.unique = unique;
                index.columns.addAll(columnNames);

                system.indexes.put(indexName, index);
            } catch (Exception ex) {
                LOGGER.error("Unable to parse index '{}.{}'", tableName, indexName);
                throw ex;
            }
        }
    }

    private void processIndexComment(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_COMMENT_INDEX.matcher(section);
        if (matcher.find()) {
            String owner = matcher.group("owner");
            String tableName = matcher.group("tn");
            String indexName = matcher.group("n");
            String comment = matcher.group("comment");
            try {
                LOGGER.trace("Getting comment of table index {}.{}", tableName, indexName);

                DbIndexComment indexComment = new DbIndexComment();
                indexComment.tableName = tableName;
                indexComment.indexName = indexName;
                indexComment.owner = owner;
                indexComment.comment = comment;

                system.indexComments.put(indexName, indexComment);
            } catch (Exception ex) {
                LOGGER.error("Unable to parse comment for table index '{}.{}'. Original SQL=\n{}", tableName, indexName, section);
                throw ex;
            }
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

    /**
     * ALTER TABLE "dba"."KIS_MRG"
     *     ADD CONSTRAINT "KIS_MRG UNIQUE PAT_FID_OLD" UNIQUE ( "PAT_FID_OLD","KIS_SYSTEM","MAN_ID" )
     * go
     * @param section SQL GO section from reload file
     * @param system Db system DTO
     */
    private void processTableConstraint(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_ALTER_COLUMN_UNIQUE.matcher(section);
        if (matcher.find()) {
            try {
                String name = "".equals(matcher.group("name")) ? null : matcher.group("name").replace("\"", "");
                String tableName = matcher.group("tn");
                Set<String> columnNames = Arrays.stream(matcher.group("cn").split(",")).map(c -> c.replace("\"", "")).collect(Collectors.toSet());
                LOGGER.trace("Getting comment of table {}", tableName);

                DbTableConstraint constraint = new DbTableConstraint();
                constraint.name = name;
                constraint.columns.addAll(columnNames);

                system.tables.get(tableName).constraints.add(constraint);
            } catch (Exception ex) {
                LOGGER.error("Unable to parse section '{}'", section);
                throw ex;
            }
        }
    }

    private void processResetIdentity(@NotNull String section, @NotNull DbSystem system) {
        Matcher matcher = PATTERN_RESET_IDENTITY.matcher(section);
        if (matcher.find()) {
            String tableName = matcher.group("tn");
            String nextValue = matcher.group("nv");
            try {
                LOGGER.trace("Getting next value of PK of table index {}", tableName);

                system.tables
                        .get(tableName)
                        .columns
                        .values()
                        .stream()
                        .filter(c -> c.autoinc)
                        .findFirst()
                        .orElseThrow(() -> new AppRuntimeException("Unable to find table '" + tableName + "'or his autoinc column of table '"))
                        .nextValue = Long.parseLong(nextValue);
            } catch (Exception ex) {
                LOGGER.error("Unable to parse 'reset identity' for table '{}'. Original SQL=\n{}", tableName, section);
                throw ex;
            }
        }
    }

    private Optional<String> findPrimaryKey(@NotNull String createTable) {
        Matcher m1 = PATTERN_TABLE_PK.matcher(createTable);
        return m1.find() ? Optional.ofNullable(m1.group(1)) : Optional.empty();
    }

}
