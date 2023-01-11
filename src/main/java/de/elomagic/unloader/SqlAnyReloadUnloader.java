package de.elomagic.unloader;

import java.util.regex.Pattern;

/**
 * Doesn't work well because of an RegEx issue?
 */
public interface SqlAnyReloadUnloader extends SqlAnyUnloader {

    String REGEX_GO_SECTIONS = "(^\\w.*?go\\n\\n)";
    Pattern PATTERN_GO_SECTIONS = Pattern.compile(REGEX_GO_SECTIONS, Pattern.MULTILINE | Pattern.DOTALL);

    String REGEX_TABLE_SQL = "^(?<sql>CREATE TABLE.*?\\))(\\sIN\\s\\\".*\\\")?\\ngo";
    Pattern PATTERN_TABLE_SQL = Pattern.compile(REGEX_TABLE_SQL, Pattern.MULTILINE | Pattern.DOTALL);
    String REGEX_TABLE_NAME = "(CREATE TABLE \\\"(?<o>.*)\\\"\\.\\\")(?<tn>.*)(\\\")";
    Pattern PATTERN_TABLE_NAME = Pattern.compile(REGEX_TABLE_NAME, Pattern.MULTILINE);
    // ^   [\s|,]\"(?<column>\S*?)\"\s+(?<datatype>.*?)\s(?<nullable>NOT NULL?|NULL)(?<rest>.*?)$
    String REGEX_TABLE_COLUMNS = "^   [\\s|,]\\\"(?<cn>\\S*?)\\\"\\s+(?<datatype>.*?)\\s(?<nullable>NOT NULL?|NULL)(?<def> DEFAULT )?(?<dv>.*?)$";
    Pattern PATTERN_TABLE_COLUMNS = Pattern.compile(REGEX_TABLE_COLUMNS, Pattern.MULTILINE);
    static final String REGEX_TABLE_PK = "^   ,PRIMARY KEY \\(\\\"(?<column>.*)\\\"\\s+(?<order>ASC|DESC)\\)";
    Pattern PATTERN_TABLE_PK = Pattern.compile(REGEX_TABLE_PK, Pattern.MULTILINE);
    String REGEX_TABLE_COMMENT = "^COMMENT ON TABLE \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    Pattern PATTERN_TABLE_COMMENT = Pattern.compile(REGEX_TABLE_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);
    String REGEX_COLUMN_COMMENT = "^COMMENT ON COLUMN \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\.\\\"(?<cn>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    Pattern PATTERN_COLUMN_COMMENT = Pattern.compile(REGEX_COLUMN_COMMENT, Pattern.MULTILINE | Pattern.DOTALL);
    // ^ALTER TABLE \"(?<o>.*)\".\"(?<tn>.*)\"\n\s{4}ADD( CONSTRAINT )?\"?(?<n>.*)\"? UNIQUE \(\s(?<cn>\".*\")\s\)
    // ALTER TABLE \"(?<o>.*)\".\"(?<tn>.*)\"\n\s{4}ADD UNIQUE \(\s\"(?<cn>.*)\"\s\)
    String REGEX_ALTER_COLUMN_UNIQUE = "^ALTER TABLE \\\"(?<o>.*)\\\".\\\"(?<tn>.*)\\\"\\n\\s{4}ADD( CONSTRAINT )?\\\"?(?<name>.*)\\\"? UNIQUE \\(\\s(?<cn>\\\".*\\\")\\s\\)";
    Pattern PATTERN_ALTER_COLUMN_UNIQUE = Pattern.compile(REGEX_ALTER_COLUMN_UNIQUE);//, Pattern.MULTILINE | Pattern.DOTALL);

    String REGEX_SEQUENCE = "^CREATE SEQUENCE \\\"(?<owner>.*?)\\\"\\.\\\"(?<n>.*?)\\\"\\sMINVALUE\\s(?<min>\\d+) MAXVALUE (?<max>\\d+) INCREMENT BY (?<step>\\d+) START WITH (?<start>\\d+)";
    Pattern PATTERN_SEQUENCE = Pattern.compile(REGEX_SEQUENCE);//, Pattern.MULTILINE | Pattern.DOTALL);

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
    String REGEX_LOAD_TABLE = "^LOAD TABLE \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\" \\n?\\((?<cols>.*?)\\)\\n?\\s{4}FROM\\s\\'(?<file>.*?)\\'";
    Pattern PATTERN_LOAD_TABLE = Pattern.compile(REGEX_LOAD_TABLE, Pattern.MULTILINE | Pattern.DOTALL);

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
    String REGEX_CREATE_INDEX = "^CREATE (?<u>UNIQUE)?\\s?INDEX \\\"(?<n>.*?)\\\" ON \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\"\\n\\s{4}\\((?<cols>.*?)\\)$";
    Pattern PATTERN_CREATE_INDEX = Pattern.compile(REGEX_CREATE_INDEX, Pattern.MULTILINE | Pattern.DOTALL);
    /**
     * COMMENT ON INDEX "dba"."TABLE_NAME"."Idx_NAME" IS
     * 	'Some nice text'
     * go
     */
    String REGEX_COMMENT_INDEX = "^COMMENT ON INDEX \\\"(?<owner>.*?)\\\"\\.\\\"(?<tn>.*?)\\\".\\\"(?<n>.*?)\\\"\\sIS\\s\\n\\t'(?<comment>.*?)\\'$";
    Pattern PATTERN_COMMENT_INDEX = Pattern.compile(REGEX_COMMENT_INDEX, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * ALTER TABLE "dba"."Table_Name"
     *     ADD FOREIGN KEY "FK_NAME" ("BFCS_Id" ASC)
     *     REFERENCES "dba"."REF_TABLE_NAME" ("BFCS_Id")
     *
     * go
     *
     *
     * ^ALTER TABLE \"(?<o>.*)\".\"(?<tn>.*)\"\n\s{4}ADD (NOT NULL )?FOREIGN KEY \"(?<name>.*)\" \((?<cns>.*)\)\n\s{4}REFERENCES \"(?<ro>.*)\".\"(?<rtn>.*)\" \((?<rcns>.*)\)\n\s{4}(?<oua>ON UPDATE\s(SET NULL|CASCADE))\s?(?<oda>ON DELETE\s(SET NULL|CASCADE))?
     */
    String REGEX_CREATE_FK = "^ALTER TABLE \\\"(?<o>.*)\\\".\\\"(?<tn>.*)\\\"\\n\\s{4}ADD (NOT NULL )?FOREIGN KEY \\\"(?<name>.*)\\\" \\((?<cns>.*)\\)\\n\\s{4}REFERENCES \\\"(?<ro>.*)\\\".\\\"(?<rtn>.*)\\\" \\((?<rcns>.*)\\)\\n\\s{4}(?<oua>ON UPDATE\\s(SET NULL|CASCADE))\\s?(?<oda>ON DELETE\\s(SET NULL|CASCADE))?";
    Pattern PATTERN_CREATE_FK = Pattern.compile(REGEX_CREATE_FK, Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * <pre>
     * call dbo.sa_reset_identity('TABLE_NAME', 'dba', 10001);
     * go
     * </pre>
     */
    String REGEX_RESET_IDENTITY = "^call dbo.sa_reset_identity\\('(?<tn>.*?)', '(?<owner>.*?)', (?<nv>\\d*?)\\);";
    Pattern PATTERN_RESET_IDENTITY = Pattern.compile(REGEX_RESET_IDENTITY);

}
