package de.elomagic.dto;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL Any
 * <code>
 *     ALTER TABLE "dba"."TABLE"
 *         ADD FOREIGN KEY "FK_NAME" ("DOC_ID" ASC)
 *         REFERENCES "dba"."DOC" ("DOC_ID")
 *         ON UPDATE SET NULL ON DELETE SET NULL
 * </code>
 *
 * <code>
 *     ALTER TABLE "dba"."TABLE"
 *         ADD FOREIGN KEY "FK_NAME" ("RECHNUNG_ID" ASC)
 *         REFERENCES "dba"."INVOICE" ("RECHNUNG_ID")
 *         ON UPDATE CASCADE ON DELETE CASCADE
 * go
 * </code>
 */
public class DbForeignKey {

    public enum RefAction {
        NO_ACTION,
        RESTRICT,
        CASCADE,
        SET_NULL
    }

    public String owner;
    public String tableName;
    public String fkName;
    /**
     * Key = Column Name,
     * Value = Sorting (DESC=true)]
     */
    public List<Pair<String, Boolean>> fkColumns = new ArrayList<>();
    public String referenceOwner;
    public String referenceTable;
    /**
     * Item = Reference column name
     */
    public List<String> referenceColumns = new ArrayList<>();
    public RefAction actionOnUpdate = RefAction.NO_ACTION;
    public RefAction actionOnDelete = RefAction.NO_ACTION;

}
