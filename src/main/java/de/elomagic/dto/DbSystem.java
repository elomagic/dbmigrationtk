package de.elomagic.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbSystem {

    /**
     * Key = Name of the database table
     */
    public final Map<String, DbTable> tables = new HashMap<>();
    /**
     * Key = Name of the sequence
     */
    public final Map<String, DbSequence> sequences = new HashMap<>();
    /**
     * Key = Name of the foreign key
     */
    public final List<DbForeignKey> foreignKeys = new ArrayList<>();
    /**
     * Key = Name of the index
     * TODO Could be a bug because index name not unique system wide. Have to check
     */
    public final Map<String, DbIndex> indexes = new HashMap<>();
    /**
     * Key = Name of the index
     * TODO Could be a bug because index name not unique system wide. Have to check
     */
    public final Map<String, DbIndexComment> indexComments = new HashMap<>();

}
