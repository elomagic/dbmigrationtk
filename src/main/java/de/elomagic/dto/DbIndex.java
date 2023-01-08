package de.elomagic.dto;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public class DbIndex {

    public String owner;
    public String tableName;
    public String indexName;
    public boolean unique;
    /**
     * Pair[Column name, Sort (DESC=true)]
     */
    public final List<Pair<String, Boolean>> columns = new ArrayList<>();

}
