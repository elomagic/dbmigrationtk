package de.elomagic.dto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DbTable {

    public int id;
    public String name;
    public String owner;
    public String comment;
    public DbTableContent content;

    public final Map<String, DbColumn> columns = new HashMap<>();
    public final Set<DbTableConstraint> constraints = new HashSet<>();

    public int getId() {
        return id;
    }

}
