package de.elomagic.dto;

import java.util.HashSet;
import java.util.Set;

public class DbTableConstraint {

    public String name;
    public final Set<String> columns = new HashSet<>();

}
