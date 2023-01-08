package de.elomagic.dto;

public class DbColumn {

    public int index;
    public String name;
    public boolean primaryKey;
    public DbDataType datatype;
    public String defaultValue;
    public boolean autoinc;
    public boolean nullable;
    public String comment;
    public Integer width;
    public Integer scale;
    public Long nextValue;

    public int getIndex() {
        return index;
    }

}
