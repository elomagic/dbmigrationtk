package de.elomagic.dto;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DbTableContent {

    public Path file;
    public final List<String> columns = new ArrayList<>();
    public Charset encoding;

}
