package de.elomagic.loader;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbColumn;
import de.elomagic.dto.DbSystem;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Writer;

public interface SchemaLoader {

    void export(@NotNull DbSystem system, @NotNull Writer writer) throws AppRuntimeException;

    @Nullable
    String denormalizeValue(@Nullable String rawValue, @NotNull DbColumn column);

}
