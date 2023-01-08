package de.elomagic.exporter;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbSystem;

import org.jetbrains.annotations.NotNull;

import java.io.Writer;

@FunctionalInterface
public interface SchemaExporter {

    void export(@NotNull DbSystem system, @NotNull Writer writer) throws AppRuntimeException;

}
