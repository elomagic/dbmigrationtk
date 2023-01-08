package de.elomagic.importer;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbSystem;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SchemaImporter {

    @NotNull DbSystem importDatabase(String[] args) throws AppRuntimeException;

}
