package de.elomagic.unloader;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbSystem;
import de.elomagic.loader.SchemaLoader;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SchemaImporter {

    @NotNull DbSystem importDatabase(@NotNull SchemaLoader targetLoader) throws AppRuntimeException;

}
