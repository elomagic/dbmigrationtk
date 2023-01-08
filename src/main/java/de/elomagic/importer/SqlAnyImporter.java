package de.elomagic.importer;

import de.elomagic.AppRuntimeException;
import de.elomagic.dto.DbDataType;

import org.jetbrains.annotations.NotNull;

public interface SqlAnyImporter extends SchemaImporter {

    default DbDataType mapToDbDataType(@NotNull String datatype) {
        return switch (datatype) {
            case "bit" -> DbDataType.BIT;
            case "integer" -> DbDataType.INTEGER;
            case "char" -> DbDataType.CHAR;
            case "varchar" -> DbDataType.VARCHAR;
            case "binary", "long binary" -> DbDataType.LONG_BINARY;
            case "long varchar" -> DbDataType.LONG_VARCHAR;
            case "numeric" -> DbDataType.NUMERIC;
            case "timestamp" -> DbDataType.TIMESTAMP;
            case "date" -> DbDataType.DATE;
            case "datetime", "\"datetime\"" -> DbDataType.DATETIME;
            case "smallint" -> DbDataType.SMALLINT;
            case "bigint" -> DbDataType.BIGINT;
            case "decimal" -> DbDataType.DECIMAL;
            case "tinyint" -> DbDataType.TINYINT;
            case "unsigned int" -> DbDataType.UNSIGNED_INT;
            case "unsigned smallint" -> DbDataType.UNSIGNED_SMALLINT;
            case "xml" -> DbDataType.XML;

            default -> {
                if (datatype.startsWith("char(")) {
                    yield DbDataType.CHAR;
                } else if (datatype.startsWith("varchar(")) {
                    yield DbDataType.VARCHAR;
                } else if (datatype.startsWith("numeric(")) {
                    yield DbDataType.NUMERIC;
                } else if (datatype.startsWith("decimal(")) {
                    yield DbDataType.DECIMAL;
                } else if (datatype.startsWith("binary(")) {
                    yield DbDataType.BINARY;
                }

                throw new AppRuntimeException("Unsupported datatype '" + datatype + "'.");
            }
        };
    }


}
