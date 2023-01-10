package de.elomagic;

import de.elomagic.dto.DbSystem;
import de.elomagic.loader.PostgresLoader;
import de.elomagic.loader.SchemaLoader;
import de.elomagic.unloader.SchemaImporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main( String[] args ) {
        try {
            Properties properties = new Properties();
            properties.putAll(readDefaults());
            try (Reader reader = Files.newBufferedReader(Paths.get("application-dev.properties"))) {
                properties.load(reader);
                System.getProperties().putAll(properties);
            }

            App app = new App();
            app.start();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private static Properties readDefaults() throws IOException {
        Properties properties = new Properties();
        try (InputStream in = App.class.getResourceAsStream("/application.properties")) {
            properties.load(in);
        }
        return properties;
    }

    private void start() throws Exception {
        String translatorClassName = Configuration.getString(Configuration.SOURCE_TRANSLATOR);
        LOGGER.info("Using translator {}", translatorClassName);

        SchemaImporter importer = (SchemaImporter)Class
                .forName(translatorClassName)
                .getDeclaredConstructor()
                .newInstance();

        SchemaLoader exporter = new PostgresLoader();

        DbSystem system = importer.importDatabase(exporter);

        try (Writer writer = createWriter()) {
            exporter.export(system, writer);
        }
    }

    @NotNull
    Writer createWriter() throws IOException {
        String s = Configuration.getString(Configuration.TARGET_OUTPUT_PATH);
        return s == null || s.trim().length() == 0 ? createConsoleWriter() : createFileWriter(Paths.get(s));
    }

    @NotNull
    private Writer createFileWriter(@NotNull Path path) throws IOException {
        Path file = path.resolve("reload-postgres.sql");
        LOGGER.info("Creating SQL reload script file '{}'", file);
        return Files.newBufferedWriter(file);
    }

    @NotNull
    private Writer createConsoleWriter() {
        LOGGER.info("SQK script to console");
        return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    }

}