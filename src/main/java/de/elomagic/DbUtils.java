package de.elomagic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public final class DbUtils {

    private static Logger LOGGER = LogManager.getLogger(DbUtils.class);

    private DbUtils() {
    }

    @NotNull
    public static Connection createConnection() throws SQLException {
        String url = Configuration.getString(Configuration.SOURCE_DATABASE_URL);

        LOGGER.info("Connecting to database '{}'", url);

        return DriverManager.getConnection(
                url,
                Configuration.getString(Configuration.SOURCE_USERNAME),
                Configuration.getString(Configuration.SOURCE_PASSWORD));
    }

    @NotNull
    public static PreparedStatement createPrepareStatement(@NotNull Connection con, @NotNull String sql, @NotNull List values) throws SQLException {
        PreparedStatement statement = con.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

        for (int i = 0; i < values.size(); i++) {
            statement.setString(i+1, String.valueOf(values.get(i)));
        }

        return statement;
    }

}
