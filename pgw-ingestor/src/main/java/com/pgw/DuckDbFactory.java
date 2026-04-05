package com.pgw;

import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Factory for DuckDB connections with the Arrow community extension pre-loaded.
 *
 * <p>DuckDB's {@code COPY TO (FORMAT arrow)} and {@code read_arrow()} functions
 * are provided by the DuckDB Arrow community extension. This factory installs and
 * loads that extension for every new connection, so callers can use Arrow export
 * and import without extra setup.</p>
 *
 * <p>On the first call the extension is downloaded from the DuckDB community
 * extension repository (requires internet access). Subsequent calls re-use the
 * locally cached copy.</p>
 */
public final class DuckDbFactory {

    private DuckDbFactory() {}

    /**
     * Opens a new in-process DuckDB connection and loads the Arrow community extension.
     *
     * @return a ready-to-use {@link DuckDBConnection} with Arrow support enabled
     * @throws SQLException if the connection cannot be created or the Arrow extension
     *                      cannot be installed / loaded
     */
    public static DuckDBConnection newConnection() throws SQLException {
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (var stmt = conn.createStatement()) {
            stmt.execute("SET autoinstall_known_extensions = true");
            stmt.execute("SET autoload_known_extensions = true");
            stmt.execute("LOAD arrow");
        } catch (SQLException e) {
            try {
                conn.close();
            } catch (SQLException ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
        return conn;
    }
}
