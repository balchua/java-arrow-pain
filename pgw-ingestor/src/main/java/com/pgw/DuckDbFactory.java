package com.pgw;

import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Factory for DuckDB connections with the Arrow community extension pre-loaded.
 *
 * <p>DuckDB's {@code COPY TO (FORMAT arrow)} and {@code read_arrow()} functions
 * require the Arrow community extension to be installed locally before running.
 * Use {@code install-arrow-extension.sh} (project root) to download it once.</p>
 *
 * <h3>Air-gapped environments</h3>
 * <p>Download {@code arrow.duckdb_extension} on a machine with internet access and
 * copy it to {@code ~/.duckdb/extensions/v1.4.4/<platform>/arrow.duckdb_extension}
 * on the target machine.  Alternatively set the {@code DUCKDB_EXTENSION_DIR}
 * environment variable to point to a directory that already contains the file.</p>
 */
public final class DuckDbFactory {

    /**
     * Optional env var override for the DuckDB extension directory.
     * If set, DuckDB will search this directory instead of the default
     * {@code ~/.duckdb/extensions/} tree.
     */
    static final String DUCKDB_EXTENSION_DIR_ENV = "DUCKDB_EXTENSION_DIR";

    private DuckDbFactory() {}

    /**
     * Opens a new in-process DuckDB connection and loads the Arrow extension.
     *
     * <p>If the {@code DUCKDB_EXTENSION_DIR} environment variable is set its value
     * is used as the DuckDB extension search directory; otherwise the default
     * {@code ~/.duckdb/extensions/} tree is used.</p>
     *
     * @return a ready-to-use {@link DuckDBConnection} with Arrow support enabled
     * @throws SQLException if the extension is not found — run
     *                      {@code install-arrow-extension.sh} first
     */
    public static DuckDBConnection newConnection() throws SQLException {
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        try (var stmt = conn.createStatement()) {
            String extDir = System.getenv(DUCKDB_EXTENSION_DIR_ENV);
            if (extDir != null && !extDir.isBlank()) {
                stmt.execute("SET extension_directory = '" + extDir.replace("'", "''") + "'");
            }
            try {
                stmt.execute("LOAD arrow");
            } catch (SQLException e) {
                throw new SQLException(
                    "DuckDB arrow extension not found. "
                    + "Run install-arrow-extension.sh to install it, or set "
                    + DUCKDB_EXTENSION_DIR_ENV + " to the directory containing "
                    + "arrow.duckdb_extension. Original error: " + e.getMessage(), e);
            }
        } catch (SQLException e) {
            try { conn.close(); } catch (SQLException ex) { e.addSuppressed(ex); }
            throw e;
        }
        return conn;
    }
}
