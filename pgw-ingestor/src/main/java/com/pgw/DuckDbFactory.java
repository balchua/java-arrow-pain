package com.pgw;

import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Factory for in-process DuckDB connections.
 *
 * <p>Arrow export and import are handled by {@link ArrowIpc} which uses DuckDB's
 * built-in Arrow C Data Interface ({@link DuckDBConnection#registerArrowStream} /
 * {@code DuckDBResultSet.arrowExportStream}) — no DuckDB arrow community extension
 * is required.</p>
 */
public final class DuckDbFactory {

    private DuckDbFactory() {}

    /**
     * Opens a new in-process DuckDB connection.
     *
     * @return a ready-to-use {@link DuckDBConnection}
     * @throws SQLException if the connection cannot be opened
     */
    public static DuckDBConnection newConnection() throws SQLException {
        return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    }
}
