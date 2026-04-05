#!/usr/bin/env bash
# install-arrow-extension.sh
#
# NOTE: This script is no longer required.
#
# Arrow export and import now use DuckDB's built-in C Data Interface
# (DuckDBResultSet.arrowExportStream / registerArrowStream) which is
# part of the DuckDB JDBC driver itself — no community extension needed.
# This works fully in air-gapped environments without internet access.
#
# The script is kept for reference only. It previously downloaded the
# DuckDB Arrow community extension for COPY TO (FORMAT arrow) / read_arrow().

echo "INFO: The DuckDB Arrow community extension is no longer required."
echo "      Arrow export/load now uses DuckDB's built-in C Data Interface."
echo "      No installation step needed — tests run directly with:"
echo ""
echo "      MAVEN_OPTS=\"--add-opens=java.base/java.nio=ALL-UNNAMED -Xmx4g\" \\"
echo "        mvn test -pl pgw-ingestor,pgw-validator"
