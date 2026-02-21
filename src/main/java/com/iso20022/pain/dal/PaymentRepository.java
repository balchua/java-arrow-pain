package com.iso20022.pain.dal;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

/**
 * Contract for SQL-based access to the three pain.001 Arrow tables
 * (message, remittance, transactions) loaded into an in-process DuckDB instance.
 */
public interface PaymentRepository extends AutoCloseable {

    record Issue(String id, String message) {}

    List<Issue> validateMessageFields()    throws SQLException;
    List<Issue> validateRemittanceFields() throws SQLException;
    List<Issue> validateTransactionFields() throws SQLException;
    List<Issue> validateControlSums()       throws SQLException;

    List<String> findInvalidIbans()         throws SQLException;
    BigDecimal   sumTransactionsByRemittance(String pmtInfId) throws SQLException;
    String       getMessageSummary()        throws SQLException;
    long         getRemittanceCount()       throws SQLException;
    long         getTransactionCount()      throws SQLException;
    BigDecimal   getTotalTransactionAmount() throws SQLException;

    @Override
    void close() throws Exception;
}
