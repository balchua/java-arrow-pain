package com.pgw.dal;

import com.pgw.domain.model.Message;
import com.pgw.domain.model.Remittance;
import com.pgw.domain.model.Transaction;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Contract for SQL-based access to the three pain.001 Arrow tables
 * (message, remittance, transactions) loaded into an in-process DuckDB instance.
 *
 * <p>Streaming methods ({@code streamMessages}, {@code streamRemittances},
 * {@code streamTransactions}) iterate over rows one at a time via a JDBC cursor
 * and invoke the supplied {@link Consumer} for each row. At no point is more
 * than one domain object resident in heap, preventing out-of-memory errors when
 * processing files with millions of transactions.</p>
 */
public interface PaymentRepository extends AutoCloseable {

    record Issue(String id, String message) {}

    // ── SQL-level bulk validators ────────────────────────────────────────────

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

    // ── Streaming domain-object access ───────────────────────────────────────

    /**
     * Streams all {@link Message} rows, invoking {@code handler} once per row.
     * Rows are fetched from DuckDB one at a time; the heap holds at most one
     * {@code Message} instance during iteration.
     *
     * @param handler callback invoked for each message row
     * @throws SQLException if the underlying query fails
     */
    void streamMessages(Consumer<Message> handler) throws SQLException;

    /**
     * Streams all {@link Remittance} rows belonging to the given message,
     * invoking {@code handler} once per row.
     *
     * @param messageId the parent message identifier (foreign key)
     * @param handler   callback invoked for each remittance row
     * @throws SQLException if the underlying query fails
     */
    void streamRemittances(String messageId, Consumer<Remittance> handler) throws SQLException;

    /**
     * Streams all {@link Transaction} rows belonging to the given remittance,
     * invoking {@code handler} once per row.
     *
     * @param remittanceId the parent remittance identifier (foreign key)
     * @param handler      callback invoked for each transaction row
     * @throws SQLException if the underlying query fails
     */
    void streamTransactions(String remittanceId, Consumer<Transaction> handler) throws SQLException;

    /**
     * Streams every {@link Transaction} row in the transactions table,
     * invoking {@code handler} once per row regardless of remittance.
     *
     * <p>Rows are fetched from DuckDB one at a time; the heap holds at most one
     * {@code Transaction} instance during iteration. This is intended for
     * use-cases that must inspect every transaction individually (e.g., calling
     * an external API per record) and want to measure the raw cost of full-table
     * streaming through DuckDB.</p>
     *
     * @param handler callback invoked for each transaction row
     * @throws SQLException if the underlying query fails
     */
    void streamAllTransactions(Consumer<Transaction> handler) throws SQLException;

    @Override
    void close() throws Exception;
}
