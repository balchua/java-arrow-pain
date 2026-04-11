package com.pgw.validation.validators;

import com.pgw.dal.PaymentRepository;
import com.pgw.domain.model.Transaction;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validates all transaction records by streaming each row individually from DuckDB
 * and mapping it to a {@link Transaction} domain object.
 *
 * <p>For each transaction retrieved, asserts that the instructed amount is greater
 * than zero. This simulates a real-world validation pattern where each record is
 * inspected individually — for example, by calling an external API per row — and
 * intentionally avoids SQL aggregation so that the full cost of row-by-row streaming
 * through an OLAP database like DuckDB can be measured.</p>
 *
 * <p>This validator is marked non-parallelizable because it performs a sequential
 * full-table scan; running it concurrently with other validators would distort the
 * timing measurement.</p>
 */
public final class StreamingTransactionIteratorValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingTransactionIteratorValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        AtomicLong rowCount = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);
        try {
            repository.streamAllTransactions(tx -> {
                rowCount.incrementAndGet();
                if (tx.instructedAmount() == null
                        || tx.instructedAmount().compareTo(BigDecimal.ZERO) <= 0) {
                    errorCount.incrementAndGet();
                    context.addError(getName(),
                            "Transaction amount must be greater than 0",
                            tx.endToEndId());
                }
            });
            LOG.debug("{} completed: {} row(s) inspected, {} error(s)",
                    getName(), rowCount.get(), errorCount.get());
        } catch (SQLException e) {
            LOG.error("{} failed: {}", getName(), e.getMessage(), e);
            context.addError(getName(), "Streaming iteration failed", e.getMessage());
        }
    }

    /**
     * Returns {@code false} so that this validator always runs sequentially.
     *
     * <p>Sequential execution is intentional: the purpose of this validator is to
     * measure the isolated cost of a full-table streaming scan. Running it in
     * parallel with other validators would invalidate the timing measurement.</p>
     */
    @Override
    public boolean isParallelizable() {
        return false;
    }

    @Override
    public String getName() {
        return "StreamingTransactionIteratorValidator";
    }
}
