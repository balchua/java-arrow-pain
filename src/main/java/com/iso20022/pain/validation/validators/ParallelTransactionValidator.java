package com.iso20022.pain.validation.validators;

import com.iso20022.pain.dal.Pain001Repository;
import com.iso20022.pain.dal.Pain001Repository.Issue;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.VirtualThreadValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Validates transaction-level fields via SQL using a virtual thread,
 * delegating to the same SQL logic as {@link TransactionValidator}.
 *
 * <p>DuckDB's vectorised engine handles internal parallelism; this class
 * demonstrates running the validation task on a virtual thread.</p>
 */
public final class ParallelTransactionValidator extends VirtualThreadValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelTransactionValidator.class);

    @Override
    protected void doValidate(Pain001Repository repository, ValidationContext context) {
        try {
            List<Issue> issues = repository.validateTransactionFields();
            for (Issue issue : issues) {
                context.addError(getName(), issue.message(), issue.id());
            }
            LOG.debug("{} completed: {} issue(s)", getName(), issues.size());
        } catch (SQLException e) {
            LOG.error("{} failed: {}", getName(), e.getMessage(), e);
            context.addError(getName(), "SQL validation failed", e.getMessage());
        }
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "ParallelTransactionValidator";
    }
}
