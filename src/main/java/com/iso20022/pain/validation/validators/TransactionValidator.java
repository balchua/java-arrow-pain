package com.iso20022.pain.validation.validators;

import com.iso20022.pain.dal.Pain001Repository;
import com.iso20022.pain.dal.Pain001Repository.Issue;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Validates transaction-level fields via SQL through the {@link Pain001Repository}.
 *
 * <p>Checks:</p>
 * <ul>
 *   <li>Instructed amounts are positive</li>
 *   <li>Creditor name is required</li>
 * </ul>
 *
 * <p>This validator is parallelizable.</p>
 */
public final class TransactionValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidator.class);

    @Override
    public void validate(Pain001Repository repository, ValidationContext context) {
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
        return "TransactionValidator";
    }
}
