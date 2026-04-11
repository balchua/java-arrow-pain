package com.pgw.validation.validators;

import com.pgw.dal.PaymentRepository;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Validates declared message and remittance transaction counts via the
 * {@link PaymentRepository}.
 */
public final class NumberOfTransactionsValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(NumberOfTransactionsValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            List<PaymentRepository.Issue> issues = repository.validateNumberOfTransactions();
            for (PaymentRepository.Issue issue : issues) {
                context.addError(getName(), issue.message(), issue.id());
            }
            if (issues.isEmpty()) {
                LOG.info("  ✓ All transaction counts valid");
            } else {
                LOG.error("  ✗ {} transaction count error(s) found", issues.size());
            }
            LOG.debug("{} completed: {} issue(s)", getName(), issues.size());
        } catch (SQLException e) {
            LOG.error("{} failed: {}", getName(), e.getMessage(), e);
            context.addError(getName(), "SQL validation failed", e.getMessage());
        }
    }

    @Override
    public boolean isParallelizable() {
        return false;
    }

    @Override
    public String getName() {
        return "NumberOfTransactionsValidator";
    }
}