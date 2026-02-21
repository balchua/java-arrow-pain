package com.iso20022.pain.validation.validators;

import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Validates ISO 20022 control sums via SQL through the {@link PaymentRepository}.
 */
public final class ControlSumValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(ControlSumValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            List<PaymentRepository.Issue> issues = repository.validateControlSums();
            for (PaymentRepository.Issue issue : issues) {
                context.addError(getName(), issue.message(), issue.id());
            }
            if (issues.isEmpty()) {
                LOG.info("  ✓ All control sums valid");
            } else {
                LOG.error("  ✗ {} control sum error(s) found", issues.size());
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
        return "ControlSumValidator";
    }
}
