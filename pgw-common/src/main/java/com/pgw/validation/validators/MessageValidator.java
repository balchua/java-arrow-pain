package com.pgw.validation.validators;

import com.pgw.dal.PaymentRepository;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Validates message-level fields via SQL through the {@link PaymentRepository}.
 */
public final class MessageValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(MessageValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            List<PaymentRepository.Issue> issues = repository.validateMessageFields();
            for (PaymentRepository.Issue issue : issues) {
                if (issue.message().startsWith("WARN:")) {
                    context.addWarning(getName(), issue.message().substring(5), issue.id());
                } else {
                    context.addError(getName(), issue.message(), issue.id());
                }
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
        return "MessageValidator";
    }
}
