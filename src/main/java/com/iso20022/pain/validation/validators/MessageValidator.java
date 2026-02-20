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
 * Validates message-level fields via SQL through the {@link Pain001Repository}.
 *
 * <p>Checks:</p>
 * <ul>
 *   <li>MsgId length &le; 35 characters</li>
 *   <li>Warns if InitgPty name is missing</li>
 *   <li>Errors if CreDtTm is missing</li>
 * </ul>
 *
 * <p>This validator is parallelizable.</p>
 */
public final class MessageValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(MessageValidator.class);

    @Override
    public void validate(Pain001Repository repository, ValidationContext context) {
        try {
            List<Issue> issues = repository.validateMessageFields();
            for (Issue issue : issues) {
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
