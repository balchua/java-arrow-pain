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
 * Validates ISO 20022 control sums via SQL through the {@link Pain001Repository}.
 *
 * <h3>Validation rules</h3>
 * <ol>
 * <li><b>Remittance CtrlSum</b> — for each remittance (PmtInf), the declared
 * {@code ctrl_sum} must equal the sum of all transaction {@code instd_amt}
 * rows that share the same {@code pmt_inf_id}.</li>
 * <li><b>Message CtrlSum</b> — the declared message-level {@code msg_ctrl_sum}
 * must equal the grand total of all transaction amounts.</li>
 * </ol>
 *
 * <p>This validator is not parallelizable as it must run after other validators
 * complete.</p>
 */
public final class ControlSumValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(ControlSumValidator.class);

    @Override
    public void validate(Pain001Repository repository, ValidationContext context) {
        try {
            List<Issue> issues = repository.validateControlSums();
            for (Issue issue : issues) {
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
