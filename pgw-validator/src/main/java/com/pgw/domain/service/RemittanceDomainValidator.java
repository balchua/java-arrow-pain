package com.pgw.domain.service;

import com.pgw.dal.PaymentRepository;
import com.pgw.domain.exception.InvalidBicException;
import com.pgw.domain.exception.InvalidIbanException;
import com.pgw.domain.valueobject.Bic;
import com.pgw.domain.valueobject.Iban;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Pure-Java domain validator for ISO 20022 PaymentInformation (PmtInf) fields.
 *
 * <p>Checks performed using domain value objects:
 * <ul>
 *   <li>Debtor IBAN ({@link Iban}) — MOD-97 checksum.</li>
 *   <li>Debtor agent BIC ({@link Bic}) — SWIFT format regex.</li>
 *   <li>{@code requestedExecutionDate} must be non-null.</li>
 * </ul>
 * </p>
 */
public final class RemittanceDomainValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(RemittanceDomainValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        String[] currentMsgId = {null};
        try {
            repository.streamMessages(msg -> {
                currentMsgId[0] = msg.messageId();
                try {
                    repository.streamRemittances(msg.messageId(), rmt -> {
                        String rmtId = rmt.remittanceId();

                        try {
                            new Iban(rmt.debtorAccountIban());
                        } catch (InvalidIbanException e) {
                            context.addError(getName(),
                                    "Invalid debtor IBAN", rmtId, rmt.debtorAccountIban());
                        }

                        try {
                            new Bic(rmt.debtorAgentBic());
                        } catch (InvalidBicException e) {
                            context.addError(getName(),
                                    "Invalid debtor agent BIC", rmtId, rmt.debtorAgentBic());
                        }

                        if (rmt.requestedExecutionDate() == null) {
                            context.addError(getName(),
                                    "requestedExecutionDate must not be null", rmtId);
                        }
                    });
                } catch (SQLException e) {
                    LOG.error("{} failed streaming remittances for {}: {}",
                            getName(), currentMsgId[0], e.getMessage(), e);
                    context.addError(getName(), "Failed to stream remittances",
                            currentMsgId[0], e.getMessage());
                }
            });
            LOG.debug("{} completed", getName());
        } catch (SQLException e) {
            LOG.error("{} failed: {}", getName(), e.getMessage(), e);
            context.addError(getName(), "Failed to stream messages", e.getMessage());
        }
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "RemittanceDomainValidator";
    }
}
