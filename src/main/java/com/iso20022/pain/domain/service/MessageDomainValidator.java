package com.iso20022.pain.domain.service;

import com.iso20022.pain.dal.PaymentRepository;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * Pure-Java domain validator for ISO 20022 GroupHeader (GrpHdr) fields.
 *
 * <p>Checks performed without SQL:
 * <ul>
 *   <li>{@code messageId} must be present and at most 35 characters.</li>
 *   <li>{@code initiatingParty} must be present and non-blank.</li>
 *   <li>{@code creationDateTime} must be parseable as an ISO 8601 offset datetime.</li>
 * </ul>
 * </p>
 */
public final class MessageDomainValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDomainValidator.class);
    private static final int MAX_MSG_ID_LEN = 35;

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            repository.streamMessages(msg -> {
                String id = msg.messageId();

                if (id == null || id.isBlank()) {
                    context.addError(getName(), "messageId must not be blank", id);
                } else if (id.length() > MAX_MSG_ID_LEN) {
                    context.addError(getName(),
                            "messageId exceeds max length " + MAX_MSG_ID_LEN, id);
                }

                if (msg.initiatingParty() == null || msg.initiatingParty().isBlank()) {
                    context.addError(getName(), "initiatingParty must not be blank", id);
                }

                String cdt = msg.creationDateTime();
                if (cdt != null && !cdt.isBlank()) {
                    try {
                        OffsetDateTime.parse(cdt);
                    } catch (DateTimeParseException e) {
                        context.addError(getName(),
                                "creationDateTime is not a valid ISO 8601 offset datetime",
                                id, cdt);
                    }
                } else {
                    context.addError(getName(), "creationDateTime must not be blank", id);
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
        return "MessageDomainValidator";
    }
}
