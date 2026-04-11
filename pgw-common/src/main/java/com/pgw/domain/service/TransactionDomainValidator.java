package com.pgw.domain.service;

import com.pgw.dal.PaymentRepository;
import com.pgw.domain.exception.InvalidAmountException;
import com.pgw.domain.exception.InvalidBicException;
import com.pgw.domain.exception.InvalidCurrencyException;
import com.pgw.domain.exception.InvalidIbanException;
import com.pgw.domain.valueobject.Amount;
import com.pgw.domain.valueobject.Bic;
import com.pgw.domain.valueobject.Currency;
import com.pgw.domain.valueobject.Iban;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Pure-Java domain validator for ISO 20022 CreditTransferTransaction (CdtTrfTxInf) fields.
 *
 * <p>Checks performed using domain value objects:
 * <ul>
 *   <li>Instructed amount and currency ({@link Amount}, {@link Currency}) — value must be &gt; 0,
 *       currency must be a valid ISO 4217 code.</li>
 *   <li>Creditor agent BIC ({@link Bic}) — SWIFT format regex.</li>
 *   <li>Creditor IBAN ({@link Iban}) — MOD-97 checksum.</li>
 *   <li>{@code creditor} name must be present and non-blank.</li>
 * </ul>
 * </p>
 */
public final class TransactionDomainValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionDomainValidator.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            repository.streamMessages(msg -> {
                try {
                    repository.streamRemittances(msg.messageId(), rmt -> {
                        try {
                            repository.streamTransactions(rmt.remittanceId(), tx -> {
                                String txId = tx.endToEndId();

                                try {
                                    new Amount(tx.instructedAmount(), new Currency(tx.currency()));
                                } catch (InvalidCurrencyException e) {
                                    context.addError(getName(),
                                            "Invalid currency code", txId, tx.currency());
                                } catch (InvalidAmountException e) {
                                    context.addError(getName(),
                                            "Instructed amount must be > 0",
                                            txId, tx.instructedAmount());
                                }

                                try {
                                    new Bic(tx.creditorAgentBic());
                                } catch (InvalidBicException e) {
                                    context.addError(getName(),
                                            "Invalid creditor agent BIC",
                                            txId, tx.creditorAgentBic());
                                }

                                try {
                                    new Iban(tx.creditorAccountIban());
                                } catch (InvalidIbanException e) {
                                    context.addError(getName(),
                                            "Invalid creditor IBAN",
                                            txId, tx.creditorAccountIban());
                                }

                                if (tx.creditor() == null || tx.creditor().isBlank()) {
                                    context.addError(getName(),
                                            "Creditor name must not be blank", txId);
                                }
                            });
                        } catch (SQLException e) {
                            LOG.error("{} failed streaming transactions for {}: {}",
                                    getName(), rmt.remittanceId(), e.getMessage(), e);
                            context.addError(getName(), "Failed to stream transactions",
                                    rmt.remittanceId(), e.getMessage());
                        }
                    });
                } catch (SQLException e) {
                    LOG.error("{} failed streaming remittances for {}: {}",
                            getName(), msg.messageId(), e.getMessage(), e);
                    context.addError(getName(), "Failed to stream remittances",
                            msg.messageId(), e.getMessage());
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
        return false;
    }

    @Override
    public String getName() {
        return "TransactionDomainValidator";
    }
}
