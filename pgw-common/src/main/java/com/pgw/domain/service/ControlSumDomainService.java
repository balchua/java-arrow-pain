package com.pgw.domain.service;

import com.pgw.dal.PaymentRepository;
import com.pgw.domain.exception.InvalidAmountException;
import com.pgw.domain.exception.InvalidControlSumException;
import com.pgw.domain.exception.InvalidCurrencyException;
import com.pgw.domain.valueobject.Amount;
import com.pgw.domain.valueobject.ControlSum;
import com.pgw.domain.valueobject.Currency;
import com.pgw.validation.ValidationContext;
import com.pgw.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * In-memory arithmetic control-sum check using domain value objects.
 *
 * <p>For each remittance, sums the instructed amounts of its transactions using
 * {@link Amount#add(Amount)} and compares the result against the remittance-level
 * {@link ControlSum}. Then repeats the check at message level across all remittances.</p>
 *
 * <p>This validator must run sequentially ({@link #isParallelizable()} returns
 * {@code false}) because it accumulates state across nested streaming calls.</p>
 */
public final class ControlSumDomainService implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(ControlSumDomainService.class);

    @Override
    public void validate(PaymentRepository repository, ValidationContext context) {
        try {
            repository.streamMessages(msg -> {
                String msgId = msg.messageId();

                try {
                    ControlSum msgControlSum = new ControlSum(msg.controlSum());
                    BigDecimal[] grandTotal = {BigDecimal.ZERO};

                    repository.streamRemittances(msgId, rmt -> {
                        String rmtId = rmt.remittanceId();
                        BigDecimal[] rmtTotal = {BigDecimal.ZERO};

                        try {
                            ControlSum rmtControlSum = new ControlSum(rmt.controlSum());

                            repository.streamTransactions(rmtId, tx -> {
                                try {
                                    Amount txAmount = new Amount(
                                            tx.instructedAmount(), new Currency(tx.currency()));
                                    BigDecimal txValue = txAmount.value();
                                    rmtTotal[0] = rmtTotal[0].add(txValue);
                                    grandTotal[0] = grandTotal[0].add(txValue);
                                } catch (InvalidCurrencyException | InvalidAmountException e) {
                                    // skip invalid transactions — TransactionDomainValidator reports these
                                }
                            });

                            if (!rmtControlSum.matches(rmtTotal[0])) {
                                context.addError(getName(),
                                        "Remittance control sum mismatch: declared="
                                                + rmtControlSum.value()
                                                + " computed=" + rmtTotal[0],
                                        rmtId);
                            }
                        } catch (InvalidControlSumException e) {
                            context.addError(getName(),
                                    "Invalid remittance control sum", rmtId, rmt.controlSum());
                        } catch (SQLException e) {
                            LOG.error("{} failed streaming transactions for {}: {}",
                                    getName(), rmtId, e.getMessage(), e);
                            context.addError(getName(), "Failed to stream transactions",
                                    rmtId, e.getMessage());
                        }
                    });

                    if (!msgControlSum.matches(grandTotal[0])) {
                        context.addError(getName(),
                                "Message control sum mismatch: declared="
                                        + msgControlSum.value()
                                        + " computed=" + grandTotal[0],
                                msgId);
                    }
                } catch (InvalidControlSumException e) {
                    context.addError(getName(),
                            "Invalid message control sum", msgId, msg.controlSum());
                } catch (SQLException e) {
                    LOG.error("{} failed streaming remittances for {}: {}",
                            getName(), msgId, e.getMessage(), e);
                    context.addError(getName(), "Failed to stream remittances",
                            msgId, e.getMessage());
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
        return "ControlSumDomainService";
    }
}
