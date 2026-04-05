package com.iso20022.pain.domain.model;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Domain model for one ISO 20022 pain.001 PaymentInformation block (PmtInf).
 *
 * <p>Carries scalar fields only. Child {@link Transaction} records are never held
 * here; they are accessed on demand via streaming DuckDB queries to avoid
 * materialising millions of transactions on the Java heap.</p>
 *
 * <p>{@code messageId} is a foreign key back to the parent {@link Message}.</p>
 */
public record Remittance(
        String messageId,
        String remittanceId,
        String paymentMethod,
        String numberOfTransactions,
        BigDecimal controlSum,
        String serviceLevelCode,
        LocalDate requestedExecutionDate,
        String debtor,
        String debtorAccountIban,
        String debtorAgentBic) {
}
