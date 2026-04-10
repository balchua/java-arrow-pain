package com.pgw.domain.model;

import java.math.BigDecimal;

/**
 * Domain model for one ISO 20022 pain.001 CreditTransferTransaction (CdtTrfTxInf).
 *
 * <p>Carries scalar fields only. {@code remittanceId} is a foreign key back to the
 * parent {@link Remittance}. The hierarchy exists only in the DuckDB schema
 * (foreign keys), not in Java object references.</p>
 */
public record Transaction(
        String remittanceId,
        String instructionId,
        String endToEndId,
        BigDecimal instructedAmount,
        String currency,
        String creditorAgentBic,
        String creditor,
        String creditorAccountIban,
        String remittanceInfoUnstructured,
        String regulatoryReportingCode,
        String remittanceInfoStructuredRef,
        String remittanceInfoStructuredRefType,
        String purposeCode,
        String ultimateCreditorName,
        String creditorCountry) {
}
