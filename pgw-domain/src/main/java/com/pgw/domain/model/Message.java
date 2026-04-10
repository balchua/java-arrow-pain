package com.pgw.domain.model;

import java.math.BigDecimal;

/**
 * Domain model for one ISO 20022 pain.001 GroupHeader (GrpHdr).
 *
 * <p>Carries scalar fields only. Child {@link Remittance} records are never held
 * here; they are accessed on demand via streaming DuckDB queries to avoid
 * materialising large collections on the Java heap.</p>
 */
public record Message(
        String messageId,
        String creationDateTime,
        String numberOfTransactions,
        BigDecimal controlSum,
        String initiatingParty) {
}
