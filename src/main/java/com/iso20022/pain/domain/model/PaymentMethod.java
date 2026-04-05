package com.iso20022.pain.domain.model;

/**
 * ISO 20022 payment method codes for credit-transfer payment instructions.
 *
 * <p>TRF — Credit Transfer (standard SEPA/SWIFT wire).</p>
 * <p>CHK — Cheque payment.</p>
 */
public enum PaymentMethod {
    TRF, CHK
}
