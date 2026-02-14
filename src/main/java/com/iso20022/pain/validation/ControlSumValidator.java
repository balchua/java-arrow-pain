package com.iso20022.pain.validation;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validates ISO 20022 control sums by iterating over the Arrow tables
 * in memory.
 *
 * <h3>Validation rules</h3>
 * <ol>
 * <li><b>Remittance CtrlSum</b> — for each remittance (PmtInf), the declared
 * {@code ctrl_sum} must equal the sum of all transaction {@code instd_amt}
 * rows that share the same {@code pmt_inf_id}.</li>
 * <li><b>Message CtrlSum</b> — the declared message-level {@code ctrl_sum}
 * must equal the sum of all remittance {@code ctrl_sum} values (which in
 * turn must equal the grand total of all transaction amounts).</li>
 * </ol>
 *
 * <p>
 * All iteration happens over Arrow off-heap vectors — no XML re-parsing.
 * This lets you benchmark how fast Arrow columnar scans are.
 * </p>
 */
public final class ControlSumValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ControlSumValidator.class);

    private ControlSumValidator() {
    }

    /**
     * Result record for the validation.
     *
     * @param valid               true if all control sums match
     * @param remittancesChecked  number of remittance rows checked
     * @param transactionsScanned total transaction rows scanned
     * @param errors              number of mismatches found
     * @param details             human-readable detail lines (empty if valid)
     */
    public record ValidationResult(
            boolean valid,
            long remittancesChecked,
            long transactionsScanned,
            int errors,
            List<String> details) {
    }

    /**
     * Validates the control sums in the given Arrow result.
     *
     * @param result the three Arrow tables (message, remittance, transaction)
     * @return a {@link ValidationResult}
     */
    public static ValidationResult validate(ArrowBatchResult result) {

        // ─── Step 1: sum transaction amounts per pmt_inf_id ─────────────
        // Scan every transaction batch, accumulate BigDecimal sums
        // keyed by pmt_inf_id.
        Map<String, BigDecimal> txSumByPmtInfId = new HashMap<>();
        long txScanned = 0;

        for (VectorSchemaRoot txBatch : result.getTransactionBatches()) {
            VarCharVector pmtInfIdVec = (VarCharVector) txBatch.getVector(
                    Pain001ArrowSchema.TX_PMT_INF_ID);
            DecimalVector amtVec = (DecimalVector) txBatch.getVector(
                    Pain001ArrowSchema.TX_INSTD_AMT);

            int rows = txBatch.getRowCount();
            for (int i = 0; i < rows; i++) {
                String pmtInfId = new String(pmtInfIdVec.get(i), StandardCharsets.UTF_8);
                BigDecimal amt = amtVec.getObject(i);
                txSumByPmtInfId.merge(pmtInfId, amt, BigDecimal::add);
                txScanned++;
            }
        }

        // ─── Step 2: check each remittance ctrl_sum ─────────────────────
        java.util.List<String> errors = new java.util.ArrayList<>();
        long rmtChecked = 0;
        BigDecimal grandTotalFromRemittances = BigDecimal.ZERO;
        int errorCount = 0;

        for (VectorSchemaRoot rmtBatch : result.getRemittanceBatches()) {
            VarCharVector pmtInfIdVec = (VarCharVector) rmtBatch.getVector(
                    Pain001ArrowSchema.RMT_PMT_INF_ID);
            DecimalVector ctrlSumVec = (DecimalVector) rmtBatch.getVector(
                    Pain001ArrowSchema.RMT_CTRL_SUM);

            int rows = rmtBatch.getRowCount();
            for (int i = 0; i < rows; i++) {
                rmtChecked++;
                String pmtInfId = new String(pmtInfIdVec.get(i), StandardCharsets.UTF_8);
                BigDecimal declaredSum = ctrlSumVec.isNull(i)
                        ? null
                        : ctrlSumVec.getObject(i);

                BigDecimal actualSum = txSumByPmtInfId.getOrDefault(pmtInfId, BigDecimal.ZERO);

                if (declaredSum != null) {
                    // Compare at the declared scale (18,2) — rescale actual to match
                    BigDecimal actualScaled = actualSum.setScale(
                            declaredSum.scale(), RoundingMode.HALF_UP);
                    if (declaredSum.compareTo(actualScaled) != 0) {
                        errorCount++;
                        String msg = String.format(
                                "Remittance [%s] CtrlSum mismatch: declared=%s, actual=%s",
                                pmtInfId, declaredSum.toPlainString(),
                                actualScaled.toPlainString());
                        errors.add(msg);
                        if (errorCount <= 5) {
                            LOG.warn("  ✗ {}", msg);
                        }
                    }
                    grandTotalFromRemittances = grandTotalFromRemittances.add(declaredSum);
                } else {
                    // CtrlSum is optional; if absent, use actual for grand total
                    grandTotalFromRemittances = grandTotalFromRemittances.add(
                            actualSum.setScale(2, RoundingMode.HALF_UP));
                }
            }
        }

        // ─── Step 3: check message-level ctrl_sum ───────────────────────
        VectorSchemaRoot msgRoot = result.getMessageRoot();
        if (msgRoot.getRowCount() > 0) {
            DecimalVector msgCtrlSumVec = (DecimalVector) msgRoot.getVector(
                    Pain001ArrowSchema.MSG_CTRL_SUM);
            BigDecimal msgCtrlSum = msgCtrlSumVec.getObject(0);

            BigDecimal grandScaled = grandTotalFromRemittances.setScale(
                    msgCtrlSum.scale(), RoundingMode.HALF_UP);

            if (msgCtrlSum.compareTo(grandScaled) != 0) {
                errorCount++;
                String msg = String.format(
                        "Message CtrlSum mismatch: declared=%s, sum_of_remittances=%s",
                        msgCtrlSum.toPlainString(), grandScaled.toPlainString());
                errors.add(msg);
                LOG.warn("  ✗ {}", msg);
            }
        }

        boolean valid = errorCount == 0;
        if (valid) {
            LOG.info("  ✓ All control sums valid  ({} remittances, {} transactions)",
                    rmtChecked, txScanned);
        } else {
            LOG.error("  ✗ {} control sum error(s) found", errorCount);
        }

        return new ValidationResult(valid, rmtChecked, txScanned, errorCount,
                List.copyOf(errors));
    }
}
