package com.iso20022.pain.validation.validators;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.Validator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Validates transaction-level fields in the Arrow batch result.
 * 
 * <p>Checks:</p>
 * <ul>
 *   <li>Amounts are positive</li>
 *   <li>Creditor name is required</li>
 * </ul>
 * 
 * <p>This validator is parallelizable.</p>
 */
public final class TransactionValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidator.class);

    @Override
    public void validate(ArrowBatchResult result, ValidationContext context) {
        int validatedCount = 0;
        int errorCount = 0;

        for (VectorSchemaRoot txBatch : result.getTransactionBatches()) {
            DecimalVector amtVec = (DecimalVector) txBatch.getVector(
                    Pain001ArrowSchema.TX_INSTD_AMT);
            VarCharVector cdtrVec = (VarCharVector) txBatch.getVector(
                    Pain001ArrowSchema.TX_CDTR_NM);
            VarCharVector instrIdVec = (VarCharVector) txBatch.getVector(
                    Pain001ArrowSchema.TX_INSTR_ID);

            int rows = txBatch.getRowCount();
            for (int i = 0; i < rows; i++) {
                validatedCount++;
                String instrId = instrIdVec.isNull(i) ? "unknown" 
                        : new String(instrIdVec.get(i), StandardCharsets.UTF_8);

                // Validate amount is positive
                if (!amtVec.isNull(i)) {
                    BigDecimal amt = amtVec.getObject(i);
                    if (amt.compareTo(BigDecimal.ZERO) <= 0) {
                        errorCount++;
                        context.addError(getName(), 
                                "Amount must be positive", 
                                "InstrId=" + instrId + ", Amount=" + amt);
                    }
                }

                // Validate creditor name
                if (cdtrVec.isNull(i)) {
                    errorCount++;
                    context.addError(getName(), 
                            "Creditor name is required", 
                            "InstrId=" + instrId);
                }
            }
        }

        LOG.debug("{} completed: validated {} transactions, {} errors", 
                getName(), validatedCount, errorCount);
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "TransactionValidator";
    }
}
