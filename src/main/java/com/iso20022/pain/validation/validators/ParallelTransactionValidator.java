package com.iso20022.pain.validation.validators;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.VirtualThreadValidator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Validates transaction-level fields using virtual threads for batch-level parallelism.
 * 
 * <p>Each transaction batch is processed in its own virtual thread, allowing
 * efficient parallel validation of large datasets.</p>
 * 
 * <p>Same validation logic as {@link TransactionValidator} but with parallel batch processing.</p>
 */
public final class ParallelTransactionValidator extends VirtualThreadValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelTransactionValidator.class);

    @Override
    protected List<VectorSchemaRoot> getBatches(ArrowBatchResult result) {
        return result.getTransactionBatches();
    }

    @Override
    protected void validateBatch(VectorSchemaRoot txBatch, int batchIndex, ValidationContext context) {
        DecimalVector amtVec = (DecimalVector) txBatch.getVector(
                Pain001ArrowSchema.TX_INSTD_AMT);
        VarCharVector cdtrVec = (VarCharVector) txBatch.getVector(
                Pain001ArrowSchema.TX_CDTR_NM);
        VarCharVector instrIdVec = (VarCharVector) txBatch.getVector(
                Pain001ArrowSchema.TX_INSTR_ID);

        int rows = txBatch.getRowCount();
        int errorCount = 0;

        for (int i = 0; i < rows; i++) {
            String instrId = instrIdVec.isNull(i) ? "unknown" 
                    : new String(instrIdVec.get(i), StandardCharsets.UTF_8);

            // Validate amount is positive
            if (!amtVec.isNull(i)) {
                BigDecimal amt = amtVec.getObject(i);
                if (amt.compareTo(BigDecimal.ZERO) <= 0) {
                    errorCount++;
                    context.addError(getName(), 
                            "Amount must be positive", 
                            "Batch=" + batchIndex + ", InstrId=" + instrId + ", Amount=" + amt);
                }
            }

            // Validate creditor name
            if (cdtrVec.isNull(i)) {
                errorCount++;
                context.addError(getName(), 
                        "Creditor name is required", 
                        "Batch=" + batchIndex + ", InstrId=" + instrId);
            }
        }

        LOG.debug("{} batch {} completed: {} rows, {} errors", 
                getName(), batchIndex, rows, errorCount);
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "ParallelTransactionValidator";
    }
}
