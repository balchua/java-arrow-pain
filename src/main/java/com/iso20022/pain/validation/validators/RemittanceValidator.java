package com.iso20022.pain.validation.validators;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import com.iso20022.pain.validation.ValidationContext;
import com.iso20022.pain.validation.Validator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Validates remittance-level fields in the Arrow batch result.
 * 
 * <p>Checks:</p>
 * <ul>
 *   <li>IBAN format validation using regex: ^[A-Z]{2}[0-9]{2}[A-Z0-9]+$</li>
 *   <li>Payment method is required</li>
 * </ul>
 * 
 * <p>This validator is parallelizable.</p>
 */
public final class RemittanceValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(RemittanceValidator.class);
    private static final Pattern IBAN_PATTERN = Pattern.compile("^[A-Z]{2}[0-9]{2}[A-Z0-9]+$");

    @Override
    public void validate(ArrowBatchResult result, ValidationContext context) {
        int validatedCount = 0;
        int errorCount = 0;

        for (VectorSchemaRoot rmtBatch : result.getRemittanceBatches()) {
            VarCharVector ibanVec = (VarCharVector) rmtBatch.getVector(
                    Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN);
            VarCharVector pmtMtdVec = (VarCharVector) rmtBatch.getVector(
                    Pain001ArrowSchema.RMT_PMT_MTD);
            VarCharVector pmtInfIdVec = (VarCharVector) rmtBatch.getVector(
                    Pain001ArrowSchema.RMT_PMT_INF_ID);

            int rows = rmtBatch.getRowCount();
            for (int i = 0; i < rows; i++) {
                validatedCount++;
                String pmtInfId = pmtInfIdVec.isNull(i) ? "unknown" 
                        : new String(pmtInfIdVec.get(i), StandardCharsets.UTF_8);

                // Validate IBAN
                if (!ibanVec.isNull(i)) {
                    String iban = new String(ibanVec.get(i), StandardCharsets.UTF_8);
                    if (!IBAN_PATTERN.matcher(iban).matches()) {
                        errorCount++;
                        context.addError(getName(), 
                                "Invalid IBAN format", 
                                "PmtInfId=" + pmtInfId + ", IBAN=" + iban);
                    }
                }

                // Validate payment method
                if (pmtMtdVec.isNull(i)) {
                    errorCount++;
                    context.addError(getName(), 
                            "Payment method is required", 
                            "PmtInfId=" + pmtInfId);
                }
            }
        }

        LOG.debug("{} completed: validated {} remittances, {} errors", 
                getName(), validatedCount, errorCount);
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "RemittanceValidator";
    }
}
