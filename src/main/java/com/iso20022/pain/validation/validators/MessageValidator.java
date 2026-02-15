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

/**
 * Validates message-level fields in the Arrow batch result.
 * 
 * <p>Checks:</p>
 * <ul>
 *   <li>MsgId length ≤ 35 characters</li>
 *   <li>Warns if InitgPty is missing</li>
 *   <li>Errors if CreDtTm is missing</li>
 * </ul>
 * 
 * <p>This validator is parallelizable.</p>
 */
public final class MessageValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(MessageValidator.class);
    private static final int MAX_MSG_ID_LENGTH = 35;

    @Override
    public void validate(ArrowBatchResult result, ValidationContext context) {
        VectorSchemaRoot msgRoot = result.getMessageRoot();
        
        if (msgRoot.getRowCount() == 0) {
            context.addError(getName(), "No message row found");
            return;
        }

        // Validate MsgId
        VarCharVector msgIdVec = (VarCharVector) msgRoot.getVector(Pain001ArrowSchema.MSG_ID);
        if (!msgIdVec.isNull(0)) {
            String msgId = new String(msgIdVec.get(0), StandardCharsets.UTF_8);
            if (msgId.length() > MAX_MSG_ID_LENGTH) {
                context.addError(getName(), 
                        "MsgId exceeds maximum length of " + MAX_MSG_ID_LENGTH + " characters",
                        msgId.length());
            }
        }

        // Validate InitgPty (warn if missing)
        VarCharVector initgPtyVec = (VarCharVector) msgRoot.getVector(Pain001ArrowSchema.MSG_INITG_PTY_NM);
        if (initgPtyVec.isNull(0)) {
            context.addWarning(getName(), "Initiating party (InitgPty) is missing");
        }

        // Validate CreDtTm (error if missing)
        VarCharVector creDtTmVec = (VarCharVector) msgRoot.getVector(Pain001ArrowSchema.MSG_CRE_DT_TM);
        if (creDtTmVec.isNull(0)) {
            context.addError(getName(), "Creation date/time (CreDtTm) is required but missing");
        }

        LOG.debug("{} completed", getName());
    }

    @Override
    public boolean isParallelizable() {
        return true;
    }

    @Override
    public String getName() {
        return "MessageValidator";
    }
}
