package com.pgw.purearrow.validator.dal;

import com.pgw.arrow.Pain001ArrowSchema;
import com.pgw.domain.model.Message;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Consumer;

/**
 * In-memory Arrow-backed accessor for the <b>message</b> table
 * (one row per ISO 20022 pain.001 GroupHeader / GrpHdr).
 *
 * <p>Wraps a list of materialised {@link VectorSchemaRoot} batches loaded by
 * {@link ArrowTableLoader}. Column values are accessed by name using the field
 * constants from {@link Pain001ArrowSchema}. All access is zero-copy: no
 * Java-heap objects are created until an actual row is read.</p>
 *
 * <p>Call {@link #close()} to release all Arrow off-heap memory when the table
 * is no longer needed.</p>
 */
public final class ArrowMessageTable implements AutoCloseable {

    private final List<VectorSchemaRoot> batches;

    ArrowMessageTable(List<VectorSchemaRoot> batches) {
        this.batches = batches;
    }

    // ── Row count ─────────────────────────────────────────────────────────────

    /** Total number of message rows across all batches. */
    public long getRowCount() {
        return ArrowTableLoader.totalRows(batches);
    }

    // ── Typed column accessors ────────────────────────────────────────────────

    /** Returns the {@code msg_id} value for row {@code i} in {@code batch}, or {@code null}. */
    public String getMsgId(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.MSG_ID, i);
    }

    /** Returns the {@code msg_cre_dt_tm} value for row {@code i}, or {@code null}. */
    public String getCreDtTm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.MSG_CRE_DT_TM, i);
    }

    /** Returns the {@code msg_nb_of_txs} value for row {@code i}, or {@code null}. */
    public String getNbOfTxs(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.MSG_NB_OF_TXS, i);
    }

    /** Returns the {@code msg_ctrl_sum} value for row {@code i}, or {@code null}. */
    public BigDecimal getCtrlSum(VectorSchemaRoot batch, int i) {
        return decimal(batch, Pain001ArrowSchema.MSG_CTRL_SUM, i);
    }

    /** Returns the {@code msg_initg_pty_nm} value for row {@code i}, or {@code null}. */
    public String getInitgPtyNm(VectorSchemaRoot batch, int i) {
        return varchar(batch, Pain001ArrowSchema.MSG_INITG_PTY_NM, i);
    }

    // ── Row iteration ─────────────────────────────────────────────────────────

    /**
     * Iterates over every row in all batches and invokes {@code consumer} with a
     * fully-populated {@link Message} domain object for each row.
     */
    public void forEach(Consumer<Message> consumer) {
        for (VectorSchemaRoot batch : batches) {
            int rows = batch.getRowCount();
            for (int i = 0; i < rows; i++) {
                consumer.accept(new Message(
                        getMsgId(batch, i),
                        getCreDtTm(batch, i),
                        getNbOfTxs(batch, i),
                        getCtrlSum(batch, i),
                        getInitgPtyNm(batch, i)));
            }
        }
    }

    // ── Resource management ───────────────────────────────────────────────────

    @Override
    public void close() {
        ArrowTableLoader.closeBatches(batches);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private static String varchar(VectorSchemaRoot root, String field, int i) {
        VarCharVector v = (VarCharVector) root.getVector(field);
        if (v == null || v.isNull(i)) return null;
        return v.getObject(i).toString();
    }

    private static BigDecimal decimal(VectorSchemaRoot root, String field, int i) {
        DecimalVector v = (DecimalVector) root.getVector(field);
        if (v == null || v.isNull(i)) return null;
        return v.getObject(i);
    }
}
