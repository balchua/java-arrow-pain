package com.iso20022.pain.arrow;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Defines three Apache Arrow schemas for ISO 20022 pain.001.001.09:
 * <ol>
 * <li><b>Message</b> — one row per GroupHeader (GrpHdr / GroupHeader85)</li>
 * <li><b>Remittance</b> — one row per PaymentInformation (PmtInf /
 * PaymentInstruction30)</li>
 * <li><b>Transaction</b> — one row per CreditTransferTransaction (CdtTrfTxInf /
 * CreditTransferTransaction34)</li>
 * </ol>
 *
 * <p>
 * Relational links:
 * </p>
 * <ul>
 * <li>Remittance → Message via {@code msg_id} (foreign key)</li>
 * <li>Transaction → Remittance via {@code pmt_inf_id} (foreign key)</li>
 * </ul>
 *
 * <p>
 * Arrow type mappings follow ISO 20022 data type definitions:
 * </p>
 * <table>
 * <tr>
 * <th>ISO 20022 Type</th>
 * <th>Arrow Type</th>
 * </tr>
 * <tr>
 * <td>Max35Text / Max140Text / Max15NumericText</td>
 * <td>Utf8</td>
 * </tr>
 * <tr>
 * <td>ISODateTime</td>
 * <td>Utf8 (ISO 8601 string)</td>
 * </tr>
 * <tr>
 * <td>ISODate</td>
 * <td>Date (DateUnit.DAY)</td>
 * </tr>
 * <tr>
 * <td>DecimalNumber (totalDigits:18, fractionDigits:17)</td>
 * <td>Decimal128(18,2)</td>
 * </tr>
 * <tr>
 * <td>ActiveOrHistoricCurrencyAndAmount (fractionDigits:5)</td>
 * <td>Decimal128(18,5)</td>
 * </tr>
 * <tr>
 * <td>ActiveOrHistoricCurrencyCode ([A-Z]{3})</td>
 * <td>Utf8</td>
 * </tr>
 * <tr>
 * <td>PaymentMethod3Code (TRF/CHK/TRA)</td>
 * <td>Utf8</td>
 * </tr>
 * <tr>
 * <td>ExternalServiceLevel1Code (Max4Text)</td>
 * <td>Utf8</td>
 * </tr>
 * <tr>
 * <td>IBAN2007Identifier</td>
 * <td>Utf8</td>
 * </tr>
 * <tr>
 * <td>BICFIDec2014Identifier</td>
 * <td>Utf8</td>
 * </tr>
 * </table>
 */
public final class Pain001ArrowSchema {

    private Pain001ArrowSchema() {
    }

    // ─── Arrow type constants ────────────────────────────────────────────────
    private static final ArrowType UTF8 = ArrowType.Utf8.INSTANCE;
    private static final ArrowType DECIMAL_18_2 = new ArrowType.Decimal(18, 2, 128);
    private static final ArrowType DECIMAL_18_5 = new ArrowType.Decimal(18, 5, 128);
    private static final ArrowType DATE_DAY = new ArrowType.Date(DateUnit.DAY);

    // ═══════════════════════════════════════════════════════════════════════════
    // Message (GroupHeader85) field names
    // ═══════════════════════════════════════════════════════════════════════════

    /** MsgId: Max35Text — primary key */
    public static final String MSG_ID = "msg_id";
    /** CreDtTm: ISODateTime */
    public static final String MSG_CRE_DT_TM = "msg_cre_dt_tm";
    /** NbOfTxs: Max15NumericText */
    public static final String MSG_NB_OF_TXS = "msg_nb_of_txs";
    /** CtrlSum: DecimalNumber */
    public static final String MSG_CTRL_SUM = "msg_ctrl_sum";
    /** InitgPty/Nm: Max140Text */
    public static final String MSG_INITG_PTY_NM = "msg_initg_pty_nm";

    // ═══════════════════════════════════════════════════════════════════════════
    // Remittance (PaymentInstruction30) field names
    // ═══════════════════════════════════════════════════════════════════════════

    /** Foreign key → Message.msg_id */
    public static final String RMT_MSG_ID = "msg_id";
    /** PmtInfId: Max35Text — primary key */
    public static final String RMT_PMT_INF_ID = "pmt_inf_id";
    /** PmtMtd: PaymentMethod3Code */
    public static final String RMT_PMT_MTD = "pmt_mtd";
    /** NbOfTxs: Max15NumericText */
    public static final String RMT_NB_OF_TXS = "nb_of_txs";
    /** CtrlSum: DecimalNumber */
    public static final String RMT_CTRL_SUM = "ctrl_sum";
    /** SvcLvl/Cd: ExternalServiceLevel1Code */
    public static final String RMT_SVC_LVL_CD = "svc_lvl_cd";
    /** ReqdExctnDt/Dt: ISODate */
    public static final String RMT_REQD_EXCTN_DT = "reqd_exctn_dt";
    /** Dbtr/Nm: Max140Text */
    public static final String RMT_DBTR_NM = "dbtr_nm";
    /** DbtrAcct/Id/IBAN: IBAN2007Identifier */
    public static final String RMT_DBTR_ACCT_IBAN = "dbtr_acct_iban";
    /** DbtrAgt/FinInstnId/BICFI: BICFIDec2014Identifier */
    public static final String RMT_DBTR_AGT_BICFI = "dbtr_agt_bicfi";

    // ═══════════════════════════════════════════════════════════════════════════
    // Transaction (CreditTransferTransaction34) field names
    // ═══════════════════════════════════════════════════════════════════════════

    /** Foreign key → Remittance.pmt_inf_id */
    public static final String TX_PMT_INF_ID = "pmt_inf_id";
    /** PmtId/InstrId: Max35Text */
    public static final String TX_INSTR_ID = "instr_id";
    /** PmtId/EndToEndId: Max35Text */
    public static final String TX_END_TO_END_ID = "end_to_end_id";
    /** Amt/InstdAmt: ActiveOrHistoricCurrencyAndAmount */
    public static final String TX_INSTD_AMT = "instd_amt";
    /** Amt/InstdAmt/@Ccy: ActiveOrHistoricCurrencyCode */
    public static final String TX_CCY = "ccy";
    /** CdtrAgt/FinInstnId/BICFI: BICFIDec2014Identifier */
    public static final String TX_CDTR_AGT_BICFI = "cdtr_agt_bicfi";
    /** Cdtr/Nm: Max140Text */
    public static final String TX_CDTR_NM = "cdtr_nm";
    /** CdtrAcct/Id/IBAN: IBAN2007Identifier */
    public static final String TX_CDTR_ACCT_IBAN = "cdtr_acct_iban";
    /** RmtInf/Ustrd: Max140Text */
    public static final String TX_RMT_INF_USTRD = "rmt_inf_ustrd";
    /** RgltryRptg/Dtls/Cd: newline-delimited regulatory reporting codes */
    public static final String TX_RGLTY_RPTG_CD = "rglty_rptg_cd";

    // ═══════════════════════════════════════════════════════════════════════════
    // Schema factory methods
    // ═══════════════════════════════════════════════════════════════════════════

    /**
     * Arrow schema for the <b>Message</b> table (one row per GrpHdr).
     */
    public static Schema createMessageSchema() {
        return new Schema(List.of(
                field(MSG_ID, UTF8, false),
                field(MSG_CRE_DT_TM, UTF8, false),
                field(MSG_NB_OF_TXS, UTF8, false),
                field(MSG_CTRL_SUM, DECIMAL_18_2, false),
                field(MSG_INITG_PTY_NM, UTF8, false)));
    }

    /**
     * Arrow schema for the <b>Remittance</b> table (one row per PmtInf).
     * Linked to Message via {@code msg_id}.
     */
    public static Schema createRemittanceSchema() {
        return new Schema(List.of(
                field(RMT_MSG_ID, UTF8, false),
                field(RMT_PMT_INF_ID, UTF8, false),
                field(RMT_PMT_MTD, UTF8, false),
                field(RMT_NB_OF_TXS, UTF8, false),
                field(RMT_CTRL_SUM, DECIMAL_18_2, true),
                field(RMT_SVC_LVL_CD, UTF8, true),
                field(RMT_REQD_EXCTN_DT, DATE_DAY, false),
                field(RMT_DBTR_NM, UTF8, false),
                field(RMT_DBTR_ACCT_IBAN, UTF8, false),
                field(RMT_DBTR_AGT_BICFI, UTF8, false)));
    }

    /**
     * Arrow schema for the <b>Transaction</b> table (one row per CdtTrfTxInf).
     * Linked to Remittance via {@code pmt_inf_id}.
     */
    public static Schema createTransactionSchema() {
        return new Schema(List.of(
                field(TX_PMT_INF_ID, UTF8, false),
                field(TX_INSTR_ID, UTF8, true),
                field(TX_END_TO_END_ID, UTF8, false),
                field(TX_INSTD_AMT, DECIMAL_18_5, false),
                field(TX_CCY, UTF8, false),
                field(TX_CDTR_AGT_BICFI, UTF8, true),
                field(TX_CDTR_NM, UTF8, false),
                field(TX_CDTR_ACCT_IBAN, UTF8, false),
                field(TX_RMT_INF_USTRD, UTF8, true),
                field(TX_RGLTY_RPTG_CD, UTF8, true)));
    }

    private static Field field(String name, ArrowType type, boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }
}
