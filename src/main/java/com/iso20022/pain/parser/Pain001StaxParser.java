package com.iso20022.pain.parser;

import com.iso20022.pain.arrow.ArrowBatchResult;
import com.iso20022.pain.arrow.Pain001ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming StAX parser for ISO 20022 pain.001.001.09 XML files.
 * <p>
 * Parses the XML using a pull-based StAX approach (no DOM, no JAXB) and writes
 * values <b>directly</b> into three Apache Arrow {@link VectorSchemaRoot}
 * tables:
 * </p>
 * <ol>
 * <li><b>Message</b> — one row per GrpHdr (GroupHeader85)</li>
 * <li><b>Remittance</b> — one row per PmtInf (PaymentInstruction30),
 * batched</li>
 * <li><b>Transaction</b> — one row per CdtTrfTxInf
 * (CreditTransferTransaction34), batched</li>
 * </ol>
 * <p>
 * No intermediate POJOs are created; XML text content is pushed straight into
 * Arrow vectors as it is encountered. Batching keeps memory bounded.
 * </p>
 */
public final class Pain001StaxParser {

    private static final Logger LOG = LoggerFactory.getLogger(Pain001StaxParser.class);

    /** Number of rows per Arrow batch for remittance and transaction tables. */
    private static final int BATCH_SIZE = 65_536;

    private final BufferAllocator allocator;
    private final Schema messageSchema;
    private final Schema remittanceSchema;
    private final Schema transactionSchema;

    public Pain001StaxParser(BufferAllocator allocator) {
        this.allocator = allocator;
        this.messageSchema = Pain001ArrowSchema.createMessageSchema();
        this.remittanceSchema = Pain001ArrowSchema.createRemittanceSchema();
        this.transactionSchema = Pain001ArrowSchema.createTransactionSchema();
    }

    /**
     * Parses a pain.001.001.09 XML file into three Arrow tables.
     *
     * @param xmlFile path to the XML file
     * @return an {@link ArrowBatchResult} containing message, remittance, and
     *         transaction tables
     * @throws IOException        if the file cannot be read
     * @throws XMLStreamException if XML parsing fails
     */
    public ArrowBatchResult parse(Path xmlFile) throws IOException, XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);

        try (InputStream fis = new BufferedInputStream(
                Files.newInputStream(xmlFile), 1024 * 1024)) {

            XMLStreamReader reader = factory.createXMLStreamReader(fis, "UTF-8");
            try {
                return doParse(reader);
            } finally {
                reader.close();
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Core parsing state machine
    // ═══════════════════════════════════════════════════════════════════════════

    private ArrowBatchResult doParse(XMLStreamReader reader) throws XMLStreamException {

        // ── Message table (single root, typically 1 row) ────────────────────
        VectorSchemaRoot msgRoot = VectorSchemaRoot.create(messageSchema, allocator);
        msgRoot.allocateNew();
        int msgRow = 0;

        // ── Remittance table (batched) ──────────────────────────────────────
        List<VectorSchemaRoot> rmtBatches = new ArrayList<>();
        VectorSchemaRoot rmtRoot = VectorSchemaRoot.create(remittanceSchema, allocator);
        rmtRoot.allocateNew();
        int rmtRow = 0;

        // ── Transaction table (batched) ─────────────────────────────────────
        List<VectorSchemaRoot> txBatches = new ArrayList<>();
        VectorSchemaRoot txRoot = VectorSchemaRoot.create(transactionSchema, allocator);
        txRoot.allocateNew();
        int txRow = 0;

        // ── Parsing state ───────────────────────────────────────────────────
        // Current GrpHdr values (carried as FK into remittance rows)
        String currentMsgId = null;

        // Current PmtInf values (carried as FK into transaction rows)
        String currentPmtInfId = null;

        // Temporary holders for element text being accumulated
        StringBuilder textContent = new StringBuilder();

        // ── Depth flags (O(1) instead of scanning an ArrayList) ─────────
        boolean inGrpHdr = false;
        boolean inPmtInf = false;
        boolean inCdtTrfTxInf = false;
        boolean inDbtrAgt = false;
        boolean inCdtrAgt = false;
        boolean inDbtrAcct = false;
        boolean inCdtrAcct = false;
        boolean inSvcLvl = false;
        boolean inReqdExctnDt = false;
        boolean inDbtr = false; // direct Dbtr under PmtInf
        boolean inCdtr = false; // direct Cdtr under CdtTrfTxInf
        boolean inRgltryRptg = false; // RgltryRptg under CdtTrfTxInf
        boolean inRgltryRptgDtls = false; // Dtls under RgltryRptg

        // Temporary accumulators for current PmtInf fields
        String pmtMtd = null;
        String pmtNbOfTxs = null;
        BigDecimal pmtCtrlSum = null;
        String pmtSvcLvlCd = null;
        String pmtReqdExctnDt = null;
        String pmtDbtrNm = null;
        String pmtDbtrAcctIban = null;
        String pmtDbtrAgtBicfi = null;

        // GrpHdr accumulators
        String grpCreDtTm = null;
        String grpNbOfTxs = null;
        BigDecimal grpCtrlSum = null;
        String grpInitgPtyNm = null;

        // CdtTrfTxInf accumulators
        String txInstrId = null;
        String txEndToEndId = null;
        BigDecimal txInstdAmt = null;
        String txCcy = null;
        String txCdtrAgtBicfi = null;
        String txCdtrNm = null;
        String txCdtrAcctIban = null;
        StringBuilder txRmtInfUstrd = null;   // accumulates multiple Ustrd lines
        StringBuilder txRgltyRptgCd = null;   // accumulates multiple RgltryRptg/Dtls/Cd values

        long totalTx = 0;
        long totalRmt = 0;

        while (reader.hasNext()) {
            int event = reader.next();

            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    String localName = reader.getLocalName();
                    textContent.setLength(0);

                    // Set depth flags — O(1) instead of list scanning
                    switch (localName) {
                        case "GrpHdr" -> inGrpHdr = true;
                        case "PmtInf" -> inPmtInf = true;
                        case "CdtTrfTxInf" -> inCdtTrfTxInf = true;
                        case "DbtrAgt" -> inDbtrAgt = true;
                        case "CdtrAgt" -> inCdtrAgt = true;
                        case "DbtrAcct" -> inDbtrAcct = true;
                        case "CdtrAcct" -> inCdtrAcct = true;
                        case "SvcLvl" -> inSvcLvl = true;
                        case "ReqdExctnDt" -> inReqdExctnDt = true;
                        case "Dbtr" -> {
                            if (inPmtInf && !inCdtTrfTxInf)
                                inDbtr = true;
                        }
                        case "Cdtr" -> {
                            if (inCdtTrfTxInf)
                                inCdtr = true;
                        }
                        case "RgltryRptg" -> {
                            if (inCdtTrfTxInf)
                                inRgltryRptg = true;
                        }
                        case "Dtls" -> {
                            if (inRgltryRptg)
                                inRgltryRptgDtls = true;
                        }
                        case "InstdAmt" -> txCcy = reader.getAttributeValue(null, "Ccy");
                        default -> {
                        }
                    }
                }

                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA ->
                    textContent.append(reader.getText());

                case XMLStreamConstants.END_ELEMENT -> {
                    String localName = reader.getLocalName();

                    switch (localName) {
                        // ── GroupHeader fields → accumulate ──────────────
                        case "MsgId" -> {
                            if (inGrpHdr)
                                currentMsgId = textContent.toString().trim();
                        }
                        case "CreDtTm" -> {
                            if (inGrpHdr)
                                grpCreDtTm = textContent.toString().trim();
                        }
                        case "NbOfTxs" -> {
                            if (inGrpHdr && !inPmtInf)
                                grpNbOfTxs = textContent.toString().trim();
                            else if (inPmtInf && !inCdtTrfTxInf)
                                pmtNbOfTxs = textContent.toString().trim();
                        }
                        case "CtrlSum" -> {
                            String t = textContent.toString().trim();
                            if (inGrpHdr && !inPmtInf)
                                grpCtrlSum = new BigDecimal(t);
                            else if (inPmtInf && !inCdtTrfTxInf)
                                pmtCtrlSum = new BigDecimal(t);
                        }
                        case "Nm" -> {
                            if (inGrpHdr && !inPmtInf)
                                grpInitgPtyNm = textContent.toString().trim();
                            else if (inCdtTrfTxInf && inCdtr)
                                txCdtrNm = textContent.toString().trim();
                            else if (inPmtInf && !inCdtTrfTxInf && inDbtr)
                                pmtDbtrNm = textContent.toString().trim();
                        }

                        // ── GrpHdr end → write message row ──────────────
                        case "GrpHdr" -> {
                            setVarChar(msgRoot, Pain001ArrowSchema.MSG_ID, msgRow, currentMsgId);
                            setVarChar(msgRoot, Pain001ArrowSchema.MSG_CRE_DT_TM, msgRow, grpCreDtTm);
                            setVarChar(msgRoot, Pain001ArrowSchema.MSG_NB_OF_TXS, msgRow, grpNbOfTxs);
                            setDecimal(msgRoot, Pain001ArrowSchema.MSG_CTRL_SUM, msgRow, grpCtrlSum);
                            setVarChar(msgRoot, Pain001ArrowSchema.MSG_INITG_PTY_NM, msgRow, grpInitgPtyNm);
                            msgRow++;
                            inGrpHdr = false;
                        }

                        // ── PaymentInformation fields → accumulate ──────
                        case "PmtInfId" -> {
                            if (inPmtInf && !inCdtTrfTxInf)
                                currentPmtInfId = textContent.toString().trim();
                        }
                        case "PmtMtd" -> {
                            if (inPmtInf)
                                pmtMtd = textContent.toString().trim();
                        }
                        case "Cd" -> {
                            if (inPmtInf && !inCdtTrfTxInf && inSvcLvl)
                                pmtSvcLvlCd = textContent.toString().trim();
                            else if (inCdtTrfTxInf && inRgltryRptg && inRgltryRptgDtls) {
                                String code = textContent.toString().trim();
                                if (!code.isEmpty()) {
                                    if (txRgltyRptgCd == null)
                                        txRgltyRptgCd = new StringBuilder(code);
                                    else
                                        txRgltyRptgCd.append('\n').append(code);
                                }
                            }
                        }
                        case "Dt" -> {
                            if (inPmtInf && !inCdtTrfTxInf && inReqdExctnDt)
                                pmtReqdExctnDt = textContent.toString().trim();
                        }
                        case "IBAN" -> {
                            if (inCdtTrfTxInf && inCdtrAcct)
                                txCdtrAcctIban = textContent.toString().trim();
                            else if (inPmtInf && !inCdtTrfTxInf && inDbtrAcct)
                                pmtDbtrAcctIban = textContent.toString().trim();
                        }
                        case "BICFI" -> {
                            if (inCdtTrfTxInf && inCdtrAgt)
                                txCdtrAgtBicfi = textContent.toString().trim();
                            else if (inPmtInf && !inCdtTrfTxInf && inDbtrAgt)
                                pmtDbtrAgtBicfi = textContent.toString().trim();
                        }

                        // ── Container end → clear depth flags ───────────
                        case "SvcLvl" -> inSvcLvl = false;
                        case "ReqdExctnDt" -> inReqdExctnDt = false;
                        case "DbtrAgt" -> inDbtrAgt = false;
                        case "CdtrAgt" -> inCdtrAgt = false;
                        case "DbtrAcct" -> inDbtrAcct = false;
                        case "CdtrAcct" -> inCdtrAcct = false;
                        case "Dbtr" -> {
                            if (inPmtInf && !inCdtTrfTxInf)
                                inDbtr = false;
                        }
                        case "Cdtr" -> {
                            if (inCdtTrfTxInf)
                                inCdtr = false;
                        }
                        case "Dtls" -> {
                            if (inRgltryRptg)
                                inRgltryRptgDtls = false;
                        }
                        case "RgltryRptg" -> {
                            if (inCdtTrfTxInf)
                                inRgltryRptg = false;
                        }

                        // ── PmtInf end → write remittance row ───────────
                        case "PmtInf" -> {
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_MSG_ID, rmtRow, currentMsgId);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_PMT_INF_ID, rmtRow, currentPmtInfId);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_PMT_MTD, rmtRow, pmtMtd);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_NB_OF_TXS, rmtRow, pmtNbOfTxs);
                            setDecimal(rmtRoot, Pain001ArrowSchema.RMT_CTRL_SUM, rmtRow, pmtCtrlSum);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_SVC_LVL_CD, rmtRow, pmtSvcLvlCd);
                            setDateDay(rmtRoot, Pain001ArrowSchema.RMT_REQD_EXCTN_DT, rmtRow, pmtReqdExctnDt);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_NM, rmtRow, pmtDbtrNm);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN, rmtRow, pmtDbtrAcctIban);
                            setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_AGT_BICFI, rmtRow, pmtDbtrAgtBicfi);
                            rmtRow++;
                            totalRmt++;

                            // Flush remittance batch if full
                            if (rmtRow >= BATCH_SIZE) {
                                rmtRoot.setRowCount(rmtRow);
                                rmtBatches.add(rmtRoot);
                                rmtRoot = VectorSchemaRoot.create(remittanceSchema, allocator);
                                rmtRoot.allocateNew();
                                rmtRow = 0;
                            }

                            // Reset PmtInf accumulators and depth flag
                            inPmtInf = false;
                            currentPmtInfId = null;
                            pmtMtd = null;
                            pmtNbOfTxs = null;
                            pmtCtrlSum = null;
                            pmtSvcLvlCd = null;
                            pmtReqdExctnDt = null;
                            pmtDbtrNm = null;
                            pmtDbtrAcctIban = null;
                            pmtDbtrAgtBicfi = null;
                        }

                        // ── CdtTrfTxInf fields → accumulate ────────────
                        case "InstrId" -> {
                            if (inCdtTrfTxInf)
                                txInstrId = textContent.toString().trim();
                        }
                        case "EndToEndId" -> {
                            if (inCdtTrfTxInf)
                                txEndToEndId = textContent.toString().trim();
                        }
                        case "InstdAmt" -> {
                            if (inCdtTrfTxInf) {
                                String t = textContent.toString().trim();
                                if (!t.isEmpty())
                                    txInstdAmt = new BigDecimal(t);
                            }
                        }
                        case "Ustrd" -> {
                            if (inCdtTrfTxInf) {
                                String line = textContent.toString().trim();
                                if (!line.isEmpty()) {
                                    if (txRmtInfUstrd == null) {
                                        txRmtInfUstrd = new StringBuilder(line);
                                    } else {
                                        txRmtInfUstrd.append('\n').append(line);
                                    }
                                }
                            }
                        }

                        // ── CdtTrfTxInf end → write transaction row ────
                        case "CdtTrfTxInf" -> {
                            setVarChar(txRoot, Pain001ArrowSchema.TX_PMT_INF_ID, txRow, currentPmtInfId);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_INSTR_ID, txRow, txInstrId);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_END_TO_END_ID, txRow, txEndToEndId);
                            setDecimal(txRoot, Pain001ArrowSchema.TX_INSTD_AMT, txRow, txInstdAmt);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_CCY, txRow, txCcy);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_AGT_BICFI, txRow, txCdtrAgtBicfi);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_NM, txRow, txCdtrNm);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_ACCT_IBAN, txRow, txCdtrAcctIban);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_RMT_INF_USTRD, txRow,
                                    txRmtInfUstrd != null ? txRmtInfUstrd.toString() : null);
                            setVarChar(txRoot, Pain001ArrowSchema.TX_RGLTY_RPTG_CD, txRow,
                                    txRgltyRptgCd != null ? txRgltyRptgCd.toString() : null);
                            txRow++;
                            totalTx++;

                            // Reset tx accumulators and depth flag
                            txInstrId = null;
                            txEndToEndId = null;
                            txInstdAmt = null;
                            txCcy = null;
                            txCdtrAgtBicfi = null;
                            txCdtrNm = null;
                            txCdtrAcctIban = null;
                            txRmtInfUstrd = null;
                            txRgltyRptgCd = null;
                            inCdtTrfTxInf = false;

                            // Flush transaction batch if full
                            if (txRow >= BATCH_SIZE) {
                                txRoot.setRowCount(txRow);
                                txBatches.add(txRoot);
                                LOG.debug("Flushed transaction batch ({} total tx)", totalTx);
                                txRoot = VectorSchemaRoot.create(transactionSchema, allocator);
                                txRoot.allocateNew();
                                txRow = 0;
                            }
                        }

                        default -> {
                            // ignore other elements
                        }
                    }

                    textContent.setLength(0);
                }
            }
        }

        // ── Flush remaining partial batches ─────────────────────────────────
        msgRoot.setRowCount(msgRow);

        if (rmtRow > 0) {
            rmtRoot.setRowCount(rmtRow);
            rmtBatches.add(rmtRoot);
        } else {
            rmtRoot.close();
        }

        if (txRow > 0) {
            txRoot.setRowCount(txRow);
            txBatches.add(txRoot);
        } else {
            txRoot.close();
        }

        LOG.info("Parsing complete: {} messages, {} remittances, {} transactions",
                msgRow, totalRmt, totalTx);

        return new ArrowBatchResult(msgRoot, rmtBatches, txBatches);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Arrow vector writers — write directly into vectors, no POJOs
    // ═══════════════════════════════════════════════════════════════════════════

    private static void setVarChar(VectorSchemaRoot root, String fieldName,
            int index, String value) {
        VarCharVector vec = (VarCharVector) root.getVector(fieldName);
        if (value != null) {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vec.setNull(index);
        }
    }

    private static void setDecimal(VectorSchemaRoot root, String fieldName,
            int index, BigDecimal value) {
        DecimalVector vec = (DecimalVector) root.getVector(fieldName);
        if (value != null) {
            // Arrow requires exact scale match — rescale to the vector's declared scale
            int vectorScale = vec.getScale();
            vec.setSafe(index, value.setScale(vectorScale, java.math.RoundingMode.HALF_UP));
        } else {
            vec.setNull(index);
        }
    }

    private static void setDateDay(VectorSchemaRoot root, String fieldName,
            int index, String isoDateStr) {
        DateDayVector vec = (DateDayVector) root.getVector(fieldName);
        if (isoDateStr != null) {
            LocalDate date = LocalDate.parse(isoDateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            vec.setSafe(index, (int) date.toEpochDay());
        } else {
            vec.setNull(index);
        }
    }
}
