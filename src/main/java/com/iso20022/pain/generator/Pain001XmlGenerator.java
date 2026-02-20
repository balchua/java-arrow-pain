package com.iso20022.pain.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Generates ISO 20022 pain.001.001.09 (CustomerCreditTransferInitiationV09) XML
 * files
 * using StAX (XMLStreamWriter) for memory-efficient streaming output.
 * <p>
 * The generator produces valid XML with the correct ISO 20022 namespace and
 * element
 * hierarchy. All data values conform to the ISO 20022 data type constraints.
 * </p>
 */
public final class Pain001XmlGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(Pain001XmlGenerator.class);

    private static final String ISO20022_NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09";
    private static final String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";

    private static final DateTimeFormatter ISO_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter ISO_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // Sample IBAN and BIC values for realistic data
    private static final String[] DEBTOR_IBANS = {
            "DE89370400440532013000", "FR7630006000011234567890189",
            "GB29NWBK60161331926819", "NL91ABNA0417164300"
    };
    private static final String[] DEBTOR_BICS = {
            "COBADEFFXXX", "BNPAFRPPXXX", "NWBKGB2LXXX", "ABNANL2AXXX"
    };
    private static final String[] CREDITOR_IBANS = {
            "IT60X0542811101000000123456", "ES9121000418450200051332",
            "AT611904300234573201", "BE68539007547034",
            "CH9300762011623852957", "PT50000201231234567890154"
    };
    private static final String[] CREDITOR_BICS = {
            "BPPIITRRXXX", "CABORKMRXXX", "BKAUATWWXXX", "BBRUBEBB010",
            "UBSWCHZH80A", "CGDIPTPLXXX"
    };
    private static final String[] CREDITOR_NAMES = {
            "Acme Corporation", "Global Industries Ltd", "Smith Trading Co",
            "EuroPayments GmbH", "Nordic Supplies AB", "Atlantic Services SA"
    };
    private static final String[] REGULATORY_CODES = {
            "BENEFICIARY_RESIDENT", "RESIDENT", "NONRESIDENT", "REGLF001", "REGLF002"
    };

    private Pain001XmlGenerator() {
        // utility class
    }

    /**
     * Generates a pain.001.001.09 XML file (compact / minified).
     */
    public static Path generate(SampleFileSpec spec, Path outputDir)
            throws IOException, XMLStreamException {
        return generate(spec, outputDir, false);
    }

    /**
     * Generates a pain.001.001.09 XML file based on the given specification.
     *
     * @param spec        the file specification (number of payment blocks and
     *                    transactions)
     * @param outputDir   the directory where the file will be written
     * @param prettyPrint if {@code true}, the XML output will be indented (2-space)
     * @return the path of the generated file
     * @throws IOException        if an I/O error occurs
     * @throws XMLStreamException if an XML writing error occurs
     */
    public static Path generate(SampleFileSpec spec, Path outputDir, boolean prettyPrint)
            throws IOException, XMLStreamException {

        Files.createDirectories(outputDir);
        Path outputFile = outputDir.resolve(spec.fileName());

        // Skip generation if file already exists and is complete
        if (Files.exists(outputFile) && isFileComplete(outputFile)) {
            long fileSizeMb = Files.size(outputFile) / (1024 * 1024);
            LOG.info("Skipping {} — already exists and complete ({} MB)",
                    spec.name(), fileSizeMb);
            return outputFile;
        }

        LOG.info("Generating {} -> {}", spec.name(), outputFile);
        long startTime = System.currentTimeMillis();

        XMLOutputFactory factory = XMLOutputFactory.newInstance();

        try (OutputStream fileOut = new BufferedOutputStream(
                Files.newOutputStream(outputFile), 1024 * 1024)) {

            XMLStreamWriter raw = factory.createXMLStreamWriter(fileOut, "UTF-8");
            XMLStreamWriter writer = prettyPrint ? new IndentingXMLStreamWriter(raw) : raw;
            try {
                writeDocument(writer, spec);
            } finally {
                writer.close();
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        long fileSizeMb = Files.size(outputFile) / (1024 * 1024);
        LOG.info("Generated {} in {} ms ({} MB)", spec.name(), elapsed, fileSizeMb);

        return outputFile;
    }

    private static void writeDocument(XMLStreamWriter writer, SampleFileSpec spec)
            throws XMLStreamException {

        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("Document");
        writer.writeNamespace("", ISO20022_NAMESPACE);
        writer.writeNamespace("xsi", XSI_NAMESPACE);

        writer.writeStartElement("CstmrCdtTrfInitn");

        writeGroupHeader(writer, spec);

        BigDecimal txAmount = new BigDecimal("100.00");
        for (int pmtIdx = 0; pmtIdx < spec.numberOfPaymentInfoBlocks(); pmtIdx++) {
            writePaymentInformation(writer, spec, pmtIdx, txAmount);
        }

        writer.writeEndElement(); // CstmrCdtTrfInitn
        writer.writeEndElement(); // Document
        writer.writeEndDocument();
        writer.flush();
    }

    /**
     * Writes the GrpHdr (GroupHeader85) element.
     */
    private static void writeGroupHeader(XMLStreamWriter writer, SampleFileSpec spec)
            throws XMLStreamException {

        long totalTxns = spec.totalTransactions();
        BigDecimal controlSum = new BigDecimal("100.00")
                .multiply(BigDecimal.valueOf(totalTxns))
                .setScale(2, RoundingMode.HALF_UP);

        writer.writeStartElement("GrpHdr");

        writeElement(writer, "MsgId", "MSG-" + System.currentTimeMillis());
        writeElement(writer, "CreDtTm",
                LocalDateTime.now().format(ISO_DATETIME_FORMAT));
        writeElement(writer, "NbOfTxs", String.valueOf(totalTxns));
        writeElement(writer, "CtrlSum", controlSum.toPlainString());

        writer.writeStartElement("InitgPty");
        writeElement(writer, "Nm", "Sample Initiating Party Corp");
        writer.writeEndElement(); // InitgPty

        writer.writeEndElement(); // GrpHdr
    }

    /**
     * Writes a single PmtInf (PaymentInstruction30) element with all its
     * CdtTrfTxInf children.
     */
    private static void writePaymentInformation(XMLStreamWriter writer,
            SampleFileSpec spec,
            int pmtIndex,
            BigDecimal txAmount)
            throws XMLStreamException {

        int txCount = spec.transactionsPerBlock();
        BigDecimal blockCtrlSum = txAmount.multiply(BigDecimal.valueOf(txCount))
                .setScale(2, RoundingMode.HALF_UP);

        String debtorIban = DEBTOR_IBANS[pmtIndex % DEBTOR_IBANS.length];
        String debtorBic = DEBTOR_BICS[pmtIndex % DEBTOR_BICS.length];

        writer.writeStartElement("PmtInf");

        writeElement(writer, "PmtInfId",
                String.format("PMT-%06d", pmtIndex + 1));
        writeElement(writer, "PmtMtd", "TRF");
        writeElement(writer, "NbOfTxs", String.valueOf(txCount));
        writeElement(writer, "CtrlSum", blockCtrlSum.toPlainString());

        // PmtTpInf/SvcLvl/Cd
        writer.writeStartElement("PmtTpInf");
        writer.writeStartElement("SvcLvl");
        writeElement(writer, "Cd", "SEPA");
        writer.writeEndElement(); // SvcLvl
        writer.writeEndElement(); // PmtTpInf

        // ReqdExctnDt/Dt
        writer.writeStartElement("ReqdExctnDt");
        writeElement(writer, "Dt", LocalDate.now().plusDays(1).format(ISO_DATE_FORMAT));
        writer.writeEndElement(); // ReqdExctnDt

        // Dbtr/Nm
        writer.writeStartElement("Dbtr");
        writeElement(writer, "Nm", "Debtor Company " + (pmtIndex + 1));
        writer.writeEndElement(); // Dbtr

        // DbtrAcct/Id/IBAN
        writer.writeStartElement("DbtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", debtorIban);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // DbtrAcct

        // DbtrAgt/FinInstnId/BICFI
        writer.writeStartElement("DbtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", debtorBic);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // DbtrAgt

        // Write all credit transfer transactions for this block
        for (int txIdx = 0; txIdx < txCount; txIdx++) {
            writeCreditTransferTransaction(writer, pmtIndex, txIdx, txAmount);
        }

        writer.writeEndElement(); // PmtInf
    }

    /**
     * Writes a single CdtTrfTxInf (CreditTransferTransaction34) element.
     */
    private static void writeCreditTransferTransaction(XMLStreamWriter writer,
            int pmtIndex,
            int txIndex,
            BigDecimal amount)
            throws XMLStreamException {

        int credIdx = txIndex % CREDITOR_NAMES.length;

        writer.writeStartElement("CdtTrfTxInf");

        // PmtId
        writer.writeStartElement("PmtId");
        writeElement(writer, "InstrId",
                String.format("INSTR-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "EndToEndId",
                String.format("E2E-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writer.writeEndElement(); // PmtId

        // Amt/InstdAmt with Ccy attribute
        writer.writeStartElement("Amt");
        writer.writeStartElement("InstdAmt");
        writer.writeAttribute("Ccy", "EUR");
        writer.writeCharacters(amount.toPlainString());
        writer.writeEndElement(); // InstdAmt
        writer.writeEndElement(); // Amt

        // CdtrAgt/FinInstnId/BICFI
        writer.writeStartElement("CdtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", CREDITOR_BICS[credIdx]);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // CdtrAgt

        // Cdtr/Nm
        writer.writeStartElement("Cdtr");
        writeElement(writer, "Nm", CREDITOR_NAMES[credIdx]);
        writer.writeEndElement(); // Cdtr

        // CdtrAcct/Id/IBAN
        writer.writeStartElement("CdtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", CREDITOR_IBANS[credIdx]);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // CdtrAcct

        // RmtInf — 2 Ustrd lines (ISO 20022 allows up to 140; realistic messages use a few)
        writer.writeStartElement("RmtInf");
        writeElement(writer, "Ustrd",
                String.format("Invoice PMT%06d-TX%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "Ustrd",
                String.format("Ref %s/%s", CREDITOR_NAMES[credIdx].replaceAll("\\s+", "-"), txIndex + 1));
        writer.writeEndElement(); // RmtInf

        // RgltryRptg — 2 Dtls/Cd entries (parser supports up to ~10; stored flat, newline-delimited)
        writer.writeStartElement("RgltryRptg");
        writer.writeStartElement("Dtls");
        writeElement(writer, "Cd", REGULATORY_CODES[txIndex % REGULATORY_CODES.length]);
        writer.writeEndElement(); // Dtls
        writer.writeStartElement("Dtls");
        writeElement(writer, "Cd", REGULATORY_CODES[(txIndex + 1) % REGULATORY_CODES.length]);
        writer.writeEndElement(); // Dtls
        writer.writeEndElement(); // RgltryRptg

        writer.writeEndElement(); // CdtTrfTxInf
    }

    /**
     * Writes a simple text element.
     */
    private static void writeElement(XMLStreamWriter writer, String localName, String text)
            throws XMLStreamException {
        writer.writeStartElement(localName);
        writer.writeCharacters(text);
        writer.writeEndElement();
    }

    /**
     * Fast check to determine if an existing XML file is complete.
     * Reads only the last few bytes to verify it ends with {@code </Document>}.
     * This avoids scanning the entire multi-hundred-MB file.
     *
     * @param xmlFile path to the XML file
     * @return true if the file appears to be a complete pain.001 document
     */
    private static boolean isFileComplete(Path xmlFile) {
        try {
            long size = Files.size(xmlFile);
            if (size < 100)
                return false; // too small to be valid

            // Read the last 64 bytes and check for closing </Document> tag
            try (RandomAccessFile raf = new RandomAccessFile(xmlFile.toFile(), "r")) {
                long seekPos = Math.max(0, size - 64);
                raf.seek(seekPos);
                byte[] tail = new byte[(int) (size - seekPos)];
                raf.readFully(tail);
                String tailStr = new String(tail, java.nio.charset.StandardCharsets.UTF_8);
                return tailStr.contains("</Document>");
            }
        } catch (IOException e) {
            LOG.warn("Could not verify file completeness for {}: {}", xmlFile, e.getMessage());
            return false; // force regeneration
        }
    }
}
