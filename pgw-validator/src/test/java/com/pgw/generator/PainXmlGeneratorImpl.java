package com.pgw.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * StAX-based implementation of {@link PainXmlGenerator} for ISO 20022 pain.001.001.09 XML files.
 * <p>
 * Supports deliberately invalid control sums via {@link PainFileSpec#invalidControlSum()}.
 * </p>
 */
public final class PainXmlGeneratorImpl implements PainXmlGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(PainXmlGeneratorImpl.class);

    private static final String ISO20022_NAMESPACE = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09";
    private static final String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";

    private static final DateTimeFormatter ISO_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter ISO_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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

    public PainXmlGeneratorImpl() {}

    /**
     * Generates a pain.001.001.09 XML file (compact / minified).
     *
     * @param spec      generation parameters
     * @param outputDir directory in which to write the file
     * @return path of the generated file
     * @throws IOException        on I/O failure
     * @throws XMLStreamException on XML write failure
     */
    @Override
    public Path generate(PainFileSpec spec, Path outputDir)
            throws IOException, XMLStreamException {

        Files.createDirectories(outputDir);
        Path outputFile = outputDir.resolve(spec.fileName());

        LOG.info("Generating {} -> {}", spec.name(), outputFile);
        long startTime = System.currentTimeMillis();

        XMLOutputFactory factory = XMLOutputFactory.newInstance();

        try (OutputStream fileOut = new BufferedOutputStream(
                Files.newOutputStream(outputFile), 1024 * 1024)) {

            XMLStreamWriter writer = factory.createXMLStreamWriter(fileOut, "UTF-8");
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

    private static void writeDocument(XMLStreamWriter writer, PainFileSpec spec)
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

    private static void writeGroupHeader(XMLStreamWriter writer, PainFileSpec spec)
            throws XMLStreamException {

        long totalTxns = spec.totalTransactions();
        String ctrlSumStr;
        if (spec.invalidControlSum()) {
            ctrlSumStr = "99999.99";
        } else {
            BigDecimal controlSum = new BigDecimal("100.00")
                    .multiply(BigDecimal.valueOf(totalTxns))
                    .setScale(2, RoundingMode.HALF_UP);
            ctrlSumStr = controlSum.toPlainString();
        }

        writer.writeStartElement("GrpHdr");

        writeElement(writer, "MsgId", "MSG-" + System.currentTimeMillis());
        writeElement(writer, "CreDtTm",
                LocalDateTime.now().format(ISO_DATETIME_FORMAT));
        writeElement(writer, "NbOfTxs", String.valueOf(totalTxns));
        writeElement(writer, "CtrlSum", ctrlSumStr);

        writer.writeStartElement("InitgPty");
        writeElement(writer, "Nm", "Sample Initiating Party Corp");
        writer.writeEndElement(); // InitgPty

        writer.writeEndElement(); // GrpHdr
    }

    private static void writePaymentInformation(XMLStreamWriter writer,
            PainFileSpec spec,
            int pmtIndex,
            BigDecimal txAmount)
            throws XMLStreamException {

        int txCount = spec.transactionsPerBlock();
        String blockCtrlSumStr;
        if (spec.invalidControlSum()) {
            blockCtrlSumStr = "0.01";
        } else {
            BigDecimal blockCtrlSum = txAmount.multiply(BigDecimal.valueOf(txCount))
                    .setScale(2, RoundingMode.HALF_UP);
            blockCtrlSumStr = blockCtrlSum.toPlainString();
        }

        String debtorIban = DEBTOR_IBANS[pmtIndex % DEBTOR_IBANS.length];
        String debtorBic = DEBTOR_BICS[pmtIndex % DEBTOR_BICS.length];

        writer.writeStartElement("PmtInf");

        writeElement(writer, "PmtInfId",
                String.format("PMT-%06d", pmtIndex + 1));
        writeElement(writer, "PmtMtd", "TRF");
        writeElement(writer, "NbOfTxs", String.valueOf(txCount));
        writeElement(writer, "CtrlSum", blockCtrlSumStr);

        writer.writeStartElement("PmtTpInf");
        writer.writeStartElement("SvcLvl");
        writeElement(writer, "Cd", "SEPA");
        writer.writeEndElement(); // SvcLvl
        writer.writeEndElement(); // PmtTpInf

        writer.writeStartElement("ReqdExctnDt");
        writeElement(writer, "Dt", LocalDate.now().plusDays(1).format(ISO_DATE_FORMAT));
        writer.writeEndElement(); // ReqdExctnDt

        writer.writeStartElement("Dbtr");
        writeElement(writer, "Nm", "Debtor Company " + (pmtIndex + 1));
        writer.writeEndElement(); // Dbtr

        writer.writeStartElement("DbtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", debtorIban);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // DbtrAcct

        writer.writeStartElement("DbtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", debtorBic);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // DbtrAgt

        for (int txIdx = 0; txIdx < txCount; txIdx++) {
            writeCreditTransferTransaction(writer, pmtIndex, txIdx, txAmount);
        }

        writer.writeEndElement(); // PmtInf
    }

    private static void writeCreditTransferTransaction(XMLStreamWriter writer,
            int pmtIndex,
            int txIndex,
            BigDecimal amount)
            throws XMLStreamException {

        int credIdx = txIndex % CREDITOR_NAMES.length;

        writer.writeStartElement("CdtTrfTxInf");

        writer.writeStartElement("PmtId");
        writeElement(writer, "InstrId",
                String.format("INSTR-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "EndToEndId",
                String.format("E2E-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writer.writeEndElement(); // PmtId

        writer.writeStartElement("Amt");
        writer.writeStartElement("InstdAmt");
        writer.writeAttribute("Ccy", "EUR");
        writer.writeCharacters(amount.toPlainString());
        writer.writeEndElement(); // InstdAmt
        writer.writeEndElement(); // Amt

        writer.writeStartElement("CdtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", CREDITOR_BICS[credIdx]);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // CdtrAgt

        writer.writeStartElement("Cdtr");
        writeElement(writer, "Nm", CREDITOR_NAMES[credIdx]);
        writer.writeEndElement(); // Cdtr

        writer.writeStartElement("CdtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", CREDITOR_IBANS[credIdx]);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // CdtrAcct

        writer.writeStartElement("RmtInf");
        writeElement(writer, "Ustrd",
                String.format("Invoice PMT%06d-TX%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "Ustrd",
                String.format("Ref %s/%s", CREDITOR_NAMES[credIdx].replaceAll("\\s+", "-"), txIndex + 1));
        writer.writeEndElement(); // RmtInf

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

    private static void writeElement(XMLStreamWriter writer, String localName, String text)
            throws XMLStreamException {
        writer.writeStartElement(localName);
        writer.writeCharacters(text);
        writer.writeEndElement();
    }
}
