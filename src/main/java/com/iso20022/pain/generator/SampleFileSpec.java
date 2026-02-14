package com.iso20022.pain.generator;

/**
 * Defines the three sample file configurations for pain.001.001.09 generation.
 *
 * @param name                      descriptive name for the file type
 * @param fileName                  output file name
 * @param numberOfPaymentInfoBlocks number of PmtInf (remittance) blocks
 * @param transactionsPerBlock      number of CdtTrfTxInf per PmtInf block
 */
public record SampleFileSpec(
        String name,
        String fileName,
        int numberOfPaymentInfoBlocks,
        int transactionsPerBlock) {

    /** Type A: 1 message, 1 remittance, 1,000,000 transactions */
    public static final SampleFileSpec TYPE_A = new SampleFileSpec(
            "Type A (1 PmtInf × 1,000,000 TxInf)",
            "pain001_type_a_1x1M.xml",
            1,
            1_000_000);

    /** Type B: 1 message, 2 remittances, 500,000 transactions each */
    public static final SampleFileSpec TYPE_B = new SampleFileSpec(
            "Type B (2 PmtInf × 500,000 TxInf)",
            "pain001_type_b_2x500K.xml",
            2,
            500_000);

    /** Type C: 1 message, 1,000,000 remittances, 1 transaction each */
    public static final SampleFileSpec TYPE_C = new SampleFileSpec(
            "Type C (1,000,000 PmtInf × 1 TxInf)",
            "pain001_type_c_1Mx1.xml",
            1_000_000,
            1);

    /**
     * Returns the total number of transactions across all blocks.
     */
    public long totalTransactions() {
        return (long) numberOfPaymentInfoBlocks * transactionsPerBlock;
    }
}
