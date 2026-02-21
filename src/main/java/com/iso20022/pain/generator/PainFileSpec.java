package com.iso20022.pain.generator;

/**
 * Specifies a pain.001.001.09 file to generate.
 *
 * @param name                      human-readable label
 * @param fileName                  output file name
 * @param numberOfPaymentInfoBlocks number of PmtInf blocks
 * @param transactionsPerBlock      number of CdtTrfTxInf per PmtInf block
 * @param invalidControlSum         if true, GrpHdr CtrlSum is deliberately wrong
 *                                  (for negative test scenarios)
 */
public record PainFileSpec(
        String name,
        String fileName,
        int    numberOfPaymentInfoBlocks,
        int    transactionsPerBlock,
        boolean invalidControlSum) {

    /** Convenience constructor — valid control sum by default. */
    public PainFileSpec(String name, String fileName,
                        int numberOfPaymentInfoBlocks, int transactionsPerBlock) {
        this(name, fileName, numberOfPaymentInfoBlocks, transactionsPerBlock, false);
    }

    public long totalTransactions() {
        return (long) numberOfPaymentInfoBlocks * transactionsPerBlock;
    }
}
