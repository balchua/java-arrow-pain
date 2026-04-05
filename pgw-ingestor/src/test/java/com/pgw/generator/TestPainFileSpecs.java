package com.pgw.generator;

/**
 * Test-only pain.001 file specifications.
 * All production file specs are defined here to avoid polluting PainFileSpec.
 */
public final class TestPainFileSpecs {

    private TestPainFileSpecs() {}

    /** Type A: 1 PmtInf × 1,000,000 CdtTrfTxInf — fat single batch. */
    public static final PainFileSpec TYPE_A = new PainFileSpec(
            "Type A (1 PmtInf × 1,000,000 TxInf)",
            "pain001_type_a_1x1M.xml", 1, 1_000_000);

    /** Type B: 2 PmtInf × 500,000 CdtTrfTxInf each. */
    public static final PainFileSpec TYPE_B = new PainFileSpec(
            "Type B (2 PmtInf × 500,000 TxInf)",
            "pain001_type_b_2x500K.xml", 2, 500_000);

    /** Type C: 1,000,000 PmtInf × 1 CdtTrfTxInf — many small remittances. */
    public static final PainFileSpec TYPE_C = new PainFileSpec(
            "Type C (1,000,000 PmtInf × 1 TxInf)",
            "pain001_type_c_1Mx1.xml", 1_000_000, 1);

    /** Type D: 2 PmtInf × 100 CdtTrfTxInf — small valid file for fast tests. */
    public static final PainFileSpec TYPE_D = new PainFileSpec(
            "Type D (2 PmtInf × 100 TxInf — valid)",
            "pain001_type_d_2x100_valid.xml", 2, 100);

    /** Type E: 2 PmtInf × 100 CdtTrfTxInf — invalid control sum. */
    public static final PainFileSpec TYPE_E = new PainFileSpec(
            "Type E (2 PmtInf × 100 TxInf — invalid CtrlSum)",
            "pain001_type_e_2x100_invalid_ctrlsum.xml", 2, 100, true);
}
