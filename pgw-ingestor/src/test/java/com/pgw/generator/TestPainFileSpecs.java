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

    /** Type F: 1 PmtInf × 2,000,000 CdtTrfTxInf — single remittance, 2M transactions. */
    public static final PainFileSpec TYPE_F = new PainFileSpec(
            "Type F (1 PmtInf × 2,000,000 TxInf)",
            "pain001_type_f_1x2M.xml", 1, 2_000_000);

    /** Type G: 1 PmtInf × 4,000,000 CdtTrfTxInf — single remittance, 4M transactions. */
    public static final PainFileSpec TYPE_G = new PainFileSpec(
            "Type G (1 PmtInf × 4,000,000 TxInf)",
            "pain001_type_g_1x4M.xml", 1, 4_000_000);

    /** Type H: 10 PmtInf × 200 CdtTrfTxInf — 10 remittances, 200 transactions each (2000 total). */
    public static final PainFileSpec TYPE_H = new PainFileSpec(
            "Type H (10 PmtInf × 200 TxInf)",
            "pain001_type_h_10x200.xml", 10, 200);

    /** Type J: 5 PmtInf × 400 CdtTrfTxInf — 5 remittances, 2000 transactions total. */
    public static final PainFileSpec TYPE_J = new PainFileSpec(
            "Type J (5 PmtInf × 400 TxInf)",
            "pain001_type_j_5x400.xml", 5, 400);
}
