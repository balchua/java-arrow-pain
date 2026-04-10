package com.pgw.purearrow;

import com.pgw.parser.ParseStats;

import java.nio.file.Path;

/**
 * Result of a pure-Arrow ingest operation.
 *
 * @param parseStats      lightweight parse statistics (row counts per table)
 * @param store           in-memory store holding all accumulated Arrow record batches;
 *                        caller must call {@link PureArrowInMemoryStore#close()} when done
 * @param messageFile     path to the message Arrow IPC stream file ({@code *_message.arrow})
 * @param remittanceFile  path to the remittance Arrow IPC stream file ({@code *_remittance.arrow})
 * @param transactionFile path to the transaction Arrow IPC stream file ({@code *_transaction.arrow})
 */
public record PureArrowIngestResult(
        ParseStats parseStats,
        PureArrowInMemoryStore store,
        Path messageFile,
        Path remittanceFile,
        Path transactionFile
) {}
