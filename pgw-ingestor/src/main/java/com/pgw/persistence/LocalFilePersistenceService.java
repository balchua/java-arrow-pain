package com.pgw.persistence;

import com.pgw.parser.BatchConsumer.TableType;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Writes three Arrow IPC <b>Stream</b> files ({@code .arrows} extension) — one per
 * table type — into a configurable directory using {@link ArrowStreamWriter}.
 *
 * <p>Files are created lazily on the first {@link #writeBatch} call for each
 * table type, so empty tables produce no file.</p>
 *
 * <p>Output file names: {@code {baseName}_message.arrows},
 * {@code {baseName}_remittance.arrows}, {@code {baseName}_transaction.arrows}.</p>
 */
public final class LocalFilePersistenceService implements PersistenceService {

    private final Path outputDir;
    private final String baseName;
    private final Schema messageSchema;
    private final Schema remittanceSchema;
    private final Schema transactionSchema;

    private ArrowStreamWriter messageWriter;
    private ArrowStreamWriter remittanceWriter;
    private ArrowStreamWriter transactionWriter;

    private OutputStream messageStream;
    private OutputStream remittanceStream;
    private OutputStream transactionStream;

    private Path messagePath;
    private Path remittancePath;
    private Path transactionPath;

    /**
     * Creates a new service that writes Arrow IPC Stream files to {@code outputDir}.
     *
     * @param outputDir         target directory (created if absent)
     * @param baseName          base name for output files (e.g. "pain001_type_d")
     * @param messageSchema     Arrow schema for the message table
     * @param remittanceSchema  Arrow schema for the remittance table
     * @param transactionSchema Arrow schema for the transaction table
     * @throws IOException if the output directory cannot be created
     */
    public LocalFilePersistenceService(Path outputDir, String baseName,
            Schema messageSchema, Schema remittanceSchema, Schema transactionSchema)
            throws IOException {
        this.outputDir = outputDir;
        this.baseName = baseName;
        this.messageSchema = messageSchema;
        this.remittanceSchema = remittanceSchema;
        this.transactionSchema = transactionSchema;
        Files.createDirectories(outputDir);
    }

    @Override
    public void writeBatch(TableType tableType, VectorSchemaRoot root) throws IOException {
        switch (tableType) {
            case MESSAGE -> {
                if (messageWriter == null) {
                    messagePath = outputDir.resolve(baseName + "_message.arrows");
                    messageStream = Files.newOutputStream(messagePath);
                    messageWriter = new ArrowStreamWriter(root, null,
                            Channels.newChannel(messageStream));
                    messageWriter.start();
                }
                messageWriter.writeBatch();
            }
            case REMITTANCE -> {
                if (remittanceWriter == null) {
                    remittancePath = outputDir.resolve(baseName + "_remittance.arrows");
                    remittanceStream = Files.newOutputStream(remittancePath);
                    remittanceWriter = new ArrowStreamWriter(root, null,
                            Channels.newChannel(remittanceStream));
                    remittanceWriter.start();
                }
                remittanceWriter.writeBatch();
            }
            case TRANSACTION -> {
                if (transactionWriter == null) {
                    transactionPath = outputDir.resolve(baseName + "_transaction.arrows");
                    transactionStream = Files.newOutputStream(transactionPath);
                    transactionWriter = new ArrowStreamWriter(root, null,
                            Channels.newChannel(transactionStream));
                    transactionWriter.start();
                }
                transactionWriter.writeBatch();
            }
        }
    }

    @Override
    public void finish() throws IOException {
        IOException firstException = null;
        if (messageWriter != null) {
            try {
                messageWriter.end();
                messageWriter.close();
                messageStream.close();
            } catch (IOException e) {
                firstException = e;
            }
        }
        if (remittanceWriter != null) {
            try {
                remittanceWriter.end();
                remittanceWriter.close();
                remittanceStream.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
            }
        }
        if (transactionWriter != null) {
            try {
                transactionWriter.end();
                transactionWriter.close();
                transactionStream.close();
            } catch (IOException e) {
                if (firstException == null) firstException = e;
            }
        }
        if (firstException != null) throw firstException;
    }

    @Override
    public long getBytesWritten() {
        long total = 0;
        try {
            if (messagePath != null && Files.exists(messagePath))
                total += Files.size(messagePath);
            if (remittancePath != null && Files.exists(remittancePath))
                total += Files.size(remittancePath);
            if (transactionPath != null && Files.exists(transactionPath))
                total += Files.size(transactionPath);
        } catch (IOException e) {
            // best-effort
        }
        return total;
    }

    @Override
    public void close() throws IOException {
        finish();
    }
}
