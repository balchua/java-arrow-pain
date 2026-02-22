package com.iso20022.pain.persistence;

import com.iso20022.pain.parser.BatchConsumer.TableType;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Streams Arrow IPC batches to S3 via multipart upload using the AWS SDK v2 S3 client.
 *
 * <p>One S3 multipart upload per table type, keyed as
 * {@code {keyPrefix}/{baseName}_message.arrows} etc.</p>
 *
 * <p>Call {@link #finish()} after the last batch to complete all uploads.
 * If an exception occurs, all open uploads are aborted.</p>
 */
public final class S3PersistenceService implements PersistenceService {

    private static final int PART_SIZE = 5 * 1024 * 1024; // 5 MB minimum for S3 multipart

    private final S3Client s3Client;
    private final boolean ownS3Client;
    private final String bucket;
    private final String keyPrefix;
    private final String baseName;
    private final Schema messageSchema;
    private final Schema remittanceSchema;
    private final Schema transactionSchema;

    private TableUpload messageUpload;
    private TableUpload remittanceUpload;
    private TableUpload transactionUpload;

    /**
     * Creates a new service that streams Arrow IPC batches to S3 via multipart upload.
     * The provided {@code s3Client} is NOT closed when this service is closed — the
     * caller retains ownership.
     *
     * @param s3Client          the AWS S3 client (caller-owned; not closed on finish)
     * @param bucket            target S3 bucket name
     * @param keyPrefix         S3 key prefix (folder)
     * @param baseName          base name for output files
     * @param messageSchema     Arrow schema for the message table
     * @param remittanceSchema  Arrow schema for the remittance table
     * @param transactionSchema Arrow schema for the transaction table
     */
    public S3PersistenceService(S3Client s3Client, String bucket, String keyPrefix,
            String baseName, Schema messageSchema, Schema remittanceSchema,
            Schema transactionSchema) {
        this.s3Client = s3Client;
        this.ownS3Client = false;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
        this.baseName = baseName;
        this.messageSchema = messageSchema;
        this.remittanceSchema = remittanceSchema;
        this.transactionSchema = transactionSchema;
    }

    /**
     * Creates a new service, building its own default {@link S3Client} which is
     * closed when {@link #finish()} or {@link #close()} is called.
     *
     * @param bucket            target S3 bucket name
     * @param keyPrefix         S3 key prefix (folder)
     * @param baseName          base name for output files
     * @param messageSchema     Arrow schema for the message table
     * @param remittanceSchema  Arrow schema for the remittance table
     * @param transactionSchema Arrow schema for the transaction table
     */
    public S3PersistenceService(String bucket, String keyPrefix, String baseName,
            Schema messageSchema, Schema remittanceSchema, Schema transactionSchema) {
        this.s3Client = S3Client.create();
        this.ownS3Client = true;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
        this.baseName = baseName;
        this.messageSchema = messageSchema;
        this.remittanceSchema = remittanceSchema;
        this.transactionSchema = transactionSchema;
    }

    @Override
    public void writeBatch(TableType tableType, VectorSchemaRoot root) throws IOException {
        switch (tableType) {
            case MESSAGE -> {
                if (messageUpload == null) {
                    messageUpload = new TableUpload(keyPrefix + "/" + baseName + "_message.arrows",
                            messageSchema, root);
                }
                messageUpload.writeBatch();
            }
            case REMITTANCE -> {
                if (remittanceUpload == null) {
                    remittanceUpload = new TableUpload(
                            keyPrefix + "/" + baseName + "_remittance.arrows",
                            remittanceSchema, root);
                }
                remittanceUpload.writeBatch();
            }
            case TRANSACTION -> {
                if (transactionUpload == null) {
                    transactionUpload = new TableUpload(
                            keyPrefix + "/" + baseName + "_transaction.arrows",
                            transactionSchema, root);
                }
                transactionUpload.writeBatch();
            }
        }
    }

    @Override
    public void finish() throws IOException {
        IOException firstException = null;
        for (TableUpload upload : new TableUpload[]{messageUpload, remittanceUpload, transactionUpload}) {
            if (upload != null) {
                try {
                    upload.complete();
                } catch (IOException e) {
                    if (firstException == null) firstException = e;
                }
            }
        }
        if (firstException != null) throw firstException;
    }

    @Override
    public long getBytesWritten() {
        long total = 0;
        if (messageUpload != null) total += messageUpload.bytesWritten;
        if (remittanceUpload != null) total += remittanceUpload.bytesWritten;
        if (transactionUpload != null) total += transactionUpload.bytesWritten;
        return total;
    }

    @Override
    public void close() throws IOException {
        finish();
        if (ownS3Client) {
            s3Client.close();
        }
    }

    // ── Internal: one multipart upload per table ──────────────────────────────

    private final class TableUpload {
        private final String key;
        private final String uploadId;
        private final List<CompletedPart> completedParts = new ArrayList<>();
        private final S3OutputStream s3Out;
        private final ArrowStreamWriter writer;
        private int partNumber = 1;
        private long bytesWritten = 0;
        private boolean completed = false;

        TableUpload(String key, Schema schema, VectorSchemaRoot root) throws IOException {
            this.key = key;
            CreateMultipartUploadResponse response = s3Client.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucket).key(key).build());
            this.uploadId = response.uploadId();
            this.s3Out = new S3OutputStream();
            this.writer = new ArrowStreamWriter(root, null, Channels.newChannel(s3Out));
            try {
                this.writer.start();
            } catch (IOException e) {
                abort();
                throw e;
            }
        }

        void writeBatch() throws IOException {
            writer.writeBatch();
            if (s3Out.buffer.size() >= PART_SIZE) {
                flush();
            }
        }

        void complete() throws IOException {
            if (completed) return;
            completed = true;
            try {
                writer.end();
                writer.close();
                if (s3Out.buffer.size() > 0) {
                    flush();
                }
                s3Client.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                                .bucket(bucket).key(key).uploadId(uploadId)
                                .multipartUpload(CompletedMultipartUpload.builder()
                                        .parts(completedParts).build())
                                .build());
            } catch (IOException e) {
                abort();
                throw e;
            }
        }

        private void flush() {
            byte[] data = s3Out.buffer.toByteArray();
            s3Out.buffer.reset();
            bytesWritten += data.length;
            UploadPartResponse partResponse = s3Client.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(bucket).key(key).uploadId(uploadId)
                            .partNumber(partNumber).build(),
                    RequestBody.fromBytes(data));
            completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber).eTag(partResponse.eTag()).build());
            partNumber++;
        }

        private void abort() {
            try {
                s3Client.abortMultipartUpload(
                        AbortMultipartUploadRequest.builder()
                                .bucket(bucket).key(key).uploadId(uploadId).build());
            } catch (Exception ignored) {
                // best-effort abort
            }
        }
    }

    // ── Internal: buffered OutputStream that accumulates data for S3 parts ────

    private static final class S3OutputStream extends OutputStream {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(PART_SIZE);

        @Override
        public void write(int b) {
            buffer.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            buffer.write(b, off, len);
        }
    }
}
