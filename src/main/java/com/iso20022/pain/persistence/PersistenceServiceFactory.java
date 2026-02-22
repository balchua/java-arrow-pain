package com.iso20022.pain.persistence;

import org.apache.arrow.vector.types.pojo.Schema;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Factory that reads environment variables and constructs the correct {@link PersistenceService}.
 *
 * <table>
 * <caption>Environment Variables</caption>
 * <tr><th>Env var</th><th>Default</th><th>Meaning</th></tr>
 * <tr><td>{@code PAIN_PERSISTENCE_MODE}</td><td>{@code local}</td><td>{@code local} or {@code s3}</td></tr>
 * <tr><td>{@code PAIN_LOCAL_OUTPUT_DIR}</td><td>{@code src/main/resources/output}</td><td>Used when mode=local</td></tr>
 * <tr><td>{@code PAIN_S3_BUCKET}</td><td>—</td><td>Required when mode=s3</td></tr>
 * <tr><td>{@code PAIN_S3_KEY_PREFIX}</td><td>{@code pain001}</td><td>S3 key prefix</td></tr>
 * </table>
 */
public final class PersistenceServiceFactory {

    private PersistenceServiceFactory() {}

    /**
     * Creates a {@link PersistenceService} based on environment variable configuration.
     *
     * @param baseName base name for output files
     * @param msg      Arrow schema for the message table
     * @param rmt      Arrow schema for the remittance table
     * @param tx       Arrow schema for the transaction table
     * @return configured persistence service
     * @throws IOException           if local output directory cannot be created
     * @throws IllegalStateException if s3 mode is selected but PAIN_S3_BUCKET is missing
     */
    public static PersistenceService create(String baseName, Schema msg, Schema rmt, Schema tx)
            throws IOException {
        String mode = System.getenv("PAIN_PERSISTENCE_MODE");
        if (mode == null || mode.isBlank()) {
            mode = "local";
        }

        if ("s3".equalsIgnoreCase(mode)) {
            String bucket = System.getenv("PAIN_S3_BUCKET");
            if (bucket == null || bucket.isBlank()) {
                throw new IllegalStateException(
                        "PAIN_PERSISTENCE_MODE=s3 but PAIN_S3_BUCKET is not set. "
                        + "Set the PAIN_S3_BUCKET environment variable to the target S3 bucket name.");
            }
            String keyPrefix = System.getenv("PAIN_S3_KEY_PREFIX");
            if (keyPrefix == null || keyPrefix.isBlank()) {
                keyPrefix = "pain001";
            }
            S3Client s3Client = S3Client.create();
            return new S3PersistenceService(s3Client, bucket, keyPrefix, baseName, msg, rmt, tx);
        }

        // local mode
        String outputDirEnv = System.getenv("PAIN_LOCAL_OUTPUT_DIR");
        java.nio.file.Path outputDir;
        if (outputDirEnv != null && !outputDirEnv.isBlank()) {
            outputDir = Paths.get(outputDirEnv);
        } else {
            outputDir = Paths.get("src", "main", "resources", "output");
        }
        return new LocalFilePersistenceService(outputDir, baseName, msg, rmt, tx);
    }
}
