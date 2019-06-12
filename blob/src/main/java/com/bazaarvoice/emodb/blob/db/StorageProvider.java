package com.bazaarvoice.emodb.blob.db;

import com.bazaarvoice.emodb.table.db.Table;

import java.nio.ByteBuffer;

/**
 * Chunked storage provider based on the Astyanax {@link com.netflix.astyanax.recipes.storage.ChunkedStorageProvider}
 * interface but with the addition of a {@code timestamp} field so that the metadata and all chunks may be written with
 * the same timestamp.
 * <p>
 * See https://github.com/Netflix/astyanax/wiki/Chunked-Object-Store
 */
public interface StorageProvider {

    long getCurrentTimestamp(Table table);

    void writeChunk(Table table, String blobId, int chunkId, ByteBuffer data, long timestamp);

    ByteBuffer readChunk(Table table, String blobId, int chunkId, long timestamp);

    /**
     * will be removed in scope of extracting blob from cassandra.
     * it's used now as metadata and blob are stored in the same row in table,
     * so delete operation is performed "transactionally"
     */
    @Deprecated
    void deleteObjectWithMetadata(Table table, String blobId, Integer chunkCount);

    void deleteObject(Table table, String blobId);

    long countObjects(Table table);

    void purge(Table table);

    int getDefaultChunkSize();
}
