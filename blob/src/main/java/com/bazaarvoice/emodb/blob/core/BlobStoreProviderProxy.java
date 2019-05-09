package com.bazaarvoice.emodb.blob.core;

import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.google.inject.Provider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Supports delegation of DDL operations to the system data center.  This implementation uses a Providers for
 * the blob store injection to prevent re-entrant injection issues.
 */
public class BlobStoreProviderProxy implements BlobStore {

    private final Supplier<BlobStore> _localCassandra;
    private final Supplier<BlobStore> _localS3;
    private final Supplier<BlobStore> _system;

    @Inject
    public BlobStoreProviderProxy(@LocalCassandraBlobStore Provider<BlobStore> localCassandra, @LocalS3BlobStore Provider<BlobStore> localS3, @SystemBlobStore Provider<BlobStore> system) {
        // The providers should be singletons.  Even so, locally memoize to ensure use of a singleton.
        _localCassandra = Suppliers.memoize(localCassandra::get);
        _localS3 = Suppliers.memoize(localS3::get);
        _system = Suppliers.memoize(system::get);
    }

    // Calls which modify the blob store DDL must be redirected to the system data center

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException {
        _system.get().createTable(table, options, attributes, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        _system.get().dropTable(table, audit);
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit)
            throws UnknownTableException {
        _system.get().setTableAttributes(table, attributes, audit);
    }

    // All other calls can be serviced locally

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _localCassandra.get().listTables(fromTableExclusive, limit);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        if (isInS3(table)) {
            _localS3.get().purgeTableUnsafe(table, audit);
        } else {
            _localCassandra.get().purgeTableUnsafe(table, audit);
        }
    }

    private boolean isInS3(String table) {
        return getTablePlacement(getTableMetadata(table)).startsWith("s3");
    }

    @Override
    public boolean getTableExists(String table) {
        return _localCassandra.get().getTableExists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _localCassandra.get().isTableAvailable(table);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _localCassandra.get().getTableMetadata(table);
    }

    @Override
    public Map<String, String> getTableAttributes(String table) throws UnknownTableException {
        return _localCassandra.get().getTableAttributes(table);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _localCassandra.get().getTableOptions(table);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        if (isInS3(table)) {
            return _localS3.get().getTableApproximateSize(table);
        } else {
            return _localCassandra.get().getTableApproximateSize(table);
        }
    }

    @Override
    public BlobMetadata getMetadata(String table, String blobId) throws BlobNotFoundException {
        if (isInS3(table)) {
            return _localS3.get().getMetadata(table, blobId);
        } else {
            return _localCassandra.get().getMetadata(table, blobId);
        }
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String table, @Nullable String fromBlobIdExclusive, long limit) {
        if (isInS3(table)) {
            return _localS3.get().scanMetadata(table, fromBlobIdExclusive, limit);
        } else {
            return _localCassandra.get().scanMetadata(table, fromBlobIdExclusive, limit);
        }
    }

    @Override
    public Blob get(String table, String blobId) throws BlobNotFoundException {
        if (isInS3(table)) {
            return _localS3.get().get(table, blobId);
        } else {
            return _localCassandra.get().get(table, blobId);
        }
    }

    @Override
    public Blob get(String table, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        if (isInS3(table)) {
            return _localS3.get().get(table, blobId, rangeSpec);
        } else {
            return _localCassandra.get().get(table, blobId, rangeSpec);
        }
    }

    @Override
    public void put(String table, String blobId, InputSupplier<? extends InputStream> in, Map<String, String> attributes)
            throws IOException {
        if (isInS3(table)) {
            _localS3.get().put(table, blobId, in, attributes);
        } else {
            _localCassandra.get().put(table, blobId, in, attributes);
        }
    }

    @Override
    public void delete(String table, String blobId) {
        if (isInS3(table)) {
            _localS3.get().delete(table, blobId);
        } else {
            _localCassandra.get().delete(table, blobId);
        }
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _system.get().getTablePlacements();
    }

    private static String getTablePlacement(Table table) {
        TableAvailability availability = table.getAvailability();
        if (availability != null) {
            return availability.getPlacement();
        }
        // If the table isn't available locally then defer to it's placement from the table options.
        // If the user doesn't have permission the permission check will fail.  If he does the permission
        // check won't fail but another more informative exception will likely be thrown.

        return table.getOptions().getPlacement();
    }
}
