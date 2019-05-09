package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.bazaarvoice.emodb.blob.api.Range;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3StorageProvider {

    private static final int MAX_SCAN_METADATA_BATCH_SIZE = 250;

    private final S3PlacementFactory _s3PlacementFactory;
    private final Meter _blobReadMeter;
    private final Meter _blobWriteMeter;
    private final Timer _scanBatchTimer;
    private final Meter _scanReadMeter;
    private final Meter _copyMeter;

    @Inject
    public S3StorageProvider(final S3PlacementFactory s3PlacementFactory,
                             final MetricRegistry metricRegistry) {
        _s3PlacementFactory = Objects.requireNonNull(s3PlacementFactory);
        Objects.requireNonNull(metricRegistry);
        _blobReadMeter = metricRegistry.meter(getMetricName("blob-read"));
        _blobWriteMeter = metricRegistry.meter(getMetricName("blob-write"));
        _scanBatchTimer = metricRegistry.timer(getMetricName("scanBatch"));
        _scanReadMeter = metricRegistry.meter(getMetricName("scan-reads"));
        _copyMeter = metricRegistry.meter(getMetricName("copy"));
    }

    private static String getMetricName(final String name) {
        return MetricRegistry.name("bv.emodb.blob", "s3", name);
    }

    private AmazonS3URI getAmazonS3URI(final String tableName, final String tablePlacement, @Nullable String blobId) {
        String s3Bucket = _s3PlacementFactory.getS3Bucket(tablePlacement);
        String s3Key = null == blobId ? tableName : tableName + "/" + blobId;
        return new AmazonS3URI(String.format("s3://%s/%s", s3Bucket, s3Key));
    }

    public S3Object getObject(final String tableName, final String tablePlacement, String blobId, @Nullable Range range) {
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, blobId);
        final GetObjectRequest rangeObjectRequest = new GetObjectRequest(amazonS3URI.getBucket(), amazonS3URI.getKey());

        if (null != range) {
            rangeObjectRequest
                    .withRange(range.getOffset(), range.getOffset() + range.getLength());
        }

        return _s3PlacementFactory.getS3Client(amazonS3URI.getBucket())
                .getObject(rangeObjectRequest);
    }

    public ObjectMetadata getObjectMetadata(final String tableName, final String tablePlacement, String blobId) {
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, blobId);
        ObjectMetadata metadata;
        try {
            metadata = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket())
                    .getObjectMetadata(amazonS3URI.getBucket(), amazonS3URI.getKey());
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                metadata = null;
            } else {
                throw new RuntimeException(e);
            }
        }
        return metadata;
    }

//TODO
    public void putObject(final String tableName, final String tablePlacement, final String blobId, final InputStream input, final ObjectMetadata objectMetadata) {
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, blobId);

        final AmazonS3 writeS3Client = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket());
//        final TransferManager transferManager = TransferManagerBuilder.standard()
//                .withS3Client(writeS3Client).build();
        PutObjectRequest putObjectRequest = new PutObjectRequest(amazonS3URI.getBucket(), amazonS3URI.getKey(), input, objectMetadata);
        writeS3Client.putObject(putObjectRequest);
//        int readLimit = 1024 * 1024;
//        putObjectRequest.getRequestClientOptions().setReadLimit(readLimit);
//        final Upload upload = transferManager.upload(putObjectRequest);
//        transferManager.upload(putObjectRequest);
//        try {
//            upload.waitForCompletion();
////            transferManager.shutdownNow(false);
//        } catch (final InterruptedException e) {
//            throw new RuntimeException(e);
//        }
    }

//TODO
/*    public void putObject1(final AmazonS3URI amazonS3URI, final InputStream input, final ObjectMetadata objectMetadata) {
        final AmazonS3 writeS3Client = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket());

        // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
        // then, after each individual part has been uploaded, pass the list of ETags to
        // the request to complete the upload.
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
        InitiateMultipartUploadResult initResponse = writeS3Client.initiateMultipartUpload(initRequest);

        // Upload the file parts.
        long filePosition = 0;
        for (int i = 1; filePosition < contentLength; i++) {
            // Because the last part could be less than 5 MB, adjust the part size as needed.
            partSize = Math.min(partSize, (contentLength - filePosition));

            // Create the request to upload a part.
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(keyName)
                    .withUploadId(initResponse.getUploadId())
                    .withPartNumber(i)
                    .withFileOffset(filePosition)
                    .withFile(file)
                    .withPartSize(partSize);

            // Upload the part and add the response's ETag to our list.
            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
            partETags.add(uploadResult.getPartETag());

            filePosition += partSize;
        }

        // Complete the multipart upload.
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(amazonS3URI.getBucket(), amazonS3URI.getKey(),
                initResponse.getUploadId(), partETags);
    }*/

    public Stream<S3ObjectSummary> listObjects(final String tableName, final String tablePlacement, @Nullable final String fromBlobIdExclusive, final long limit) {
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, null);

        AmazonS3 readS3Client = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket());

        Stream<S3ObjectSummary> summaryStream = Stream.<S3ObjectSummary>builder().build();

        final ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(amazonS3URI.getBucket())
                .withPrefix(amazonS3URI.getKey())
                .withStartAfter(fromBlobIdExclusive)
                .withMaxKeys(Math.toIntExact(Math.min(limit, MAX_SCAN_METADATA_BATCH_SIZE)));
        ListObjectsV2Result result;
        do {
            result = readS3Client.listObjectsV2(request);
            summaryStream = Stream.concat(summaryStream, result.getObjectSummaries().parallelStream());

            // If there are more than maxKeys keys in the bucket, get a continuation token
            // and list the next objects.
            final String token = result.getNextContinuationToken();
            request.setContinuationToken(token);
        } while (result.isTruncated());

        return summaryStream.parallel()
                .filter(s3ObjectSummary -> s3ObjectSummary.getKey().split("/").length == 2)
                .limit(limit);
    }

    public void copy(final String tableName, final String sourcePlacement, final String destPlacement) {
        AmazonS3URI dest = getAmazonS3URI(tableName, destPlacement, null);
        AmazonS3 writeS3Client = _s3PlacementFactory.getS3Client(dest.getBucket());
        final TransferManager transferManager = TransferManagerBuilder.standard()
                .withS3Client(writeS3Client)
//                TODO tune if needed
//                .withMinimumUploadPartSize()
                .build();
        listObjects(tableName, sourcePlacement, null, Long.MAX_VALUE)
                .map(s3ObjectSummary -> transferManager.copy(
                        new CopyObjectRequest(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey(), dest.getBucket(), dest.getKey() + "/" + s3ObjectSummary.getKey().split("/")[1]))
                )
                .forEach(copy -> {
                    try {
                        copy.waitForCopyResult();
                    } catch (InterruptedException e) {
                        new RuntimeException(e);
                    }
                });
        transferManager.shutdownNow(false);
    }

    public void purge(final String tableName, final String tablePlacement) {
        final AtomicInteger counter = new AtomicInteger();
        final int chunkSize = 100;
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, null);

        AmazonS3 writeS3Client = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket());
        listObjects(tableName, tablePlacement, null, Long.MAX_VALUE)
                .map(s3ObjectSummary -> new DeleteObjectsRequest.KeyVersion(s3ObjectSummary.getKey()))
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .forEach((integer, keyVersions) -> {
                    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(amazonS3URI.getBucket())
                            .withKeys(keyVersions);
                    DeleteObjectsResult deleteObjectsResult = writeS3Client.deleteObjects(deleteObjectsRequest);
                    deleteObjectsResult.getDeletedObjects().parallelStream()
                            .forEach(deletedObject -> writeS3Client.deleteVersion(amazonS3URI.getBucket(), deletedObject.getKey(), deletedObject.getVersionId()));
                });
    }

    public void deleteObject(final String tableName, final String tablePlacement, final String blobId) {
        AmazonS3URI amazonS3URI = getAmazonS3URI(tableName, tablePlacement, blobId);

        final AmazonS3 writeS3Client = _s3PlacementFactory.getS3Client(amazonS3URI.getBucket());
        DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(amazonS3URI.getBucket())
                .withKeys(amazonS3URI.getKey());

        DeleteObjectsResult deleteObjectsResult = writeS3Client.deleteObjects(deleteObjectsRequest);
        deleteObjectsResult.getDeletedObjects().parallelStream()
                .forEach(deletedObject ->
                        writeS3Client.deleteVersion(amazonS3URI.getBucket(), deletedObject.getKey(), deletedObject.getVersionId())
                );
    }
}
