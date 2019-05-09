package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Objects;

/**
 * Returns Blob Store {@link com.amazonaws.services.s3.AmazonS3Client} object for a S3 bucket based Emo placement string.
 */
public class S3PlacementFactory {

    private final Map<String, String> _placementsToBucketNames;
    private final Map<String, AmazonS3> _bucketNamesToS3Clients;

    @Inject
    public S3PlacementFactory(@PlacementsToBuckets final Map<String, String> placementsToBuckets,
                              @BucketNamesToS3Clients final Map<String, AmazonS3> bucketNamesToS3Clients) {
        _placementsToBucketNames = Objects.requireNonNull(placementsToBuckets);
        _bucketNamesToS3Clients = Objects.requireNonNull(bucketNamesToS3Clients);
    }

    AmazonS3 getS3Client(final String bucket) {
        return _bucketNamesToS3Clients.get(bucket);
    }

    public String getS3Bucket(final String placement) {
        return _placementsToBucketNames.get(placement);
    }

    //TODO for reads only
    public String getFailoverS3Bucket(final String placement) {
        return _placementsToBucketNames.get(placement);
    }
}
