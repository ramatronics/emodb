package com.bazaarvoice.emodb.blob;

public class S3BucketConfiguration {
    private String name;
    private String roleArn;
    private String roleExternalId;

    public String getName() {
        return name;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getRoleExternalId() {
        return roleExternalId;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setRoleArn(final String roleArn) {
        this.roleArn = roleArn;
    }

    public void setRoleExternalId(final String roleExternalId) {
        this.roleExternalId = roleExternalId;
    }

    public S3BucketConfiguration() {
    }

    public S3BucketConfiguration(final String name, final String roleArn, final String roleExternalId) {
        this.name = name;
        this.roleArn = roleArn;
        this.roleExternalId = roleExternalId;
    }
}
