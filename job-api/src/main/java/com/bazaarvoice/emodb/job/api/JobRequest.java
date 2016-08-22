package com.bazaarvoice.emodb.job.api;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class JobRequest<Q, R> {

    private final JobType<Q, R> _type;
    private final Q _request;

    public JobRequest(JobType<Q, R> type, @Nullable Q request) {
        _type = checkNotNull(type, "type");
        _request = request;
    }

    public JobType<Q, R> getType() {
        return _type;
    }

    public Q getRequest() {
        return _request;
    }
}
