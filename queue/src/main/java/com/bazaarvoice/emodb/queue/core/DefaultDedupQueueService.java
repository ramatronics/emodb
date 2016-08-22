package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.google.inject.Inject;

public class DefaultDedupQueueService extends AbstractQueueService implements DedupQueueService {
    @Inject
    public DefaultDedupQueueService(DedupEventStore eventStore, JobService jobService, JobHandlerRegistry jobHandlerRegistry) {
        super(eventStore, jobService, jobHandlerRegistry, MoveDedupQueueJob.INSTANCE);
    }
}
