package com.microsoft.azure.documentdb.directconnectivity;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.RetryPolicy;

public class GoneAndRetryWithRetryPolicy implements RetryPolicy {
    private final static int WAIT_TIME_IN_SECONDS = 30;
    private final static int INITIALI_BACKOFF_SEONDS = 1;
    private final static int BACKOFF_MULTIPLIER = 2;
    private final Logger logger;
    private int attemptCount = 1;
    private DocumentClientException lastRetryWithException;
    private int currentBackOffSeconds = INITIALI_BACKOFF_SEONDS;
    private DocumentServiceRequest request;

    private long startTimeMilliSeconds;
    private long retryAfterSeconds;

    public GoneAndRetryWithRetryPolicy(DocumentServiceRequest request) {
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
        this.request = request;
        this.startTimeMilliSeconds = System.currentTimeMillis();
    }

    @Override
    public boolean shouldRetry(DocumentClientException exception) throws DocumentClientException {
        if (exception.getStatusCode() != HttpStatus.SC_GONE && (int) exception.getStatusCode() != HttpConstants.StatusCodes.RETRY_WITH) {
            return false;
        }

        if (exception.getStatusCode() == HttpConstants.StatusCodes.RETRY_WITH) {
            this.lastRetryWithException = exception;
        }

        this.retryAfterSeconds = 0;
        long remainingSeconds = WAIT_TIME_IN_SECONDS - (int) ((System.currentTimeMillis() - this.startTimeMilliSeconds) * 1.0 / 1000.0);
        if (this.attemptCount++ > 1) {
            if (remainingSeconds <= 0) {
                if (exception.getStatusCode() == HttpStatus.SC_GONE) {
                    // TODO: differentiate between Gone and InvalidPartition exceptions
                    if (this.lastRetryWithException != null) {
                        logger.log(
                                Level.WARNING,
                                "Received Gone or Invalid partition after backoff/retry including at least one RetryWithException. Will fail the request with RetryWithException",
                                this.lastRetryWithException);
                        throw this.lastRetryWithException;
                    } else {
                        logger.log(Level.WARNING, "Received gone exception or invalid partiton after backoff/retry. Will fail the request", exception);
                        throw new DocumentClientException((int) HttpStatus.SC_SERVICE_UNAVAILABLE, exception);
                    }
                } else {
                    logger.log(Level.WARNING, "Received  retryWith exception after backoff/retry. Will fail the request", exception);
                }

                return false;
            }

            this.retryAfterSeconds = Math.min(this.currentBackOffSeconds, remainingSeconds);
            this.currentBackOffSeconds *= BACKOFF_MULTIPLIER;
        }

        if (exception.getStatusCode() == HttpStatus.SC_GONE) {
            Integer substatusCode = exception.getSubStatusCode();
            
            if (substatusCode != null && substatusCode.intValue() == HttpConstants.SubStatusCodes.NAME_CACHE_IS_STALE) {
                request.setQuorumSelectedLSN(-1);
                request.setQuorumSelectedStoreResponse(null);
            }
            
            request.setForceAddressRefresh(true);
            request.setForceNameCacheRefresh(true);
        } else {
            logger.log(Level.WARNING, "Received retryWith exception, will retry", exception);
        }

        return true;
    }

    @Override
    public long getRetryAfterInMilliseconds() {
        return this.retryAfterSeconds * 1000;
    }

}
