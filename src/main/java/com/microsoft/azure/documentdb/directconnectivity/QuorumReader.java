package com.microsoft.azure.documentdb.directconnectivity;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.AuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

//=================================================================================================================
// Strong read logic:
//=================================================================================================================
//
//              ------------------- PerformPrimaryRead-------------------------------------------------------------
//              |                       ^                                                                         |
//        [RetryOnSecondary]            |                                                                         |
//              |                   [QuorumNotSelected]                                                           |
//             \/                      |                                                                         \/
// Start-------------------------->SecondaryQuorumRead-------------[QuorumMet]-------------------------------->Result
//                                      |                                                                         ^
//                                  [QuorumSelected]                                                              |
//                                      |                                                                         |
//                                      \/                                                                        |
//                                  PrimaryReadBarrier-------------------------------------------------------------
//
//=================================================================================================================
// BoundedStaleness quorum read logic:
//=================================================================================================================
//
//              ------------------- PerformPrimaryRead-------------------------------------------------------------
//              |                       ^                                                                         |
//        [RetryOnSecondary]            |                                                                         |
//              |                   [QuorumNotSelected]                                                           |
//             \/                      |                                                                         \/
// Start-------------------------->SecondaryQuorumRead-------------[QuorumMet]-------------------------------->Result
//                                      |                                                                         ^
//                                  [QuorumSelected]                                                              |
//                                      |                                                                         |
//                                      |                                                                         |
//                                      ---------------------------------------------------------------------------
class QuorumReader {
    private final static int MAX_NUMBER_OF_READ_BARRIER_RETRIES = 6;
    private final static int MAX_NUMBER_OF_READ_QUORUM_RETRIES = 6;
    private final static int DELAY_BETWEEN_READ_BARRIER_CALLS_IN_MS = 10;

    private StoreReader storeReader;
    private AuthorizationTokenProvider authorizationTokenProvider;
    private Logger logger;

    public QuorumReader(StoreReader storeReader, AuthorizationTokenProvider authorizationTokenProvider) {
        this.storeReader = storeReader;
        this.authorizationTokenProvider = authorizationTokenProvider;
        this.logger = Logger.getLogger(this.getClass().getPackage().getName());
    }

    public StoreResponse readStrong(DocumentServiceRequest request, int quorumValue) throws DocumentClientException {
        int readQuorumRetry = QuorumReader.MAX_NUMBER_OF_READ_QUORUM_RETRIES;
        boolean shouldRetryOnSecondary = false;
        boolean hasPerformedReadFromPrimary = false;
        do {
            shouldRetryOnSecondary = false;
            // First read from secondaries only.
            ReadQuorumResult secondaryQuorumReadResult = this.readQuorum(request, quorumValue, false /* includePrimary */);
            switch (secondaryQuorumReadResult.getQuorumResult()) {
            case QuorumMet:
                return secondaryQuorumReadResult.getResponse();
            case QuorumSelected:
                // QuorumSelected so a Barrier request that includes primary will be used to wait for quorum.
                DocumentServiceRequest barrierRequest = ReadBarrierRequestHelper.create(request, this.authorizationTokenProvider);
                if (this.waitForBarrierRequest(barrierRequest, true /* include primary */, quorumValue, secondaryQuorumReadResult.getSelectedLsn())) {
                    return secondaryQuorumReadResult.getResponse();
                }

                // Barrier request with primary didn't succeed, so will just exit and retry if more retries left.
                this.logger.log(Level.INFO,
                        String.format(
                                "Couldn't converge on the LSN %1d after primary read barrier with read quorum %2d for strong read.",
                                secondaryQuorumReadResult.getSelectedLsn(), quorumValue));
                request.setQuorumSelectedLSN(secondaryQuorumReadResult.getSelectedLsn());
                request.setQuorumSelectedStoreResponse(secondaryQuorumReadResult.getStoreReadResult());

                break;
            case QuorumNotSelected:
                if (hasPerformedReadFromPrimary) {
                    this.logger.log(Level.WARNING, "Primary read already attempted. Quorum couldn't be selected after retrying on secondaries.");
                    throw new DocumentClientException(HttpStatus.SC_GONE, "Primary read already attempted. Quorum couldn't be selected after retrying on secondaries.");
                }

                this.logger.log(Level.INFO, String.format("Quorum could not be selected with read quorum of %1d", quorumValue));
                ReadPrimaryResult response = this.readPrimary(request, quorumValue);

                if (response.isSuccessful()) {
                    this.logger.log(Level.INFO, "Primary read succeeded");
                    return response.getResponse();
                } else if (response.isShouldRetryOnSecondary()) {
                    this.logger.log(Level.WARNING, "ReadPrimary did not succeed. Will retry on secondary.");
                    shouldRetryOnSecondary = true;
                    hasPerformedReadFromPrimary = true;
                } else {
                    this.logger.log(Level.WARNING, "Could not get successful response from ReadPrimary");
                    throw new DocumentClientException(HttpStatus.SC_GONE, "Could not get successful response from ReadPrimary");
                }

                break;
            default:
                this.logger.log(Level.SEVERE, String.format("Unknown read quorum result %1s", secondaryQuorumReadResult.getQuorumResult().toString()));
                throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Unknown read quorum result.");
            }
        } while (--readQuorumRetry > 0 && shouldRetryOnSecondary);

        this.logger.log(Level.WARNING, String.format("Could not complete read quorum with read quorum value of %1d", quorumValue));
        throw new DocumentClientException(HttpStatus.SC_GONE, "Could not complete read quorum.");
    }

    private ReadPrimaryResult readPrimary(DocumentServiceRequest request, int readQuorum) throws DocumentClientException {
        request.setForceAddressRefresh(false);
        StoreReadResult storeReadResult = this.storeReader.readPrimary(request, true);
        if (!storeReadResult.isValid()) {
            throw storeReadResult.getExcetpion();
        }

        if (storeReadResult.getCurrentReplicaSetSize() <= 0 || storeReadResult.getLSN() < 0 || storeReadResult.getQuorumAckedLSN() < 0) {
            this.logger.log(Level.WARNING,
                    String.format(
                            "Invalid value received from response header. CurrentReplicaSetSize %1d, StoreLSN %2d, QuorumAckedLSN %2d. Throwing gone exception",
                            storeReadResult.getCurrentReplicaSetSize(), storeReadResult.getLSN(),
                            storeReadResult.getQuorumAckedLSN()));
            throw new DocumentClientException(HttpStatus.SC_GONE, "Invalid value received from response header.");
        }

        // If we are doing read primary but the replica set size is bigger than the read quorum, then we wait for secondaries
        if (storeReadResult.getCurrentReplicaSetSize() > readQuorum) {
            String.format("Unexpected response. Replica Set size is %1d which is greater than min value %2d",
                    storeReadResult.getCurrentReplicaSetSize(), readQuorum);
            return new ReadPrimaryResult(false /*isSuccessful*/, true /*retry on secondaries */, null, request.getRequestChargeTracker());
        }

        return new ReadPrimaryResult(true /*isSuccessful*/, false /*retry on secondaries */, storeReadResult, request.getRequestChargeTracker());
    }

    private ReadQuorumResult readQuorum(DocumentServiceRequest request, int readQuorum, boolean includePrimary)
            throws DocumentClientException {

        long maxLsn = 0;
        StoreReadResult highestLsnResult = null;

        if (request.getQuorumSelectedStoreResponse() == null) {
            List<StoreReadResult> responseResult = this.storeReader.readMultipleReplicaImpl(request, includePrimary,
                    readQuorum);
            int responseCount = responseResult.size();
            if (responseCount < readQuorum) {
                return new ReadQuorumResult(ReadQuorumResultKind.QuorumNotSelected, -1, null, request.getRequestChargeTracker());
            }

            // checks if quorum is met and also updates maxLsn and the corresponding response as highestLsnResult 
            int replicaCountMaxLsn = 0;
            for (StoreReadResult storeReadResult : responseResult) {
                if (storeReadResult.getLSN() == maxLsn) {
                    replicaCountMaxLsn++;
                } else if (storeReadResult.getLSN() > maxLsn) {
                    replicaCountMaxLsn = 1;
                    maxLsn = storeReadResult.getLSN();
                    highestLsnResult = storeReadResult;
                }
            }

            boolean quorumMet = replicaCountMaxLsn >= readQuorum;
            if (quorumMet) {
                return new ReadQuorumResult(ReadQuorumResultKind.QuorumMet, maxLsn, highestLsnResult, request.getRequestChargeTracker());
            }
        } else {
            logger.log(Level.WARNING, "wait to catch up max lsn");

            maxLsn = request.getQuorumSelectedLSN();
            highestLsnResult = request.getQuorumSelectedStoreResponse();
        }
        // If the replicas are not on the same LSN, we ping the replicas with a
        // head request ( Barrier request ) MAX_NUMBER_OF_READ_BARRIER_RETRIES
        // times to see
        // if the replicas can reach quorum and have the same LSN
        DocumentServiceRequest barrierRequest = ReadBarrierRequestHelper.create(request, this.authorizationTokenProvider);
        if (this.waitForBarrierRequest(barrierRequest, false, readQuorum, maxLsn)) {
            return new ReadQuorumResult(ReadQuorumResultKind.QuorumMet, maxLsn, highestLsnResult, request.getRequestChargeTracker());
        }

        this.logger.log(Level.WARNING, String.format("Quorum selected with maxLsn %1d", maxLsn));
        return new ReadQuorumResult(ReadQuorumResultKind.QuorumSelected, maxLsn, highestLsnResult, request.getRequestChargeTracker());
    }

    private boolean waitForBarrierRequest(DocumentServiceRequest barrierRequest, 
            boolean allowPrimary, 
            int readQuorum,
            long readBarrierLsn) throws DocumentClientException {

        int readBarrierRetryCount = QuorumReader.MAX_NUMBER_OF_READ_BARRIER_RETRIES;
        do {
            barrierRequest.setForceAddressRefresh(false);
            List<StoreReadResult> responses = this.storeReader.readMultipleReplicaImpl(barrierRequest, allowPrimary, readQuorum);
            int validLsnCount = 0;
            for (StoreReadResult storeReadResult : responses) {
                if (storeReadResult.getLSN() >= readBarrierLsn) {
                    validLsnCount++;
                }
            }

            if (validLsnCount >= readQuorum) {
                this.logger.log(Level.INFO, "secondaries barrier requeest succeeded");
                return true;
            }

            this.logger.log(Level.WARNING,
                    String.format(
                            "Barrier request failed with validLsnCount = %1d and readQuorum %2d with remaining retries %3d and allow primary is %4b",
                            validLsnCount, readQuorum, readBarrierRetryCount, allowPrimary));

            try {
                Thread.sleep(QuorumReader.DELAY_BETWEEN_READ_BARRIER_CALLS_IN_MS);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Delay thread interruped with exception: ", e);
            }
        } while (--readBarrierRetryCount > 0);

        return false;
    }

    public StoreResponse readBoundedStaleness(DocumentServiceRequest request, int readQuorumValue) throws DocumentClientException {

        int readQuorumRetry = QuorumReader.MAX_NUMBER_OF_READ_QUORUM_RETRIES;
        boolean shouldRetryOnSecondary = false;
        boolean hasPerformedReadFromPrimary = false;

        do
        {
            logger.log(Level.WARNING, "remaining retries " + readQuorumRetry);
            ReadQuorumResult secondaryQuorumReadResult = readQuorum(request, readQuorumValue, false);
            shouldRetryOnSecondary = false;

            switch (secondaryQuorumReadResult.getQuorumResult())
            {
            case QuorumMet:
                this.logger.log(Level.INFO,"ReadQuorum successful");
                return secondaryQuorumReadResult.getResponse();

                // We do not perform the read barrier on Primary for BoundedStaleness as it has a 
                // potential to be always caught up in case of async replication
            case QuorumSelected:
                this.logger.log(Level.WARNING, String.format("Could not converge on the LSN %d after"
                        + " read barrier with read quorum %d."
                        + " Will not perform barrier call on Primary for BoundedStaleness", 
                        secondaryQuorumReadResult.getSelectedLsn(), readQuorumValue));

                request.setQuorumSelectedStoreResponse(secondaryQuorumReadResult.getStoreReadResult());
                request.setQuorumSelectedLSN(secondaryQuorumReadResult.getSelectedLsn());
                break;

            case QuorumNotSelected:
                if (hasPerformedReadFromPrimary)
                {
                    this.logger.log(Level.WARNING, "Primary read already attempted."
                            + " Quorum could not be selected after retrying on secondaries.");
                    throw new DocumentClientException(HttpStatus.SC_GONE, "Primary read already attempted."
                            + " Quorum could not be selected after retrying on secondaries.");
                }

                this.logger.log(Level.INFO, String.format(
                        "Quorum could not be selected with read quorum of %d", readQuorumValue));
                ReadPrimaryResult response = readPrimary(request, readQuorumValue);

                if (response.isSuccessful() && response.isShouldRetryOnSecondary())
                {
                    this.logger.log(Level.SEVERE, "PrimaryResult has both Successful and ShouldRetryOnSecondary flags set");
                    assert false : "PrimaryResult has both Successful and ShouldRetryOnSecondary flags set";
                }
                else if (response.isSuccessful())
                {
                    this.logger.log(Level.INFO, "ReadPrimary successful");

                    return response.getResponse();
                }
                else if (response.isShouldRetryOnSecondary())
                {
                    shouldRetryOnSecondary = true;
                    this.logger.log(Level.INFO, "ReadPrimary did not succeed. Will retry on secondary.");
                    hasPerformedReadFromPrimary = true;
                }
                else
                {
                    this.logger.log(Level.WARNING, "Could not get successful response from ReadPrimary");
                    throw new DocumentClientException(HttpStatus.SC_GONE, "Could not get successful response from ReadPrimary");
                }
                break;

            default:

                this.logger.log(Level.WARNING, 
                        String.format("Unknown ReadQuorum result %s", secondaryQuorumReadResult.getQuorumResult().toString()));
                throw new DocumentClientException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Unknown ReadQuorum");
            }
        } while (--readQuorumRetry > 0 && shouldRetryOnSecondary);

        this.logger.log(Level.SEVERE, String.format("Could not complete read quourm with read quorum value of %d", readQuorumValue));
        throw new DocumentClientException(HttpStatus.SC_GONE, String.format("Could not complete read quourm with read quorum value of %d", readQuorumValue));
    }
}
