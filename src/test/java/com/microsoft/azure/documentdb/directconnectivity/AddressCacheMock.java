package com.microsoft.azure.documentdb.directconnectivity;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.directconnectivity.AddressCache;
import com.microsoft.azure.documentdb.directconnectivity.AddressInformation;
import com.microsoft.azure.documentdb.internal.DocumentServiceRequest;

class AddressCacheMock extends AddressCache {
    public AddressCache orginalAddressCache;
    private DocumentClientException exceptionToThrow;
    private AddressInformation[] valueToReturn;
    private int numberOfInvocationWithAction = Integer.MAX_VALUE;
    private int currentNumberOfReturns = 0;
    private int numberOfReplicasToReturn = 0;
    private boolean includePrimary = true;

    public AddressCacheMock(AddressCache addressCache) {
        this.orginalAddressCache = addressCache;
    }

    public AddressCacheMock setExceptionToThrow(DocumentClientException e) {
        this.reset();
        this.exceptionToThrow = e;
        return this;
    }

    public AddressCacheMock setValueToReturn(AddressInformation[] response) {
        this.reset();
        this.valueToReturn = response;
        return this;
    }

    public void times(int numberOfInvocationWithAction) {
        this.numberOfInvocationWithAction = numberOfInvocationWithAction;
        this.currentNumberOfReturns = 0;
    }

    public void reset() {
        this.numberOfInvocationWithAction = Integer.MAX_VALUE;
        this.exceptionToThrow = null;
        this.valueToReturn = null;
        this.currentNumberOfReturns = 0;
        this.numberOfReplicasToReturn = 0;
        this.includePrimary = true;
    }

    public int getNumberOfReplicasToReturn() {
        return numberOfReplicasToReturn;
    }

    public AddressCacheMock setNumberOfReplicasToReturn(int numberOfReplicasToReturn) {
        this.numberOfReplicasToReturn = numberOfReplicasToReturn;
        return this;
    }

    public boolean isReturnPrimary() {
        return includePrimary;
    }

    public AddressCacheMock setReturnPrimary(boolean returnPrimary) {
        this.includePrimary = returnPrimary;
        return this;
    }


    @Override
    public AddressInformation[] resolve(DocumentServiceRequest request) throws DocumentClientException {
        if (currentNumberOfReturns >= numberOfInvocationWithAction) {
            this.reset();
        }

        if (exceptionToThrow != null) {
            currentNumberOfReturns++;
            throw exceptionToThrow;
        } else if (valueToReturn != null) {
            currentNumberOfReturns++;
            return valueToReturn;
        }

        AddressInformation[] addresses = this.orginalAddressCache.resolve(request);
        if (numberOfReplicasToReturn > 0 || !includePrimary) {
            if (!includePrimary && numberOfReplicasToReturn == 0) {
                numberOfReplicasToReturn = addresses.length - 1;
            }

            AddressInformation[] result = new AddressInformation[numberOfReplicasToReturn];
            int currentSize = 0;
            for (int i = 0; i < addresses.length; i++) {
                if (this.includePrimary || !addresses[i].isPrimary()) {
                    result[currentSize++] = addresses[i];
                }

                if (currentSize == numberOfReplicasToReturn) break;
            }

            currentNumberOfReturns++;
            return result;
        }

        return addresses;
    }
}