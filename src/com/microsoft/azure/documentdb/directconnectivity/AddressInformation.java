package com.microsoft.azure.documentdb.directconnectivity;

public class AddressInformation {
    private boolean isPublic;
    private boolean isPrimary;
    private String physicalUri;

    public AddressInformation() {

    }

    public AddressInformation(boolean isPublic, boolean isPrimary, String physicalUri) {
        super();
        this.isPublic = isPublic;
        this.isPrimary = isPrimary;
        this.physicalUri = physicalUri;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public void setPublic(boolean isPublic) {
        this.isPublic = isPublic;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public String getPhysicalUri() {
        return physicalUri;
    }

    public void setPhysicalUri(String physicalUri) {
        this.physicalUri = physicalUri;
    }
}
