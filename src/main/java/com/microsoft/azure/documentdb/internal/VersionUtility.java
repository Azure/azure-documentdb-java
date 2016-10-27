package com.microsoft.azure.documentdb.internal;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.microsoft.azure.documentdb.DocumentClientException;

public class VersionUtility {

    public static boolean isLaterThan(String compareVersion, String baseVersion) throws DocumentClientException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Date compareVersionDate;
        try {
            compareVersionDate = formatter.parse(compareVersion);
        } catch (ParseException e) {
            throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                    String.format("Invalid version format for compareVersionDate. Input Version %s", compareVersion));
        }

        Date baseVersionDate;
        try {
            baseVersionDate = formatter.parse(baseVersion);
        } catch (ParseException e) {
            throw new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                    String.format("Invalid version format for baseVersionDate. Input Version %s", baseVersion));
        }

        return compareVersionDate.compareTo(baseVersionDate) >= 0;
    }
}
