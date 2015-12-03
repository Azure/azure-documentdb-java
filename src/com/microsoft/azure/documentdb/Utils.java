package com.microsoft.azure.documentdb;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

class Utils {
    static String encodeBase64String(byte[] binaryData) {
        String encodedString = Base64.encodeBase64String(binaryData);

        if (encodedString.endsWith("\r\n")) {
            encodedString = encodedString.substring(0, encodedString.length() - 2);
        }
        return encodedString;
    }

    /**
     * Checks whether the specified link is Name based or not
     * 
     * @param link the link to analyze.
     * @return true or false
     */
    static boolean isNameBased(String link) {
        if(StringUtils.isEmpty(link)) {
            return false;
        }

        // trimming the leading "/"
        if (link.startsWith("/") && link.length() > 1) {
            link = link.substring(1);
        }

        // Splitting the link(separated by "/") into parts 
        String[] parts = link.split("/");

        // First part should be "dbs" 
        if(parts.length == 0 || StringUtils.isEmpty(parts[0]) || !parts[0].equalsIgnoreCase(Paths.DATABASES_PATH_SEGMENT)) {
            return false;
        }

        // The second part is the database id(ResourceID or Name) and cannot be empty
        if(parts.length < 2 || StringUtils.isEmpty(parts[1])) {
            return false;
        }

        // Either ResourceID or database name
        String databaseID = parts[1];

        // Length of databaseID(in case of ResourceID) is always 8
        if (databaseID.length() != 8) { 
            return true;
        }

        // Decoding the databaseID
        byte[] buffer = ResourceId.fromBase64String(databaseID);

        // Length of decoded buffer(in case of ResourceID) is always 4
        if(buffer.length != 4) {
            return true;
        }

        return false;
    }
    
    /**
     * Checks whether the specified link is a Database Self Link or a Database ID based link
     * 
     * @param link the link to analyze.
     * @return true or false
     */
    static boolean isDatabaseLink(String link) {
        if(StringUtils.isEmpty(link)) {
            return false;
        }

        // trimming the leading and trailing "/" from the input string
        link = trimBeginingAndEndingSlashes(link);
        
        // Splitting the link(separated by "/") into parts 
        String[] parts = link.split("/");
        
        if(parts.length != 2) {
            return false;
        }

        // First part should be "dbs" 
        if(StringUtils.isEmpty(parts[0]) || !parts[0].equalsIgnoreCase(Paths.DATABASES_PATH_SEGMENT)) {
            return false;
        }

        // The second part is the database id(ResourceID or Name) and cannot be empty
        if(StringUtils.isEmpty(parts[1])) {
            return false;
        }
            
        return true;
    }

    /**
     * Checks whether the specified path segment is a resource type
     * 
     * @param resourcePathSegment the path segment to analyze.
     * @return true or false
     */
    static boolean IsResourceType(String resourcePathSegment) {
        if (StringUtils.isEmpty(resourcePathSegment)) {
            return false;
        }

        switch (resourcePathSegment.toLowerCase()) {
        case Paths.ATTACHMENTS_PATH_SEGMENT:
        case Paths.COLLECTIONS_PATH_SEGMENT:
        case Paths.DATABASES_PATH_SEGMENT:
        case Paths.PERMISSIONS_PATH_SEGMENT:
        case Paths.USERS_PATH_SEGMENT:
        case Paths.DOCUMENTS_PATH_SEGMENT:
        case Paths.STORED_PROCEDURES_PATH_SEGMENT:
        case Paths.TRIGGERS_PATH_SEGMENT:
        case Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT:
        case Paths.CONFLICTS_PATH_SEGMENT:
            return true;

        default:
            return false;
        }
    }

    /**
     * Joins the specified paths by appropriately padding them with '/'
     * 
     * @param path1 the first path segment to join.
     * @param path2 the second path segment to join.
     * @return the concatenated path with '/'
     */
    static String joinPath(String path1, String path2) {
        path1 = trimBeginingAndEndingSlashes(path1);
        String result = "/" + path1 + "/";

        if(!StringUtils.isEmpty(path2)) {
            path2 = trimBeginingAndEndingSlashes(path2);
            result += path2 + "/";
        }

        return result;
    }

    /**
     * Trims the beginning and ending '/' from the given path
     * 
     * @param path the path to trim for beginning and ending slashes
     * @return the path without beginning and ending '/'
     */
    static String trimBeginingAndEndingSlashes(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }
}
