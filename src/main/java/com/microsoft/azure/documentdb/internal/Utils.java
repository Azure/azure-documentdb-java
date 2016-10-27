package com.microsoft.azure.documentdb.internal;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentCollection;

public class Utils {
    private static final ObjectMapper simpleObjectMapper = new ObjectMapper();

    public static String encodeBase64String(byte[] binaryData) {
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
    public static boolean isNameBased(String link) {
        if (StringUtils.isEmpty(link)) {
            return false;
        }

        // trimming the leading "/"
        if (link.startsWith("/") && link.length() > 1) {
            link = link.substring(1);
        }

        // Splitting the link(separated by "/") into parts
        String[] parts = link.split("/");

        // First part should be "dbs"
        if (parts.length == 0 || StringUtils.isEmpty(parts[0])
                || !parts[0].equalsIgnoreCase(Paths.DATABASES_PATH_SEGMENT)) {
            return false;
        }

        // The second part is the database id(ResourceID or Name) and cannot be
        // empty
        if (parts.length < 2 || StringUtils.isEmpty(parts[1])) {
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
        if (buffer.length != 4) {
            return true;
        }

        return false;
    }

    /**
     * Checks whether the specified link is a Database Self Link or a Database
     * ID based link
     *
     * @param link the link to analyze.
     * @return true or false
     */
    public static boolean isDatabaseLink(String link) {
        if (StringUtils.isEmpty(link)) {
            return false;
        }

        // trimming the leading and trailing "/" from the input string
        link = trimBeginingAndEndingSlashes(link);

        // Splitting the link(separated by "/") into parts
        String[] parts = link.split("/");

        if (parts.length != 2) {
            return false;
        }

        // First part should be "dbs"
        if (StringUtils.isEmpty(parts[0]) || !parts[0].equalsIgnoreCase(Paths.DATABASES_PATH_SEGMENT)) {
            return false;
        }

        // The second part is the database id(ResourceID or Name) and cannot be
        // empty
        if (StringUtils.isEmpty(parts[1])) {
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
    public static boolean IsResourceType(String resourcePathSegment) {
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
            case Paths.PARTITION_KEY_RANGE_PATH_SEGMENT:
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
    public static String joinPath(String path1, String path2) {
        path1 = trimBeginingAndEndingSlashes(path1);
        String result = "/" + path1 + "/";

        if (!StringUtils.isEmpty(path2)) {
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
    public static String trimBeginingAndEndingSlashes(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        return path;
    }

    public static URL setQuery(String urlString, String query) {

        if (urlString == null)
            throw new IllegalStateException("urlString parameter can't be null.");
        query = Utils.removeLeadingQuestionMark(query);
        try {
            if (query != null && !query.isEmpty()) {
                return new URI(Utils.addTrailingSlash(urlString) + RuntimeConstants.Separators.Query[0] + query)
                        .toURL();
            } else {
                return new URI(Utils.addTrailingSlash(urlString)).toURL();
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Uri is invalid: ", e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Uri is invalid: ", e);
        }
    }

    public static String createQuery(List<NameValuePair> queryParameters) {
        if (queryParameters == null)
            return "";

        StringBuilder queryString = new StringBuilder();

        for (NameValuePair nameValuePair : queryParameters) {
            String key = nameValuePair.getName();
            String value = nameValuePair.getValue();

            if (key != null && !key.isEmpty()) {
                if (queryString.length() > 0) {
                    queryString.append(RuntimeConstants.Separators.Query[1]);
                }

                queryString.append(key);

                if (value != null) {
                    queryString.append(RuntimeConstants.Separators.Query[2]);
                    queryString.append(value);
                }
            }
        }

        return queryString.toString();
    }

    /**
     * Given the full path to a resource, extract the collection path.
     *
     * @param resourceFullName the full path to the resource.
     * @return the path of the collection in which the resource is.
     */
    public static String getCollectionName(String resourceFullName) {
        if (resourceFullName != null) {
            resourceFullName = Utils.trimBeginingAndEndingSlashes(resourceFullName);

            int slashCount = 0;
            for (int i = 0; i < resourceFullName.length(); i++) {
                if (resourceFullName.charAt(i) == '/') {
                    slashCount++;
                    if (slashCount == 4) {
                        return resourceFullName.substring(0, i);
                    }
                }
            }
        }
        return resourceFullName;
    }

    public static Boolean isCollectionPartitioned(DocumentCollection collection) {
        if (collection == null) {
            throw new IllegalArgumentException("collection");
        }

        return collection.getPartitionKey() != null
                && collection.getPartitionKey().getPaths() != null
                && collection.getPartitionKey().getPaths().size() > 0;
    }

    public static boolean isCollectionChild(ResourceType type) {
        return type == ResourceType.Document || type == ResourceType.Attachment || type == ResourceType.Conflict
                || type == ResourceType.StoredProcedure || type == ResourceType.Trigger || type == ResourceType.UserDefinedFunction;
    }

    public static boolean isWriteOperation(OperationType operationType) {
        return operationType == OperationType.Create || operationType == OperationType.Upsert || operationType == OperationType.Delete || operationType == OperationType.Replace
                || operationType == OperationType.ExecuteJavaScript;
    }

    public static boolean isFeedRequest(OperationType requestOperationType) {
        return requestOperationType == OperationType.Create ||
                requestOperationType == OperationType.Upsert ||
                requestOperationType == OperationType.ReadFeed ||
                requestOperationType == OperationType.Query ||
                requestOperationType == OperationType.SqlQuery;
    }

    private static String addTrailingSlash(String path) {
        if (path == null || path.isEmpty())
            path = new String(RuntimeConstants.Separators.Url);
        else if (path.charAt(path.length() - 1) != RuntimeConstants.Separators.Url[0])
            path = path + RuntimeConstants.Separators.Url[0];

        return path;
    }

    private static String removeLeadingQuestionMark(String path) {
        if (path == null || path.isEmpty())
            return path;

        if (path.charAt(0) == RuntimeConstants.Separators.Query[0])
            return path.substring(1);

        return path;
    }

    public static boolean isValidConsistency(ConsistencyLevel backendConsistency,
                                             ConsistencyLevel desiredConsistency) {
        switch (backendConsistency) {
            case Strong:
                return desiredConsistency == ConsistencyLevel.Strong ||
                        desiredConsistency == ConsistencyLevel.BoundedStaleness ||
                        desiredConsistency == ConsistencyLevel.Session ||
                        desiredConsistency == ConsistencyLevel.Eventual;

            case BoundedStaleness:
                return desiredConsistency == ConsistencyLevel.BoundedStaleness ||
                        desiredConsistency == ConsistencyLevel.Session ||
                        desiredConsistency == ConsistencyLevel.Eventual;

            case Session:
            case Eventual:
                return desiredConsistency == ConsistencyLevel.Session ||
                        desiredConsistency == ConsistencyLevel.Eventual;

            default:
                throw new IllegalArgumentException("backendConsistency");
        }
    }

    public static String getUserAgent() {
        String osName = System.getProperty("os.name");
        if (osName == null) {
            osName = "Unknown";
        }
        osName = osName.replaceAll("\\s", "");
        String userAgent = String.format("%s/%s JRE/%s %s/%s",
                osName,
                System.getProperty("os.version"),
                System.getProperty("java.version"),
                HttpConstants.Versions.USER_AGENT_VERSION_STRING,
                HttpConstants.Versions.CURRENT_USER_AGENT_VERSION);
        return userAgent;
    }

    public static ObjectMapper getSimpleObjectMapper() {
        return Utils.simpleObjectMapper;
    }
}
