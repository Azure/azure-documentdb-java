package com.microsoft.azure.documentdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

class TestUtils {
    private static final String DATABASES_PATH_SEGMENT = "dbs";
    private static final String USERS_PATH_SEGMENT = "users";
    private static final String PERMISSIONS_PATH_SEGMENT = "permissions";
    private static final String COLLECTIONS_PATH_SEGMENT = "colls";
    private static final String DOCUMENTS_PATH_SEGMENT = "docs";
    private static final String ATTACHMENTS_PATH_SEGMENT = "attachments";
    private static final String STORED_PROCEDURES_PATH_SEGMENT = "sprocs";
    private static final String TRIGGERS_PATH_SEGMENT = "triggers";
    private static final String USER_DEFINED_FUNCTIONS_PATH_SEGMENT = "udfs";

    public static String getStringFromInputStream(InputStream is) {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();
    }

    public static String getUID() {
        UUID u = UUID.randomUUID();
        return ("" + u.getMostSignificantBits()) + Math.abs(u.getLeastSignificantBits());
    }

    public static String getDatabaseLink(Database database, boolean isNameBased) {
        if (isNameBased) {
            return getDatabaseNameLink(database.getId());
        } else {
            return database.getSelfLink();
        }
    }

    public static String getDatabaseNameLink(String databaseId) {
        return DATABASES_PATH_SEGMENT + "/" + databaseId;
    }

    public static String getUserLink(Database database, User user, boolean isNameBased) {
        if (isNameBased) {
            return getDatabaseLink(database, true) + "/" + USERS_PATH_SEGMENT + "/" + user.getId();
        } else {
            return user.getSelfLink();
        }
    }

    public static String getPermissionLink(Database database, User user, Permission permission,
                                           boolean isNameBased) {
        if (isNameBased) {
            return getUserLink(database, user, true) + "/" + PERMISSIONS_PATH_SEGMENT + "/" + permission.getId();
        } else {
            return permission.getSelfLink();
        }
    }

    public static String getDocumentCollectionLink(Database database, DocumentCollection coll, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionNameLink(database.getId(), coll.getId());
        } else {
            return coll.getSelfLink();
        }
    }

    public static String getDocumentCollectionNameLink(String databaseId, String collectionId) {
        return getDatabaseNameLink(databaseId) + "/" + COLLECTIONS_PATH_SEGMENT + "/" + collectionId;
    }

    public static String getDocumentLink(Database database, DocumentCollection coll, Document doc,
                                         boolean isNameBased) {
        if (isNameBased) {
            return getDocumentNameLink(database.getId(), coll.getId(), doc.getId());
        } else {
            return doc.getSelfLink();
        }
    }

    public static String getDocumentNameLink(String databaseId, String collectionId, String documentId) {
        return getDocumentCollectionNameLink(databaseId, collectionId) + "/" + DOCUMENTS_PATH_SEGMENT + "/" + documentId;
    }

    public static String getAttachmentLink(Database database, DocumentCollection coll, Document doc,
                                           Attachment attachment, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentLink(database, coll, doc, true) + "/" + ATTACHMENTS_PATH_SEGMENT + "/"
                    + attachment.getId();
        } else {
            return attachment.getSelfLink();
        }
    }

    public static String getTriggerLink(Database database, DocumentCollection coll, Trigger trigger,
                                        boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + TRIGGERS_PATH_SEGMENT + "/"
                    + trigger.getId();
        } else {
            return trigger.getSelfLink();
        }
    }

    public static String getStoredProcedureLink(Database database, DocumentCollection coll,
                                                StoredProcedure storedProcedure, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + STORED_PROCEDURES_PATH_SEGMENT + "/"
                    + storedProcedure.getId();
        } else {
            return storedProcedure.getSelfLink();
        }
    }

    public static String getUserDefinedFunctionLink(Database database, DocumentCollection coll,
                                                    UserDefinedFunction userDefinedFunction, boolean isNameBased) {
        if (isNameBased) {
            return getDocumentCollectionLink(database, coll, true) + "/" + USER_DEFINED_FUNCTIONS_PATH_SEGMENT + "/"
                    + userDefinedFunction.getId();
        } else {
            return userDefinedFunction.getSelfLink();
        }
    }
}
