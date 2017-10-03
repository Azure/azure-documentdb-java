/* 
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 */

package com.microsoft.azure.documentdb;

enum ResourceType {
    Attachment,
    Conflict,
    Database,
    DatabaseAccount,
    Document,
    DocumentCollection,
    Media,  // Media doesn't have a corresponding resource class.
    Offer,
    Permission,
    StoredProcedure,
    Trigger,
    User,
    UserDefinedFunction
}
