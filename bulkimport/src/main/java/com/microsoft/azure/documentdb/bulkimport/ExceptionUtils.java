/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.bulkimport;

import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.internal.HttpConstants;

class ExceptionUtils {
    public static boolean isThrottled(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.TOO_MANY_REQUESTS;
    }

    public static boolean isTimedOut(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.TIMEOUT;
    }

    public static boolean isGone(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.GONE;
    }

    public static boolean isSplit(DocumentClientException e) {
        return e.getStatusCode() == HttpConstants.StatusCodes.GONE
                && HttpConstants.SubStatusCodes.SPLITTING == e.getSubStatusCode();
    }
    
    public static DocumentClientException extractDeepestDocumentClientException(Exception e) {
        DocumentClientException dce = null;
        while(e != null) {
            if (e instanceof DocumentClientException) {
                dce = (DocumentClientException) e;
            }
            
            if (e.getCause() instanceof Exception) {
                e = (Exception) e.getCause();
            } else {
                break;
            }
        }
        return dce;
    }
    
    public static RuntimeException toRuntimeException(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new RuntimeException(e);
        }
    }
}
