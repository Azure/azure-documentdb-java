package com.microsoft.azure.documentdb;

import org.apache.commons.codec.binary.Base64;

class Helper {
    static String encodeBase64String(byte[] binaryData) {
        String encodedString = Base64.encodeBase64String(binaryData);
        
        if (encodedString.endsWith("\r\n")) {
            encodedString = encodedString.substring(0, encodedString.length() - 2);
        }
        return encodedString;
    }
}
