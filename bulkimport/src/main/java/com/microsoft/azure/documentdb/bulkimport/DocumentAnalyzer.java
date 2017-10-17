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

import java.util.Collections;
import java.util.Iterator;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;

class DocumentAnalyzer {
    private final static ObjectMapper objectMapper = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(DocumentAnalyzer.class);
    /**
     * Extracts effective {@link PartitionKeyInternal} from serialized document.
     * @param documentAsString Serialized document to extract partition key value from.
     * @param partitionKeyDefinition Information about partition key.
     * @return PartitionKeyInternal
     */
    public static PartitionKeyInternal extractPartitionKeyValue(String documentAsString,
            PartitionKeyDefinition partitionKeyDefinition)  {

        if (partitionKeyDefinition == null || partitionKeyDefinition.getPaths().size() == 0) {
            return PartitionKeyInternal.getEmpty();
        }

        return DocumentAnalyzer.extractPartitionKeyValueInternal(documentAsString, partitionKeyDefinition);
    }

    private static PartitionKeyInternal extractPartitionKeyValueInternal(String documentAsString, PartitionKeyDefinition partitionKeyDefinition) {
        JsonNode root;
        try {
            root = objectMapper.readTree(documentAsString);

            Iterator<String> path = partitionKeyDefinition.getPaths().iterator();   
            JsonNode node =  root.path(path.next().substring(1));

            while(path.hasNext()) {
                node = node.path(path.next());
            }

            Object partitionKeyValue = null;

            switch (node.getNodeType()) {
            case BOOLEAN:
                partitionKeyValue = node.booleanValue();
                break;
            case MISSING:
                partitionKeyValue = Undefined.Value();
                break;
            case NULL:
                partitionKeyValue = JSONObject.NULL;
                break;
            case NUMBER:
                partitionKeyValue = node.numberValue();
                break;
            case STRING:
                partitionKeyValue = node.textValue();
                break;
            default:
                throw new RuntimeException(String.format("undefined json type %s", node.getNodeType()));
            }

            return fromPartitionKeyvalue(partitionKeyValue);
            
        } catch (Exception e) {
            LOGGER.error("Failed to extract partition key value from document {}", documentAsString, e);
            throw ExceptionUtils.toRuntimeException(e);
        }
    }

    public static PartitionKeyInternal fromPartitionKeyvalue(Object partitionKeyValue) {
        try {
            return PartitionKeyInternal.fromObjectArray(Collections.singletonList(partitionKeyValue), true);
        } catch (Exception e) {
            LOGGER.error("Failed to instantiate ParitionKeyInternal from {}", partitionKeyValue, e);
            throw ExceptionUtils.toRuntimeException(e);
        }
    }
}
