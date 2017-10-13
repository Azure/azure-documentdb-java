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

import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;

class DocumentAnalyzer {
    
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
        String partitionKeyPath = String.join("/", partitionKeyDefinition.getPaths());

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode root;
        try {
            root = objectMapper.readTree(documentAsString);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

        JsonNode node = root.at(partitionKeyPath);
        
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

        return PartitionKeyInternal.fromObjectArray(ImmutableList.of(partitionKeyValue), true);
    }
}
