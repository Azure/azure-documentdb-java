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

import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.Undefined;

public class DocumentDataSource {
    
    public static String randomDocument(Object partitionKeyValue, PartitionKeyDefinition partitionKeyDefinition) {

        Preconditions.checkArgument(partitionKeyDefinition != null &&
                partitionKeyDefinition.getPaths().size() > 0, "there is no partition key definition");

        Collection<String> partitionKeyPath = partitionKeyDefinition.getPaths();
        Preconditions.checkArgument(partitionKeyPath.size() == 1, 
                "the command line benchmark tool only support simple partition key path");

        String partitionKeyName = partitionKeyPath.iterator().next().replaceFirst("^/", "");

        
        Document d = new Document();
        
        d.setId(UUID.randomUUID().toString());
        
        if (!Undefined.Value().equals(partitionKeyValue)) {
            d.set(partitionKeyName, partitionKeyValue);
        }
        
        for(int i = 0; i < 3; i++) {
            d.set(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        
        return d.toJson();
    }
}
