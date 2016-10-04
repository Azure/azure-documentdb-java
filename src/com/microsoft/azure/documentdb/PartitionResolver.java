/*
The MIT License (MIT)
Copyright (c) 2014 Microsoft Corporation

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package com.microsoft.azure.documentdb;

/**
 * This is an interface to be implemented for partitioning scenarios and register it at the document client level
 */
@Deprecated
public interface PartitionResolver {
    /**
     * Gets the collection Self Link or ID based link on which create operation should be directed
     *
     * @param document the document object to be created
     * @return collection SelfLink or ID based link
     */
    String resolveForCreate(Object document);

    /**
     * Gets an iterator of strings representing SelfLink(s) or ID based link(s) on which read/query operation should be directed
     *
     * @param partitionKey partition key used to resolve the collection
     * @return An iterator of strings representing SelfLink(s) or ID based link(s)
     */
    Iterable<String> resolveForRead(Object partitionKey);
}
