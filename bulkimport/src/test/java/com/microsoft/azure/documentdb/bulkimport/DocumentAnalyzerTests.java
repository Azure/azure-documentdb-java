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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.internal.routing.PartitionKeyInternal;

public class DocumentAnalyzerTests {

    @Test
    public void simple() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/city");
        partitionKeyDefinition.setPaths(paths);


        @SuppressWarnings("unused")
        class Pojo {
            public String city;
            public String state;
            public int population;
        }

        Pojo data = new Pojo();
        data.city = "Seattle";
        data.state = "WA";
        data.population = 700000;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList("Seattle"))));
    }

    @Test
    public void compositePath() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/city");
        paths.add("name");

        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class City {
            public String name;
            public String zip;
        }

        @SuppressWarnings("unused")
        class Pojo {
            public City city;
            public String state;
            public int population;
        }

        Pojo data = new Pojo();
        data.city = new City();
        data.city.name = "Seattle";
        data.state = "WA";
        data.population = 700000;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList("Seattle"))));
    }

    @Test
    public void missingPartitionKeyValue() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/city");
        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class Pojo {
            public String state;
            public int population;
        }

        Pojo data = new Pojo();
        data.state = "WA";
        data.population = 700000;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList(Collections.EMPTY_MAP))));
    }
    
    @Test
    public void partitionKeyIntValue() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/zip");
        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class Pojo {
            public String state;
            public int population;
            public int zip;
        }

        Pojo data = new Pojo();
        data.state = "WA";
        data.zip = 98052;
        data.population = 200000;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList((double) data.zip))));
    }
    
    @Test
    public void partitionKeyLongValue() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/area");
        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class Pojo {
            public String state;
            public long area;
        }

        Pojo data = new Pojo();
        data.state = "WA";
        data.area = 9223372036854775000l;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList((double) data.area))));
    }
    
    @Test
    public void nullPartitionKeyValue() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/city");
        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class Pojo {
            public String state;
            public String city;
        }

        Pojo data = new Pojo();
        data.state = "WA";
        data.city = null;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList(data.city))));
    }
    
    @Test
    public void booleanPartitionKeyValue() throws JsonProcessingException {
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        Collection<String> paths = new ArrayList<>();
        paths.add("/isRainy");
        partitionKeyDefinition.setPaths(paths);

        @SuppressWarnings("unused")
        class Pojo {
            public String state;
            public String city;
            public boolean isRainy;
        }

        Pojo data = new Pojo();
        data.state = "WA";
        data.isRainy = true;

        ObjectMapper mapper = new ObjectMapper();
        String dataAsString = mapper.writeValueAsString(data);

        PartitionKeyInternal partitionKeyValue = DocumentAnalyzer.extractPartitionKeyValue(dataAsString, partitionKeyDefinition);
        assertThat(partitionKeyValue.toJson(), equalTo(mapper.writeValueAsString(Collections.singletonList(data.isRainy))));
    }
}
