package com.microsoft.azure.documentdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class PartitionKeyTests {
    @Test
    public void TestEffectivePartitionKey() {
        HashMap<Object, String> keyToEffectivePartitionKeyString = new HashMap<Object, String>() {{
            put("", "05C1CF33970FF80800");
            put("partitionKey", "05C1E1B3D9CD2608716273756A756A706F4C667A00");
            put(new String(new char[1024]).replace("\0", "a"), "05C1EB5921F706086262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626200");
            put(null, "05C1ED45D7475601");
            put(JSONObject.NULL, "05C1ED45D7475601");
            put(Undefined.Value(), "05C1D529E345DC00");
            put(true, "05C1D7C5A903D803");
            put(false, "05C1DB857D857C02");
            put(Byte.MIN_VALUE, "05C1D73349F54C053FA0");
            put(Byte.MAX_VALUE, "05C1DD539DDFCC05C05FE0");
            put(Long.MIN_VALUE, "05C1DB35F33D1C053C20");
            put(Long.MAX_VALUE, "05C1B799AB2DD005C3E0");
            put(Integer.MIN_VALUE, "05C1DFBF252BCC053E20");
            put(Integer.MAX_VALUE, "05C1E1F503DFB205C1DFFFFFFFFC");
            put(Double.MIN_VALUE, "05C1E5C91F4D3005800101010101010102");
            put(Double.MAX_VALUE, "05C1CBE367C53005FFEFFFFFFFFFFFFFFE");
        }};

        for (Map.Entry<Object, String> entry : keyToEffectivePartitionKeyString.entrySet()) {
            PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
            partitionKeyDef.setKind(PartitionKind.Hash);
            partitionKeyDef.setPaths(Arrays.asList(new String[]{"\\id"}));
            String actualEffectiveKeyString = new PartitionKey(entry.getKey()).getInternalPartitionKey()
                    .getEffectivePartitionKeyString(partitionKeyDef, true);
            Assert.assertEquals(entry.getValue(), actualEffectiveKeyString);
        }
    }
}
