/*
 *  Copyright (c) 2022-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.engine;

import com.logicalclocks.hsfs.spark.engine.hudi.HudiEngine;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHudiEngine {

    @Test
    void testSparkSchemasMatch() throws Exception {
        StructType schema1 = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("age", "int");
        StructType schema2 = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("age", "int");

        HudiEngine engine = new HudiEngine();
        Assertions.assertTrue(engine.sparkSchemasMatch(schema1.fieldNames(), schema2.fieldNames()));
    }

    @Test
    void testSparkSchemasMatchFeatureMissing() throws Exception {
        StructType schema1 = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("age", "int");
        StructType schema2 = new StructType()
                .add("id", "int")
                .add("name", "string");

        HudiEngine engine = new HudiEngine();
        Assertions.assertFalse(engine.sparkSchemasMatch(schema1.fieldNames(), schema2.fieldNames()));
    }

    @Test
    void testSparkSchemasMatchDifferentOrder() throws Exception {
        StructType schema1 = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("age", "int");
        StructType schema2 = new StructType()
                .add("id", "int")
                .add("age", "int")
                .add("name", "string");

        HudiEngine engine = new HudiEngine();
        Assertions.assertTrue(engine.sparkSchemasMatch(schema1.fieldNames(), schema2.fieldNames()));
    }

}
