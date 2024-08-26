/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.blaze;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BlazeIT {

    protected static SparkSession spark = null;

    @BeforeAll
    public static void startSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.shuffle.manager", BlazeShuffleManager.class.getName())
                .config("spark.sql.extensions", BlazeSparkSessionExtension.class.getName())
                .getOrCreate();
    }

    @Test
    public void testCount() {
        spark.read()
                .parquet("/Users/sh00704ml/data/tpcds_parquet/web_returns")
                .createOrReplaceTempView("web_returns");
        spark.sql("select count(*) from web_returns").show();
    }
}
