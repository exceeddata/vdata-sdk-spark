package com.exceeddata.example.spark;

import com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig;
import com.exceeddata.sdk.vdata.spark.v2.VDataSparkDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
    public static void main(String[] args) {
        String path="/Users/garyshi/Documents/gitwork/sdk-example/sdk-vdata-python/sample_files/vsw/data_diff_freqency.vsw";

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLTest！！！")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .format(VDataSparkDataSource.class.getName())
                .option(VDataSparkConfig.INPUT_PATH,path)
                .option(VDataSparkConfig.INPUT_COLUMNS,"_device,_time,Signal_100Hz,Signal_20Hz,Signal_10Hz")
                .option(VDataSparkConfig.SCHEMA_NUM_MAXREAD,1)
                .option(VDataSparkConfig.DENSIFY_ROWS_AHEAD, 0)
                .load();
        df.show();
        spark.close();
    }
}
