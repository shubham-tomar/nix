package com.kafka.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.sink.FlinkSink;
import java.util.Map;
import java.util.HashMap;
import com.kafka.flink.utils.Utils;

public class KafkaFlink {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().inStreamingMode().build());

        env.enableCheckpointing(120000);
        String branchName = "dummy";

        Utils.ListNessieBranches();
        Utils.CreateBranch("main", branchName);

        // Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("dummy-src")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<RowData> rowDataStream = kafkaStream.map(new KafkaJsonToRowDataMapper());

        if (args.length < 1) {
            throw new IllegalArgumentException("Table name must be provided as an argument");
        }
        String tableName = args[0];
        TableIdentifier tableIdentifier = TableIdentifier.of("flink", tableName);
        TableLoader tableLoader = TableLoader.fromCatalog(getCatalogLoader(), tableIdentifier);
        tableLoader.open();

        // Writing Kafka data to Iceberg Table
        FlinkSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .toBranch("dummy_2")
                .distributionMode(DistributionMode.HASH)
                .writeParallelism(1)
                .upsert(false)
                .append();

        // Register Nessie Catalog in Flink
        Catalog flinkCatalog = new FlinkCatalog("nessie", getCatalogLoader());
        tableEnv.registerCatalog("nessie", flinkCatalog);
        tableEnv.useCatalog("nessie");

        // List all tables to verify connection
        tableEnv.executeSql("SHOW TABLES").print();

        // Read from Iceberg table
        Table icebergTable = tableEnv.sqlQuery("SELECT * FROM nessie.flink." + tableName);
        tableEnv.toDataStream(icebergTable).print();

        env.execute("Kafka to Iceberg Streaming Job");

        Utils.ListNessieBranches();
    }

    public static CatalogLoader getCatalogLoader() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "http://localhost:19120/api/v1");
        props.put("ref", "dummy_2");
        
        // Ensure correct S3 bucket path
        props.put("warehouse", "s3a://my-s3-bucket/warehouse"); // ✅ FIXED BUCKET NAME
        props.put("s3.endpoint", "http://localhost:9000");
        props.put("s3.region", "us-east-1");
        props.put("s3.access-key", "admin");
        props.put("s3.secret-key", "password");
        props.put("s3.path-style-access", "true");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("write.format.default", "parquet");

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.access.key", "admin");
        hadoopConf.set("fs.s3a.secret.key", "password");
        hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        return CatalogLoader.custom(
                "nessie",
                props,
                hadoopConf, "org.apache.iceberg.nessie.NessieCatalog");
    }

}
