/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.CompactorSourceBuilder;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.flink.utils.MetricUtils;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactorSinkBuilder} and {@link CompactorSink}. */
public class CompactorSinkITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                    },
                    new String[] {"k", "v", "hh", "dt"});

    private Path tablePath;
    private String commitUser;

    @BeforeEach
    public void before() throws IOException {
        tablePath = new Path(getTempDirPath());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testCompact() throws Exception {
        FileStoreTable table = createFileStoreTable();
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        write.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 200, 15, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 16, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 15, BinaryString.fromString("20221209")));
        commit.commit(1, write.prepareCommit(true, 1));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        write.close();
        commit.close();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(tablePath.toString(), table);
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(false)
                        .withPartitions(getSpecifiedPartitions())
                        .build();
        new CompactorSinkBuilder(table).withInput(source).build();
        env.execute();

        snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        TableScan.Plan plan = table.newReadBuilder().newScan().plan();
        assertThat(plan.splits().size()).isEqualTo(3);
        for (Split split : plan.splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().getInt(1) == 15) {
                // compacted
                assertThat(dataSplit.dataFiles().size()).isEqualTo(1);
            } else {
                // not compacted
                assertThat(dataSplit.dataFiles().size()).isEqualTo(2);
            }
        }
    }

    @Test
    public void testCompactParallelism() throws Exception {
        FileStoreTable table = createFileStoreTable();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CompactorSourceBuilder sourceBuilder =
                new CompactorSourceBuilder(tablePath.toString(), table);
        DataStreamSource<RowData> source =
                sourceBuilder
                        .withEnv(env)
                        .withContinuousMode(false)
                        .withPartitions(getSpecifiedPartitions())
                        .build();
        Integer sinkParalellism = new Random().nextInt(100) + 1;
        new CompactorSinkBuilder(
                        table.copy(
                                new HashMap<String, String>() {
                                    {
                                        put(
                                                FlinkConnectorOptions.SINK_PARALLELISM.key(),
                                                String.valueOf(sinkParalellism));
                                    }
                                }))
                .withInput(source)
                .build();

        Assertions.assertThat(env.getTransformations().get(0).getParallelism())
                .isEqualTo(sinkParalellism);
    }

    @Test
    public void testCompactionMetrics() throws Exception {
        FileStoreTable table = createFileStoreTable();
        prepareDataFile(table);

        StoreCompactOperator operator = createCompactOperator(table);
        OneInputStreamOperatorTestHarness<RowData, Committable> testHarness =
                createTestHarness(operator);
        testHarness.open();

        MetricGroup compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("partition", "dt=20221208_hh=15")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        DataFileMetaSerializer fileMetaSerializer = new DataFileMetaSerializer();
        RowData record =
                new FlinkRowData(
                        GenericRow.of(
                                1L,
                                partition("20221208", 15),
                                0,
                                fileMetaSerializer.serializeList(Collections.emptyList())));

        long timestamp = 0;
        testHarness.processElement(record, timestamp++);
        testHarness.endInput();
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", table.name())
                        .addGroup("partition", "dt=20221208_hh=16")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        record =
                new FlinkRowData(
                        GenericRow.of(
                                2L,
                                partition("20221208", 16),
                                0,
                                fileMetaSerializer.serializeList(Collections.emptyList())));

        testHarness.processElement(record, timestamp);
        testHarness.endInput();
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        // operator closed, metric groups should be unregistered
        testHarness.close();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted"))
                .isNull();
    }

    @Test
    public void testMultiTablesCompactionMetrics(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Path warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir);
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse.toString());
        catalogOptions.set(CatalogOptions.URI, "");
        Catalog.Loader catalogLoader =
                () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        Catalog catalog = catalogLoader.load();
        String databaseName = "test_db";
        catalog.createDatabase(databaseName, true);
        Identifier firstTableId = Identifier.create(databaseName, "test_table1");
        Identifier secondTableId = Identifier.create(databaseName, "test_table2");
        FileStoreTable firstTable = createCatalogTable(catalog, firstTableId);
        FileStoreTable secondTable = createCatalogTable(catalog, secondTableId);
        prepareDataFile(firstTable);
        prepareDataFile(secondTable);

        MultiTablesStoreCompactOperator operator = createMultiTablesCompactOperator(catalogLoader);
        OneInputStreamOperatorTestHarness<RowData, MultiTableCommittable> testHarness =
                createMultiTablesTestHarness(operator);
        testHarness.open();

        MetricGroup compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", firstTable.name())
                        .addGroup("partition", "_")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        DataFileMetaSerializer fileMetaSerializer = new DataFileMetaSerializer();
        RowData record =
                new FlinkRowData(
                        GenericRow.of(
                                1L,
                                serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                0,
                                fileMetaSerializer.serializeList(Collections.emptyList()),
                                BinaryString.fromString(databaseName),
                                BinaryString.fromString(firstTableId.getObjectName())));

        long timestamp = 0;
        testHarness.processElement(record, timestamp++);
        testHarness.endInput();
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        compactionMetricGroup =
                operator.getMetricGroup()
                        .addGroup("paimon")
                        .addGroup("table", secondTable.name())
                        .addGroup("partition", "_")
                        .addGroup("bucket", "0")
                        .addGroup("compaction");
        record =
                new FlinkRowData(
                        GenericRow.of(
                                2L,
                                serializeBinaryRow(BinaryRow.EMPTY_ROW),
                                0,
                                fileMetaSerializer.serializeList(Collections.emptyList()),
                                BinaryString.fromString(databaseName),
                                BinaryString.fromString(secondTableId.getObjectName())));

        testHarness.processElement(record, timestamp);
        testHarness.endInput();
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore")
                                .getValue())
                .isEqualTo(2L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter")
                                .getValue())
                .isEqualTo(1L);
        assertThat(
                        MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted")
                                .getValue())
                .isEqualTo(0L);

        // operator closed, metric groups should be unregistered
        testHarness.close();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedBefore"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastTableFilesCompactedAfter"))
                .isNull();
        assertThat(MetricUtils.getGauge(compactionMetricGroup, "lastChangelogFilesCompacted"))
                .isNull();
    }

    private List<Map<String, String>> getSpecifiedPartitions() {
        Map<String, String> partition1 = new HashMap<>();
        partition1.put("dt", "20221208");
        partition1.put("hh", "15");

        Map<String, String> partition2 = new HashMap<>();
        partition2.put("dt", "20221209");
        partition2.put("hh", "15");

        return Arrays.asList(partition1, partition2);
    }

    private GenericRow rowData(Object... values) {
        return GenericRow.of(values);
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.emptyMap(),
                                ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private FileStoreTable createCatalogTable(Catalog catalog, Identifier tableIdentifier)
            throws Exception {
        Schema tableSchema =
                new Schema(
                        ROW_TYPE.getFields(),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyMap(),
                        "");
        catalog.createTable(tableIdentifier, tableSchema, false);
        return (FileStoreTable) catalog.getTable(tableIdentifier);
    }

    private OneInputStreamOperatorTestHarness<RowData, Committable> createTestHarness(
            OneInputStreamOperator<RowData, Committable> operator) throws Exception {
        TypeSerializer<Committable> serializer =
                new CommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<RowData, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(serializer);
        return harness;
    }

    private OneInputStreamOperatorTestHarness<RowData, MultiTableCommittable>
            createMultiTablesTestHarness(
                    OneInputStreamOperator<RowData, MultiTableCommittable> operator)
                    throws Exception {
        TypeSerializer<MultiTableCommittable> serializer =
                new MultiTableCommittableTypeInfo().createSerializer(new ExecutionConfig());
        OneInputStreamOperatorTestHarness<RowData, MultiTableCommittable> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(serializer);
        return harness;
    }

    protected StoreCompactOperator createCompactOperator(FileStoreTable table) {
        return new StoreCompactOperator(
                table,
                (t, commitUser, state, ioManager, memoryPool, metricGroup) ->
                        new StoreSinkWriteImpl(
                                t,
                                commitUser,
                                state,
                                ioManager,
                                false,
                                false,
                                false,
                                memoryPool,
                                metricGroup),
                "test");
    }

    protected MultiTablesStoreCompactOperator createMultiTablesCompactOperator(
            Catalog.Loader catalogLoader) throws Exception {
        return new MultiTablesStoreCompactOperator(
                catalogLoader, commitUser, new CheckpointConfig(), false, false, new Options());
    }

    private static byte[] partition(String dt, int hh) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeString(0, BinaryString.fromString(dt));
        writer.writeInt(1, hh);
        writer.complete();
        return serializeBinaryRow(row);
    }

    private void prepareDataFile(FileStoreTable table) throws Exception {
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        write.write(rowData(1, 100, 15, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 16, BinaryString.fromString("20221208")));
        write.write(rowData(1, 100, 15, BinaryString.fromString("20221209")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(2, 200, 15, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 16, BinaryString.fromString("20221208")));
        write.write(rowData(2, 200, 15, BinaryString.fromString("20221209")));
        commit.commit(1, write.prepareCommit(true, 1));

        write.close();
        commit.close();
    }
}
