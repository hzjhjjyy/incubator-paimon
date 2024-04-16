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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Index file writer. */
public final class FileIndexWriter implements Closeable {

    public static final FileIndexResult EMPTY_RESULT = FileIndexResult.of(null, null);

    private final FileIO fileIO;

    private final Path path;

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long inManifestThreshold;

    private final List<IndexMaintainer> indexMaintainers = new ArrayList<>();

    private String resultFileName;

    private byte[] embeddedIndexBytes;

    public FileIndexWriter(
            FileIO fileIO, Path path, RowType rowType, FileIndexOptions fileIndexOptions) {
        this.fileIO = fileIO;
        this.path = path;
        List<DataField> fields = rowType.getFields();
        Map<String, DataField> map = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        fields.forEach(
                dataField -> {
                    map.put(dataField.name(), dataField);
                    index.put(dataField.name(), rowType.getFieldIndex(dataField.name()));
                });
        for (Map.Entry<String, Map<String, Options>> entry : fileIndexOptions.entrySet()) {
            String columnName = entry.getKey();
            DataField field = map.get(columnName);
            if (field == null) {
                throw new IllegalArgumentException(columnName + " does not exist in column fields");
            }
            for (Map.Entry<String, Options> typeEntry : entry.getValue().entrySet()) {
                String indexType = typeEntry.getKey();
                indexMaintainers.add(
                        new IndexMaintainer(
                                columnName,
                                indexType,
                                FileIndexer.create(indexType, field.type(), typeEntry.getValue())
                                        .createWriter(),
                                InternalRow.createFieldGetter(
                                        field.type(), index.get(columnName))));
            }
        }
        this.inManifestThreshold = fileIndexOptions.fileIndexInManifestThreshold();
    }

    public void write(InternalRow row) {
        indexMaintainers.forEach(indexMaintainer -> indexMaintainer.write(row));
    }

    @Override
    public void close() throws IOException {
        Map<String, Map<String, byte[]>> indexMaps = new HashMap<>();

        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            indexMaps
                    .computeIfAbsent(indexMaintainer.getColumnName(), k -> new HashMap<>())
                    .put(indexMaintainer.getIndexType(), indexMaintainer.serializedBytes());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos)) {
            writer.writeColumnIndexes(indexMaps);
        }

        if (baos.size() > inManifestThreshold) {
            try (OutputStream outputStream = fileIO.newOutputStream(path, false)) {
                outputStream.write(baos.toByteArray());
            }
            resultFileName = path.getName();
        } else {
            embeddedIndexBytes = baos.toByteArray();
        }
    }

    public FileIndexResult result() {
        return FileIndexResult.of(embeddedIndexBytes, resultFileName);
    }

    @Nullable
    public static FileIndexWriter create(
            FileIO fileIO, Path path, RowType rowType, FileIndexOptions fileIndexOptions) {
        return fileIndexOptions.isEmpty()
                ? null
                : new FileIndexWriter(fileIO, path, rowType, fileIndexOptions);
    }

    /** File index result. */
    public interface FileIndexResult {

        @Nullable
        byte[] embeddedIndexBytes();

        @Nullable
        String independentIndexFile();

        static FileIndexResult of(byte[] embeddedIndexBytes, String resultFileName) {
            return new FileIndexResult() {

                @Override
                public byte[] embeddedIndexBytes() {
                    return embeddedIndexBytes;
                }

                @Override
                public String independentIndexFile() {
                    return resultFileName;
                }
            };
        }
    }

    /** One index maintainer for one column. */
    private static class IndexMaintainer {

        private final String columnName;
        private final String indexType;
        private final org.apache.paimon.fileindex.FileIndexWriter fileIndexWriter;
        private final InternalRow.FieldGetter getter;

        public IndexMaintainer(
                String columnName,
                String indexType,
                org.apache.paimon.fileindex.FileIndexWriter fileIndexWriter,
                InternalRow.FieldGetter getter) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.fileIndexWriter = fileIndexWriter;
            this.getter = getter;
        }

        public void write(InternalRow row) {
            fileIndexWriter.write(getter.getFieldOrNull(row));
        }

        public String getIndexType() {
            return indexType;
        }

        public String getColumnName() {
            return columnName;
        }

        public byte[] serializedBytes() {
            return fileIndexWriter.serializedBytes();
        }
    }
}
