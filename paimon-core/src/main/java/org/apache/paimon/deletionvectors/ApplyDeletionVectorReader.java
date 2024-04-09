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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link RecordReader} which apply {@link DeletionVector} to filter record. */
public class ApplyDeletionVectorReader<T> implements RecordReader<T> {

    private final RecordReader<T> reader;

    private final DeletionVector deletionVector;

    public ApplyDeletionVectorReader(RecordReader<T> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    public static <T> RecordReader<T> create(RecordReader<T> reader, Optional<DeletionVector> dv) {
        return create(reader, dv.orElse(null));
    }

    public static <T> RecordReader<T> create(RecordReader<T> reader, @Nullable DeletionVector dv) {
        if (dv == null) {
            return reader;
        }

        return new ApplyDeletionVectorReader<>(reader, dv);
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        RecordIterator<T> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        checkArgument(
                batch instanceof FileRecordIterator,
                "There is a bug, RecordIterator in ApplyDeletionVectorReader must be RecordWithPositionIterator");

        FileRecordIterator<T> batchWithPosition = (FileRecordIterator<T>) batch;

        return batchWithPosition.filter(
                a -> !deletionVector.isDeleted(batchWithPosition.returnedPosition()));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
