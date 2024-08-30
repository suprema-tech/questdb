/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;

public class FullBwdPartitionFrameCursor extends AbstractFullPartitionFrameCursor {
    protected long rowGroupHi; // used for Parquet frames generation

    public FullBwdPartitionFrameCursor(CairoConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        while (partitionIndex > -1) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi > 0) {
                counter.add(hi);
            }
            partitionIndex--;
        }
    }

    @Override
    public PartitionFrame next() {
        if (rowGroupIndex > -1) {
            frame.partitionIndex = partitionIndex;
            frame.partitionFormat = PartitionFormat.PARQUET;
            frame.rowHi = rowGroupHi;
            frame.rowLo = rowGroupHi - parquetDecoder.getMetadata().rowGroupSize(rowGroupIndex);
            rowGroupHi = frame.rowLo;
            if (--rowGroupIndex == -1) {
                // Proceed to the next partition on the next call.
                partitionIndex--;
            }
            return frame;
        }

        while (partitionIndex > -1) {
            final long hi = reader.openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex--;
            } else {
                final byte format = reader.getPartitionFormat(partitionIndex);

                if (format == PartitionFormat.PARQUET) {
                    reader.initParquetDecoder(parquetDecoder, partitionIndex);
                    final PartitionDecoder.Metadata metadata = parquetDecoder.getMetadata();
                    rowGroupCount = metadata.rowGroupCount();
                    rowGroupIndex = rowGroupCount - 1;
                    rowGroupHi = hi;

                    frame.partitionIndex = partitionIndex;
                    frame.partitionFormat = PartitionFormat.PARQUET;
                    frame.rowHi = rowGroupHi;
                    frame.rowLo = rowGroupHi - parquetDecoder.getMetadata().rowGroupSize(rowGroupIndex);
                    rowGroupHi = frame.rowLo;
                    rowGroupIndex--;
                    return frame;
                }

                frame.partitionIndex = partitionIndex;
                frame.partitionFormat = PartitionFormat.NATIVE;
                frame.rowLo = 0;
                frame.rowHi = hi;
                frame.rowGroupIndex = -1;
                partitionIndex--;
                return frame;
            }
        }
        return null;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        partitionIndex = partitionHi - 1;
        rowGroupIndex = -1;
        rowGroupCount = 0;
        rowGroupHi = 0;
    }
}
