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

import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.Misc;

public class IntervalFwdPartitionFrameCursorFactory extends AbstractPartitionFrameCursorFactory {
    private final IntervalFwdPartitionFrameCursor cursor;
    private final RuntimeIntrinsicIntervalModel intervals;
    private IntervalBwdPartitionFrameCursor bwdCursor;

    public IntervalFwdPartitionFrameCursorFactory(
            CairoConfiguration configuration,
            TableToken tableToken,
            long metadataVersion,
            RuntimeIntrinsicIntervalModel intervals,
            int timestampIndex,
            GenericRecordMetadata metadata
    ) {
        super(configuration, tableToken, metadataVersion, metadata);
        this.cursor = new IntervalFwdPartitionFrameCursor(intervals, timestampIndex);
        this.intervals = intervals;
    }

    @Override
    public void close() {
        super.close();
        Misc.free(intervals);
    }

    @Override
    public PartitionFrameCursor getCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        final TableReader reader = getReader(executionContext);
        try {
            if (order == ORDER_ASC || order == ORDER_ANY) {
                cursor.of(reader, executionContext);
                return cursor;
            }

            if (bwdCursor == null) {
                bwdCursor = new IntervalBwdPartitionFrameCursor(intervals, cursor.getTimestampIndex());
            }
            return bwdCursor.of(reader, executionContext);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
    }

    @Override
    public int getOrder() {
        return ORDER_ASC;
    }

    @Override
    public boolean hasInterval() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (sink.getOrder() == ORDER_DESC) {
            sink.type("Interval backward scan");
        } else {
            sink.type("Interval forward scan");
        }
        super.toPlan(sink);
        sink.attr("intervals").val(intervals);
    }
}
