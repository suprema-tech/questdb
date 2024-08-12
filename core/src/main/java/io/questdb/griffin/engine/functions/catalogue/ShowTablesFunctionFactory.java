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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.TelemetryConfigLogger;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;

public class ShowTablesFunctionFactory implements FunctionFactory {
    private static final int DEDUP_NAME_COLUMN;
    private static final int DESIGNATED_TIMESTAMP_COLUMN;
    private static final int DIRECTORY_NAME_COLUMN;
    private static final int ID_COLUMN;
    private static final Log LOG = LogFactory.getLog(ShowTablesFunctionFactory.class);
    private static final int MAX_UNCOMMITTED_ROWS_COLUMN;
    private static final RecordMetadata METADATA;
    private static final int O3_MAX_LAG_COLUMN;
    private static final int PARTITION_BY_COLUMN;
    private static final int WAL_ENABLED_COLUMN;

    @Override
    public String getSignature() {
        return "tables()";
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CursorFunction(new ShowTablesCursorFactory(configuration)) {
            @Override
            public boolean isRuntimeConstant() {
                return true;
            }
        };
    }

    public static class ShowTablesCursorFactory extends AbstractRecordCursorFactory {
        public static final Log LOG = LogFactory.getLog(ShowTablesCursorFactory.class);
        public static final String TABLE_NAME_COLUMN_NAME = "table_name";
        public static final TableColumnMetadata TABLE_NAME_COLUMN_META = new TableColumnMetadata(TABLE_NAME_COLUMN_NAME, ColumnType.STRING);
        private final TableListRecordCursor cursor = new TableListRecordCursor();
        private final boolean hideTelemetryTables; // ignored when showAllTables is set to true
        private final boolean showAllTables;
        private final CharSequence sysTablePrefix;
        private final CharSequence tempPendingRenameTablePrefix;
        private final String toPlan;
        private TableToken tableToken;

        public ShowTablesCursorFactory(CairoConfiguration configuration, RecordMetadata metadata, String toPlan, boolean showAllTables) {
            super(metadata);
            tempPendingRenameTablePrefix = configuration.getTempRenamePendingTablePrefix();
            sysTablePrefix = configuration.getSystemTableNamePrefix();
            hideTelemetryTables = configuration.getTelemetryConfiguration().hideTables();
            this.toPlan = toPlan;
            this.showAllTables = showAllTables;
        }

        public ShowTablesCursorFactory(CairoConfiguration configuration, RecordMetadata metadata, String toPlan) {
            this(configuration, metadata, toPlan, false);
        }

        public ShowTablesCursorFactory(CairoConfiguration configuration) {
            this(configuration, METADATA, "tables()");
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            cursor.of(executionContext);
            return cursor;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type(toPlan);
        }

        @Override
        protected void _close() {
            cursor.close();
        }

        private class TableListRecordCursor implements NoRandomAccessRecordCursor {
            private final TableListRecord record = new TableListRecord();
            private final ObjHashSet<TableToken> tableBucket = new ObjHashSet<>();
            private SqlExecutionContext executionContext;
            private int tableIndex = -1;

            @Override
            public void close() {
                tableIndex = -1;
            }

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public boolean hasNext() {
                if (tableIndex < 0) {
                    executionContext.getCairoEngine().getTableTokens(tableBucket, false);
                    tableIndex = -1;
                }
                tableIndex++;
                int n = tableBucket.size();
                for (; tableIndex < n; tableIndex++) {
                    tableToken = tableBucket.get(tableIndex);
                    if (!TableUtils.isPendingRenameTempTableName(tableToken.getTableName(), tempPendingRenameTablePrefix) && record.open(tableToken)) {
                        break;
                    }
                }
                return tableIndex < n;
            }

            @Override
            public long size() {
                return -1;
            }

            @Override
            public void toTop() {
                tableIndex = -1;
            }

            private void of(SqlExecutionContext executionContext) {
                this.executionContext = executionContext;
                toTop();
            }

            private class TableListRecord implements Record {
                private final CairoTable table = new CairoTable();

                @Override
                public boolean getBool(int col) {
                    if (col == WAL_ENABLED_COLUMN) {
                        return table.getWalEnabledUnsafe();
                    }
                    if (col == DEDUP_NAME_COLUMN) {
                        return table.getIsDedupUnsafe();
                    }
                    return false;
                }

                @Override
                public int getInt(int col) {
                    if (col == ID_COLUMN) {
                        return table.getIdUnsafe();
                    }
                    assert col == MAX_UNCOMMITTED_ROWS_COLUMN;
                    return table.getMaxUncommittedRowsUnsafe();
                }

                @Override
                public long getLong(int col) {
                    assert col == O3_MAX_LAG_COLUMN;
                    return table.getO3MaxLagUnsafe();
                }

                @Override
                public CharSequence getStrA(int col) {
                    if (Chars.equals(ShowTablesCursorFactory.TABLE_NAME_COLUMN_NAME, getMetadata().getColumnName(col))) {
                        return table.getNameUnsafe();
                    }
                    if (col == PARTITION_BY_COLUMN) {
                        return table.getPartitionByUnsafe();
                    }
                    if (col == DESIGNATED_TIMESTAMP_COLUMN) {
                        return table.getTimestampNameUnsafe();
                    }
                    if (col == DIRECTORY_NAME_COLUMN) {
                        if (table.getIsSoftLinkUnsafe()) {
                            return table.getDirectoryNameUnsafe() + " (->)";
                        }
                        return table.getDirectoryNameUnsafe();
                    }
                    return null;
                }

                @Override
                public CharSequence getStrB(int col) {
                    return getStrA(col);
                }

                @Override
                public int getStrLen(int col) {
                    CharSequence str = getStrA(col);
                    return str != null ? str.length() : -1;
                }

                private boolean open(TableToken tableToken) {
                    if (!showAllTables) {
                        // sys table
                        if (Chars.startsWith(tableToken.getTableName(), sysTablePrefix)) {
                            return false;
                        }
                        // telemetry table
                        if (hideTelemetryTables && (Chars.equals(tableToken.getTableName(), TelemetryTask.TABLE_NAME)
                                || Chars.equals(tableToken.getTableName(), TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME))) {
                            return false;
                        }
                    }

                    final CairoEngine engine = executionContext.getCairoEngine();
                    final CairoMetadata metadata = engine.getCairoMetadata();
                    final TableToken lastTableToken = engine.getTableTokenIfExists(tableToken.getTableName());

                    if (lastTableToken == null) {
                        return false;
                    }

                    CairoTable table = metadata.getTableQuiet(lastTableToken.getTableName());

                    if (table == null) {
                        Path path = new Path();
                        CairoMetadata.hydrateTable(lastTableToken, engine.getConfiguration(), path, LOG, new ColumnVersionReader());
                        table = metadata.getTableQuiet(lastTableToken.getTableName());
                        path.close();
                    }

                    assert table != null;

                    table.copyTo(this.table);

                    return true;
                }
            }
        }
    }

    static {
        ID_COLUMN = 0;
        DESIGNATED_TIMESTAMP_COLUMN = 2;
        PARTITION_BY_COLUMN = 3;
        MAX_UNCOMMITTED_ROWS_COLUMN = 4;
        O3_MAX_LAG_COLUMN = 5;
        WAL_ENABLED_COLUMN = 6;
        DIRECTORY_NAME_COLUMN = 7;
        DEDUP_NAME_COLUMN = 8;
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("id", ColumnType.INT));
        metadata.add(ShowTablesCursorFactory.TABLE_NAME_COLUMN_META);
        metadata.add(new TableColumnMetadata("designatedTimestamp", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("partitionBy", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("maxUncommittedRows", ColumnType.INT));
        metadata.add(new TableColumnMetadata("o3MaxLag", ColumnType.LONG));
        metadata.add(new TableColumnMetadata("walEnabled", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("directoryName", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("dedup", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
