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
package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CairoColumn;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class ShowColumnsRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final int N_NAME_COL = 0;
    public static final int N_TYPE_COL = N_NAME_COL + 1;
    private static final int N_INDEXED_COL = N_TYPE_COL + 1;
    private static final int N_INDEX_BLOCK_CAPACITY_COL = N_INDEXED_COL + 1;
    private static final int N_SYMBOL_CACHED_COL = N_INDEX_BLOCK_CAPACITY_COL + 1;
    private static final int N_SYMBOL_CAPACITY_COL = N_SYMBOL_CACHED_COL + 1;
    private static final int N_DESIGNATED_COL = N_SYMBOL_CAPACITY_COL + 1;
    private static final int N_UPSERT_KEY_COL = N_DESIGNATED_COL + 1;
    private static final RecordMetadata METADATA;
    private final ShowColumnsCursor cursor = new ShowColumnsCursor();
    private final TableToken tableToken;
    private final int tokenPosition;

    public ShowColumnsRecordCursorFactory(TableToken tableToken, int tokenPosition) {
        super(METADATA);
        this.tableToken = tableToken;
        this.tokenPosition = tokenPosition;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return cursor.of(executionContext, tableToken, tokenPosition);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("show_columns");
        sink.meta("of").val(tableToken);
    }

    public static class ShowColumnsCursor implements NoRandomAccessRecordCursor {
        private final ShowColumnsRecord record = new ShowColumnsRecord();
        private CairoColumn cairoColumn = new CairoColumn();
        private CairoMetadata cairoMetadata;
        private CairoTable cairoTable;
        private int columnIndex;
        private SqlExecutionContext executionContext;
        //        private TableReader reader;
        private ObjList<CharSequence> names;

        @Override
        public void close() {
//            reader = Misc.free(reader);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            columnIndex++;
            if (columnIndex < cairoTable.getColumnCount()) {
                cairoTable.getColumnQuick(names.getQuick(columnIndex)).copyTo(cairoColumn);
                return true;
            }
            return false;
        }

        public ShowColumnsCursor of(SqlExecutionContext executionContext, TableToken tableToken, int tokenPosition) {
            try {
//                reader = executionContext.getReader(tableToken);
                this.executionContext = executionContext;
                cairoMetadata = executionContext.getCairoEngine().getCairoMetadata();
                cairoTable = cairoMetadata.getTableQuick(tableToken.getTableName());
                names = cairoTable.getColumnNames();
            } catch (CairoException e) {
                e.position(tokenPosition);
                throw e;
            }
            toTop();
            return this;
        }

        public ShowColumnsCursor of(SqlExecutionContext executionContext, CharSequence tableName) {
            return of(executionContext, executionContext.getTableTokenIfExists(tableName), -1);
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            columnIndex = -1;
        }

        public class ShowColumnsRecord implements Record {

            @Override
            public boolean getBool(int col) {
                if (col == N_INDEXED_COL) {
                    return cairoColumn.getIsIndexedUnsafe();
                }
                if (col == N_SYMBOL_CACHED_COL) {
                    return cairoColumn.getSymbolCachedUnsafe();
//                    if (ColumnType.isSymbol(reader.getMetadata().getColumnType(columnIndex))) {
//                        return reader.getSymbolMapReader(columnIndex).isCached();
//                    } else {
//                        return false;
//                    }
                }
                if (col == N_DESIGNATED_COL) {
                    return cairoColumn.getDesignatedUnsafe();
                }
                if (col == N_UPSERT_KEY_COL) {
                    return cairoColumn.getIsDedupKeyUnsafe();
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInt(int col) {
                if (col == N_INDEX_BLOCK_CAPACITY_COL) {
                    return cairoColumn.getIndexBlockCapacityUnsafe();
                }
                if (col == N_SYMBOL_CAPACITY_COL) {
                    return cairoColumn.getSymbolCapacityUnsafe();
//                    if (ColumnType.isSymbol(reader.getMetadata().getColumnType(columnIndex))) {
//                        return reader.getSymbolMapReader(columnIndex).getSymbolCapacity();
//                    } else {
//                        return 0;
//                    }
                }
                throw new UnsupportedOperationException();
            }

            @Override
            @NotNull
            public CharSequence getStrA(int col) {
                if (col == N_NAME_COL) {
                    return cairoColumn.getNameUnsafe();
                }
                if (col == N_TYPE_COL) {
                    return ColumnType.nameOf(cairoColumn.getTypeUnsafe());
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getStrB(int col) {
                return getStrA(col);
            }

            @Override
            public int getStrLen(int col) {
                return getStrA(col).length();
            }
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("column", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("type", ColumnType.STRING));
        metadata.add(new TableColumnMetadata("indexed", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("indexBlockCapacity", ColumnType.INT));
        metadata.add(new TableColumnMetadata("symbolCached", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("symbolCapacity", ColumnType.INT));
        metadata.add(new TableColumnMetadata("designated", ColumnType.BOOLEAN));
        metadata.add(new TableColumnMetadata("upsertKey", ColumnType.BOOLEAN));
        METADATA = metadata;
    }
}
