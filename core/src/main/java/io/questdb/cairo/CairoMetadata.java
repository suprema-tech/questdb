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

import io.questdb.std.CharSequenceObjHashMap;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.locks.StampedLock;

public class CairoMetadata {
    private CharSequenceObjHashMap<CairoTable> tables;
    // used to protect the tables hashmap
    private StampedLock tablesLock;

    public CairoMetadata() {
        this.tables = new CharSequenceObjHashMap<>();
        this.tablesLock = new StampedLock();
    }

    public CairoTable getTableQuick(@NotNull CharSequence tableName) {
        final CairoTable tbl = getTableQuiet(tableName);
        if (tbl == null) {
            throw CairoException.tableDoesNotExist(tableName);
        }
        return tbl;
    }

    public CairoTable getTableQuiet(@NotNull CharSequence tableName) {
        // try an optimistic read
        long optimisticStamp = tablesLock.tryOptimisticRead();
        CairoTable tbl = tables.get(tableName);
        // check the read
        if (!tablesLock.validate(optimisticStamp)) {
            // upgrade the lock
            final long upgradedStamp = tablesLock.readLock();
            tbl = tables.get(tableName);
            tablesLock.unlockRead(upgradedStamp);
        }
        return tbl;
    }

}


