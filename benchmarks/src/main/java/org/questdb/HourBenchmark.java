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

package org.questdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HourBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HourBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(1)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int testHour(MyState state) {
        return hour(state.ts);
    }

    @Benchmark
    public int testHourGcc(MyState state) {
        return hour_gcc(state.ts);
    }

    private static int hour(long ts) {
        return (int) ((ts / 3600000000L) % 24L);
    }

    /**
     * This is a broken port of reverse engineered assembly produced by GCC for the following code:
     * <pre>
     * #include <cstdio>
     * #include <cstdlib>
     *
     * #define HOUR_MICROS  3600000000LL
     * #define DAY_HOURS  24LL
     *
     * int32_t int64_to_hour(int64_t micro) {
     *     if (micro > -1) {
     *         int64_t div = micro / HOUR_MICROS;
     *         return div % DAY_HOURS;
     *     }
     *     return 0;
     * }
     *
     * int main()
     * {
     *     std::printf("%d\n", int64_to_hour(rand()));
     *     return 0;
     * }
     * </pre>
     */
    private static int hour_gcc(long ts) {
        long multiplier = -7442832613395060283L;
        long result = ts * multiplier;
        long remainder = result >> 31;
        long divisor = -6148914691236517205L;
        long hours = (remainder - (ts >> 63)) * divisor;
        return (int) (hours >> 4);
    }

    @State(Scope.Thread)
    public static class MyState {
        public long ts = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
