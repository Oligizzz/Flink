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

package com.mailisu.fraudedetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Objects;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    /**
     * ValueState 是一个包装类，类似于 Java 标准库里边的 AtomicReference 和 AtomicLong。 它提供了三个用于交互的方法。update 用于更新状态，
     * value 用于获取状态值，还有 clear 用于清空状态。 如果一个 key 还没有状态，例如当程序刚启动或者调用过 ValueState#clear 方法时，
     * ValueState#value 将会返回 null。 如果需要更新状态，需要调用 ValueState#update 方法，直接更改 ValueState#value 的返回值可能不会被系统识别。
     * 容错处理将在 Flink 后台自动管理，你可以像与常规变量那样与状态变量进行交互。
     */
    private transient ValueState<Boolean> flagState;

    private transient ValueState<Long> timer;

    @Override
    public void open(Configuration configuration) {
        ValueStateDescriptor<Boolean> flagDescription = new ValueStateDescriptor<Boolean>("flag", Boolean.TYPE);
        flagState = getRuntimeContext().getState(flagDescription);

        ValueStateDescriptor<Long> timerDescription = new ValueStateDescriptor<Long>("timer", Long.TYPE);
        timer = getRuntimeContext().getState(timerDescription);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {

        Boolean tag = flagState.value();

        if (!Objects.isNull(tag)) {
            if (transaction.getAmount() >= LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());
                collector.collect(alert);

            }
            Long time = timer.value();
            context.timerService().deleteProcessingTimeTimer(time);
            flagState.clear();
            timer.clear();
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);
            long targetTime = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(targetTime);
            timer.update(targetTime);
        }
    }

    /**
     * @param timestamp The timestamp of the firing timer.
     * @param ctx       An {@link OnTimerContext} that allows querying the timestamp, the {@link
     *                  TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
     *                  registering timers and querying the time. The context is only valid during the invocation
     *                  of this method, do not store it.
     * @param out       The collector for returning result values.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // remove flag after 1 minute
        timer.clear();
        flagState.clear();
    }


}
