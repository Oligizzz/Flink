package com.mailisu.fraudedetection;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @author KongXiao
 * @date 2023-01-29 21:37
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        // StreamExecutionEnvironment 用于设置你的执行环境。 任务执行环境用于定义任务的属性、创建数据源以及最终启动任务的执行。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");
        // transactions 这个数据流包含了大量的用户交易数据，
        // 需要被划分到多个并发上进行欺诈检测处理。由于欺诈行为的发生是基于某一个账户的，
        // 所以，必须要要保证同一个账户的所有交易行为数据要被同一个并发的 task 进行处理。

        // 为了保证同一个 task 处理同一个 key 的所有数据，你可以使用 DataStream#keyBy 对流进行分区。 process() 函数对流绑定了一个操作，
        // 这个操作将会对流上的每一个消息调用所定义好的函数。 通常，一个操作会紧跟着 keyBy 被调用，
        // 在这个例子中，这个操作是FraudDetector，该操作是在一个 keyed context 上执行的。
        DataStream<Alert> alerts = transactions.keyBy(Transaction::getAccountId).process(new FraudDetector()).name("fraud-detector");

        alerts.addSink(new AlertSink()).name("send-alerts");

        env.execute("Fraud Detection");
    }
}
