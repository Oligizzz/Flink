package com.mailisu;

import com.mailisu.datatypes.TaxiRide;
import com.mailisu.sources.TaxiRideGenerator;
import com.mailisu.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: chunzhao
 * @time: 2023/1/30
 */
public class Test {

    private final SourceFunction<TaxiRide> source;

    public Test(SourceFunction<TaxiRide> source) {
        this.source = source;
    }

    public static void main(String[] args) throws Exception {
        Test job = new Test(new TaxiRideGenerator());
        job.execute();
    }

    private void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TaxiRide> sourceStream = env.addSource(new TaxiRideGenerator());
        sourceStream.filter((FilterFunction<TaxiRide>) taxiRide -> GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat));
        sourceStream.print();
        env.execute("test");
    }
}
