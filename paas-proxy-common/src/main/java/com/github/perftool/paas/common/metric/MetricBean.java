package com.github.perftool.paas.common.metric;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricBean {

    private static final String PROXY_PRODUCER_TOTAL = "proxy_producer_total";

    private final AtomicInteger producerNum;

    public MetricBean(MeterRegistry meterRegistry) {
        producerNum = meterRegistry.gauge(PROXY_PRODUCER_TOTAL, new AtomicInteger());
    }

    public void recordCachedProducerNum(int producersNum){
        this.producerNum.set(producersNum);
    }

}
