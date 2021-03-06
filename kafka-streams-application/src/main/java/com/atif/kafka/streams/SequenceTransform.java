package com.atif.kafka.streams;

import avro.Message.Propensity;
import avro.Message.Row;
import com.atif.kafka.Message.PropensitySerializer;
import com.google.common.collect.MinMaxPriorityQueue;

import java.io.IOException;

public class SequenceTransform {
    private String userId;
    private String userState;
    private String userSegment;
    private MinMaxPriorityQueue<Row> qRows;
    
    public SequenceTransform(String userId, String userState, String userSegment, MinMaxPriorityQueue<Row> q) {
    	this.userId = userId;
    	this.userState = userState;
    	this.userSegment = userSegment;
	this.qRows = q;
    }

    public static byte[] init() throws IOException {
            return (new PropensitySerializer()).serializeMessage(Propensity
                    .newBuilder()
                    .setUserid("")
		    .setState("")
                    .setUsersegment("")
                    .setTimestamp(0L)
                    .setLogtype("Propensity")
                    .build());
    }

    /**
     *  There is scope to improve the following implementation by caching the intermediate results
     *  and computing the incremental delta. However this may not always be possible and it makes
     *  the arrangement less functional.
     */
    public byte[] conversionProbability() throws IOException {
        long tend = qRows.pollLast().getTimestamp();
        long imlookbackindays = 30L;
        long cllookbackindays = 7L;
        long pslookbackindays = 2L;
        long cnlookbackindays = 365L;

        long impressions = qRows
                .stream()
                .filter(r -> r.getTimestamp() >= (tend - imlookbackindays * 24 * 60 * 60 * 1000))
                .filter(r -> (r.getLogtype().toString().equalsIgnoreCase("IM")))
                .count();
        long clicks = qRows
                .stream()
                .filter(r -> r.getTimestamp() >= (tend - cllookbackindays * 24 * 60 * 60 * 1000))
                .filter(r -> (r.getLogtype().toString().equalsIgnoreCase("CL")))
                .count();
        long paidsearch = qRows
                .stream()
                .filter(r -> r.getTimestamp() >= (tend - pslookbackindays * 24 * 60 * 60 * 1000))
                .filter(r -> (r.getLogtype().toString().equalsIgnoreCase("PS")))
                .count();
        long purchases = qRows
                .stream()
                .filter(r -> r.getTimestamp() >= (tend - cnlookbackindays * 24 * 60 * 60 * 1000))
                .filter(r -> (r.getLogtype().toString().equalsIgnoreCase("CN")))
                .count();

        double lnim = java.lang.Math.log(1 + impressions);
        double lncl = java.lang.Math.log(1 + clicks);
        double lnps = java.lang.Math.log(1 + paidsearch);
        double lncn = java.lang.Math.log(1 + purchases);

        double c_base = -2;
        double c_im = 0.01;
        double c_cl = 0.1;
        double c_ps = 1;
        double c_cn = 1.99;
        double alpha = c_base + c_im * lnim + c_cl * lncl + c_ps * lnps + c_cn * lncn;
        double p = 1.0 / (1.0 + java.lang.Math.exp(-1.0 * alpha));

        /**
         * Here be the boilerplate
         */
        Propensity prop = Propensity
                .newBuilder()
                .setUserid(this.userId)
		        .setState(this.userState)
                .setUsersegment(this.userSegment)
                .setTimestamp(tend)
                .setLogtype("PROPENSITY")
                .setPropensity(p)
                .build();

        return (new PropensitySerializer()).serializeMessage(prop);
    }
}

