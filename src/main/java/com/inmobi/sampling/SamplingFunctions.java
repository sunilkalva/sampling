package com.inmobi.sampling;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by sunil.kalva on 30/12/14, BENGALORE.
 */
public class SamplingFunctions {

    static public class Initial extends EvalFunc<Tuple> {

        private double sampling = -1.0d;
        private long _localCount = 0L;

        public Initial(String samplingProbability) {
            sampling = Double.parseDouble(samplingProbability);
        }

        @Override
        public Tuple exec(Tuple input) throws IOException {

            DataBag items = (DataBag) input.get(0);
            long numItems = items.size();
            _localCount += numItems;

            return null;
        }
    }
}
