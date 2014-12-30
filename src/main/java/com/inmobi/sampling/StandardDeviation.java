package com.inmobi.sampling;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by sunil.kalva on 30/12/14, BENGALORE.
 */
public class StandardDeviation extends EvalFunc<Double> {

    private Double mean;
    private int columnNumber = 0;

    public StandardDeviation(String mean, String columnNumber) {
        this.mean = Double.valueOf(mean);
        this.columnNumber = Integer.parseInt(columnNumber);
    }

    @Override
    public Double exec(Tuple input) throws IOException {

        Double sum = sum(input);
        if (sum == null) {
            return null;
        }
        double count = count(input);
        Double avg = null;
        if (count > 0) {
            avg = sum / (count - 1);
        }
        return Math.sqrt(avg);
    }

    protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag) input.get(0);
        Iterator it = values.iterator();
        long cnt = 0;
        while (it.hasNext()) {
            Tuple t = (Tuple) it.next();
            if (t != null && t.size() > 0 && t.get(columnNumber) != null &&
                    Double.valueOf(t.get(columnNumber).toString()) > 0)
                cnt++;
        }
        return cnt;
    }

    protected Double sum(Tuple input) throws IOException {
        DataBag values = (DataBag) input.get(0);

        if (values.size() == 0) {
            return null;
        }

        double sum = 0;
        boolean sawNonNull = false;
        for (Tuple t : values) {
            try {
                Double d = (Double) (t.get(columnNumber));
                if (d == null) continue;
                sawNonNull = true;
                sum += Math.pow((d - mean), 2d);
            } catch (RuntimeException exp) {
                int errCode = -101;
                String msg = "Problem while computing sqr of difference from mean.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if (sawNonNull) {
            return sum;
        } else {
            return null;
        }
    }
}
