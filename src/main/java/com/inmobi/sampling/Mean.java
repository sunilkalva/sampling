package com.inmobi.sampling;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Iterator;

public class Mean extends EvalFunc<Double> implements Accumulator<Double> {

    private int columnNumber = 0;

    public Mean() {
    }

    public Mean(String columnNumber) {
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
        if (count > 0)
            avg = sum / count;
        return avg;
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

        // if we were handed an empty bag, return NULL
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
                sum += d;
            } catch (RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of doubles.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if (sawNonNull) {
            return sum;
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
    }

    private Double intermediateSum = null;
    private Double intermediateCount = null;

    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Double sum = sum(b);
            if (sum == null) {
                return;
            }
            // set default values
            if (intermediateSum == null || intermediateCount == null) {
                intermediateSum = 0.0;
                intermediateCount = 0.0;
            }

            double count = count(b);

            if (count > 0) {
                intermediateCount += count;
                intermediateSum += sum;
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing average in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        intermediateSum = null;
        intermediateCount = null;
    }

    @Override
    public Double getValue() {
        Double avg = null;
        if (intermediateCount != null && intermediateCount > 0) {
            avg = intermediateSum / intermediateCount;
        }
        return avg;
    }
}