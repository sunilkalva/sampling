package com.inmobi.sampling;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by sunil.kalva on 30/12/14, BENGALORE.
 */
public class MD5Sampling extends EvalFunc<DataBag> {

    private int reminder = 0;
    private float sampling = 0;
    private int columnNumber = 0;
    private int nonNullColumn = -1;

    public static final String OUTPUT_BAG_NAME_PREFIX = "MD5";

    private static final TupleFactory _TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory _BAG_FACTORY = BagFactory.getInstance();

    public MD5Sampling() {
    }

    public MD5Sampling(String sampling, String columnNumber, String remainder, String nonNullColumn) {
        this.sampling = Float.parseFloat(sampling);
        this.reminder = Integer.parseInt(remainder);
        this.columnNumber = Integer.parseInt(columnNumber);
        this.nonNullColumn = Integer.parseInt(nonNullColumn);
    }

    public DataBag exec(Tuple bag) throws IOException {
        DataBag results = _BAG_FACTORY.newDefaultBag();
        if (null == bag || bag.size() == 0) {
            return results;
        }
        int sample = (int) (100 / sampling);
        for (Tuple input : (DataBag) bag.get(0)) {
            if (nonNullColumn < 0) {
                continue;
            }
            Double nonNullable = Double.valueOf((input.get(nonNullColumn).toString()));
            if (nonNullable <= 0) {
                continue;
            }
            String rid = (String) input.get(columnNumber);
            BigInteger bi = new BigInteger(md5(rid), 16);
            BigInteger[] bigIntegers = bi.divideAndRemainder(new BigInteger(String.valueOf(sample)));
            if (reminder == bigIntegers[1].intValue()) {
                results.add(input);
            }
        }
        return results;
    }

    public String getFinal() {
        return null;
    }

    public String getInitial() {
        return null;
    }

    public String getIntermed() {
        return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema.FieldSchema inputFieldSchema = input.getField(0);
            if (inputFieldSchema.type != DataType.BAG) {
                throw new RuntimeException("Expected a BAG as input");
            }
            return new Schema(
                    new Schema.FieldSchema(super.getSchemaName(OUTPUT_BAG_NAME_PREFIX, input),
                            inputFieldSchema.schema,
                            DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] md5hash = md.digest();
            StringBuilder builder = new StringBuilder();
            for (byte b : md5hash) {
                builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Cannot find digest algorithm");
        }
        return null;
    }

}
