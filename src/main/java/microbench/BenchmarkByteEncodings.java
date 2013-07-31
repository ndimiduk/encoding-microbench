package microbench;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.Order;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.ByteRangeUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.io.BytesWritable;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.FixedBytesWritableRowKey;
import com.gotometrics.orderly.RowKeyUtils;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import com.gotometrics.orderly.VariableLengthBytesWritableRowKey;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkByteEncodings {

  @Param({ "15", "250", "1024" }) int valueLength;
  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  Random rand = new Random(System.currentTimeMillis());
  ByteRange buff = new ByteRange(1024 * 2);
  byte[] array = buff.getBytes();
  ImmutableBytesWritable w;
  byte[] val;
  int valLen;

  ColumnModifier phoenixOrder;
  FixedBytesWritableRowKey orderlyFixedBytesWritable;
  FixedByteArrayRowKey orderlyFixedByteArray;
  VariableLengthBytesWritableRowKey orderlyVariableBytesWritable;
  VariableLengthByteArrayRowKey orderlyVariableByteArray;

  @BeforeExperiment
  public void setUp() {
    ByteRangeUtils.clear(buff);
    w = new ImmutableBytesWritable(array);
    Arrays.fill(array, (byte) 0);

    // initialize val only once per value of valueLength so that all timings
    // are encoding the same value.
    if (null == val || valLen != valueLength) {
      valLen = valueLength;
      Random rand = new Random(System.currentTimeMillis());
      val = new byte[valLen];
      rand.nextBytes(val);

      // honor limitation in simplest encoders
      for (int i = 0; i < val.length; i++) {
        if (val[i] == 0) val[i] += 1;
      }
    }

    phoenixOrder = Order.ASCENDING == this.order ? null : ColumnModifier.SORT_DESC;
    orderlyFixedBytesWritable = new FixedBytesWritableRowKey(valLen);
    orderlyFixedBytesWritable.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyFixedByteArray = new FixedByteArrayRowKey(valLen);
    orderlyFixedByteArray.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyVariableBytesWritable = new VariableLengthBytesWritableRowKey();
    orderlyVariableBytesWritable.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyVariableByteArray = new VariableLengthByteArrayRowKey();
    orderlyVariableByteArray.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
  }

  @AfterExperiment
  public void tearDown() {
    phoenixOrder = null;
    orderlyFixedBytesWritable = null;
    orderlyFixedByteArray = null;
    orderlyVariableBytesWritable = null;
    orderlyVariableByteArray = null;
  }

  @Benchmark
  public int bytes(int reps) {
    if (Order.DESCENDING == this.order) throw new SkipThisScenarioException();
    byte[] val = this.val;
    byte[] array = this.array;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= Bytes.putBytes(array, 0, val, 0, val.length);
    }
    return dummy;
  }

  @Benchmark
  public int orderedBlobCopy(int reps) {
    ByteRange buff = this.buff;
    byte[] val = this.val;
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeBlobCopy(buff, val, ord);
      dummy ^= buff.getPosition();
    }
    return dummy;
  }

  @Benchmark
  public int orderedBlobVar(int reps) {
    ByteRange buff = this.buff;
    byte[] val = this.val;
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeBlobVar(buff, val, ord);
      dummy ^= buff.getPosition();
    }
    return dummy;
  }

  @Benchmark
  public int phoenixBlob(int reps) {
    byte[] val = this.val;
    ColumnModifier order = this.phoenixOrder;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= PDataType.BINARY.toBytes(val, order)[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyFixedBytesWritable(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    BytesWritable val = new BytesWritable(this.val);
    FixedBytesWritableRowKey r = this.orderlyFixedBytesWritable;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyFixedByteArrayRowKey(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    byte[] val = this.val;
    FixedByteArrayRowKey r = this.orderlyFixedByteArray;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyVariableLengthBytesWritableRowKey(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    BytesWritable val = new BytesWritable(this.val);
    VariableLengthBytesWritableRowKey r = this.orderlyVariableBytesWritable;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyVariableLengthByteArrayRowKey(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    byte[] val = this.val;
    VariableLengthByteArrayRowKey r = this.orderlyVariableByteArray;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
