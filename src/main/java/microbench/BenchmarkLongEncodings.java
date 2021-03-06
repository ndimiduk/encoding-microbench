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
import org.apache.hadoop.io.LongWritable;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.LongRowKey;
import com.gotometrics.orderly.LongWritableRowKey;
import com.gotometrics.orderly.RowKeyUtils;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkLongEncodings {

  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  ByteRange buff = new ByteRange(100);
  byte[] array = buff.getBytes();
  ImmutableBytesWritable w;
  long val = new Random(System.currentTimeMillis()).nextLong();

  ColumnModifier phoenixOrder;
  LongWritableRowKey orderlyLongWritable;
  LongRowKey orderlyLong;

  @BeforeExperiment
  public void setUp() {
    ByteRangeUtils.clear(buff);
    w = new ImmutableBytesWritable(array);
    Arrays.fill(array, (byte) 0);

    phoenixOrder = Order.ASCENDING == this.order ? null : ColumnModifier.SORT_DESC;
    orderlyLongWritable = new LongWritableRowKey();
    orderlyLongWritable.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyLong = new LongRowKey();
    orderlyLong.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
  }

  @AfterExperiment
  public void tearDown() {
    phoenixOrder = null;
    orderlyLongWritable = null;
    orderlyLong = null;
  }

  @Benchmark
  public int bytes(int reps) {
    if (Order.DESCENDING == this.order) throw new SkipThisScenarioException();
    // tutorial video recommends minimizing member variable access overhead from test
    long val = this.val;
    byte[] array = this.array;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= Bytes.putLong(array, 0, val);
    }
    return dummy;
  }

  @Benchmark
  public int orderedInt64Boxing(int reps) {
    ByteRange buff = this.buff;
    Long val = Long.valueOf(this.val);
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeInt64(buff, val, ord);
      dummy ^= buff.getPosition();
    }
    return dummy;
  }

  @Benchmark
  public int orderedInt64Primitive(int reps) {
    ByteRange buff = this.buff;
    long val = this.val;
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeInt64(buff, val, ord);
      dummy ^= buff.getPosition();
    }
    return dummy;
  }

  @Benchmark
  public int orderedBytesNumeric(int reps) {
    ByteRange buff = this.buff;
    long val = this.val;
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeNumeric(buff, val, ord);
      dummy ^= buff.getPosition();
    }
    return dummy;
  }

  @Benchmark
  public int phoenixLong(int reps) {
    long val = this.val;
    ColumnModifier order = this.phoenixOrder;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= PDataType.LONG.toBytes(val, order)[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyLongWritable(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    LongWritable val = new LongWritable(this.val);
    LongWritableRowKey r = this.orderlyLongWritable;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyLong(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    long val = this.val;
    LongRowKey r = this.orderlyLong;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
