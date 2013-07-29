package microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.OrderedInt64;
import org.apache.hadoop.hbase.types.OrderedNumeric;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.io.LongWritable;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.LongRowKey;
import com.gotometrics.orderly.LongWritableRowKey;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkLongEncodings {

  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  ByteBuffer buff = ByteBuffer.allocate(100);
  byte[] array = buff.array();
  ImmutableBytesWritable w;
  long val = new Random(System.currentTimeMillis()).nextLong();

  OrderedInt64 orderedInt64;
  OrderedNumeric orderedNumeric;
  ColumnModifier phoenixOrder;
  LongWritableRowKey orderlyLongWritable;
  LongRowKey orderlyLong;

  @BeforeExperiment
  public void setUp() {
    buff.clear();
    w = new ImmutableBytesWritable(array);
    Arrays.fill(array, (byte) 0);

    orderedInt64 = this.order == Order.ASCENDING ? OrderedInt64.ASCENDING : OrderedInt64.DESCENDING;
    orderedNumeric =
        this.order == Order.ASCENDING ? OrderedNumeric.ASCENDING : OrderedNumeric.DESCENDING;
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
    orderedInt64 = null;
    orderedNumeric = null;
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
    ByteBuffer buff = this.buff;
    long val = this.val;
    OrderedInt64 ob = this.orderedInt64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ob.encode(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int orderedInt64Primitive(int reps) {
    ByteBuffer buff = this.buff;
    long val = this.val;
    OrderedInt64 ob = this.orderedInt64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ob.encodeLong(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int orderedBytesNumeric(int reps) {
    ByteBuffer buff = this.buff;
    long val = this.val;
    OrderedNumeric o = this.orderedNumeric;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      o.encodeLong(buff, val);
      dummy ^= buff.position();
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
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
