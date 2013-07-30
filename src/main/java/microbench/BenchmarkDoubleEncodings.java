package microbench;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.OrderedFloat64;
import org.apache.hadoop.hbase.types.OrderedNumeric;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.io.DoubleWritable;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.DoubleRowKey;
import com.gotometrics.orderly.DoubleWritableRowKey;
import com.gotometrics.orderly.RowKeyUtils;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkDoubleEncodings {

  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  ByteBuffer buff = ByteBuffer.allocate(100);
  byte[] array = buff.array();
  ImmutableBytesWritable w;
  double val = new Random(System.currentTimeMillis()).nextDouble();

  OrderedFloat64 orderedFloat64;
  OrderedNumeric orderedNumeric;
  ColumnModifier phoenixOrder;
  DoubleWritableRowKey orderlyDoubleWritable;
  DoubleRowKey orderlyDouble;

  @BeforeExperiment
  public void setUp() {
    buff.clear();
    w = new ImmutableBytesWritable(array);
    Arrays.fill(array, (byte) 0);

    orderedFloat64 =
        this.order == Order.ASCENDING ? OrderedFloat64.ASCENDING : OrderedFloat64.DESCENDING;
    orderedNumeric =
        this.order == Order.ASCENDING ? OrderedNumeric.ASCENDING : OrderedNumeric.DESCENDING;
    phoenixOrder = Order.ASCENDING == this.order ? null : ColumnModifier.SORT_DESC;
    orderlyDoubleWritable = new DoubleWritableRowKey();
    orderlyDoubleWritable.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyDouble = new DoubleRowKey();
    orderlyDouble.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
  }

  @AfterExperiment
  public void tearDown() {
    orderedFloat64 = null;
    orderedNumeric = null;
    phoenixOrder = null;
    orderlyDoubleWritable = null;
    orderlyDouble = null;
  }

  @Benchmark
  public int bytes(int reps) {
    if (Order.DESCENDING == this.order) throw new SkipThisScenarioException();
    // tutorial video recommends minimizing member variable access overhead from test
    double val = this.val;
    byte[] array = this.array;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= Bytes.putDouble(array, 0, val);
    }
    return dummy;
  }

  @Benchmark
  public int orderedFloat64Boxing(int reps) {
    ByteBuffer buff = this.buff;
    double val = this.val;
    OrderedFloat64 ob = this.orderedFloat64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      buff.clear();
      ob.encode(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int orderedFloat64Primitive(int reps) {
    ByteBuffer buff = this.buff;
    double val = this.val;
    OrderedFloat64 ob = this.orderedFloat64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      buff.clear();
      ob.encodeDouble(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int orderedBytesNumeric(int reps) {
    ByteBuffer buff = this.buff;
    double val = this.val;
    OrderedNumeric o = this.orderedNumeric;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      buff.clear();
      o.encodeDouble(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int phoenixDecimal(int reps) {
    double val = this.val;
    ColumnModifier order = this.phoenixOrder;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= PDataType.DECIMAL.toBytes(BigDecimal.valueOf(val), order)[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyDoubleWritable(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    DoubleWritable val = new DoubleWritable(this.val);
    DoubleWritableRowKey r = this.orderlyDoubleWritable;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyDouble(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    double val = this.val;
    DoubleRowKey r = this.orderlyDouble;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
