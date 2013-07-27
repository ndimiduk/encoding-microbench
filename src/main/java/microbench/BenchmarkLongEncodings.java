package microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

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

  @Param({ "100" }) int numVals;
  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  Random rand = new Random(System.currentTimeMillis());
  ByteBuffer[] buffs;
  byte[][] arrays;
  long[] vals;
  LongWritable[] ws;

  OrderedInt64 orderedInt64;
  OrderedNumeric orderedNumeric;
  ColumnModifier phoenixOrder;
  LongWritableRowKey orderlyLongWritable;
  LongRowKey orderlyLong;

  @BeforeExperiment
  public void setUp() {
    buffs = new ByteBuffer[numVals];
    arrays = new byte[numVals][];
    vals = new long[numVals];
    ws = new LongWritable[numVals];

    for (int i = 0; i < numVals; i++) {
      buffs[i] = ByteBuffer.allocate(100);
      arrays[i] = buffs[i].array();
      vals[i] = rand.nextLong();
      ws[i] = new LongWritable(vals[i]);
    }

    orderedInt64 = this.order == Order.ASCENDING ? OrderedInt64.ASCENDING : OrderedInt64.DESCENDING;
    orderedNumeric = this.order == Order.ASCENDING ?
        OrderedNumeric.ASCENDING :
        OrderedNumeric.DESCENDING;
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
    buffs = null;
    vals = null;
    ws = null;

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
    long[] vals = this.vals;
    byte[][] arrays = this.arrays;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        dummy ^= Bytes.putLong(arrays[j], 0, vals[j]);
      }
    }
    return dummy;
  }

  @Benchmark
  public int orderedInt64Boxing(int reps) {
    ByteBuffer[] buffs = this.buffs;
    long[] vals = this.vals;
    OrderedInt64 ob = this.orderedInt64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        buffs[j].clear();
        ob.encode(buffs[j], vals[j]);
        dummy ^= buffs[j].position();
      }
    }
    return dummy;
  }

  @Benchmark
  public int orderedInt64Primitive(int reps) {
    ByteBuffer[] buffs = this.buffs;
    long[] vals = this.vals;
    OrderedInt64 ob = this.orderedInt64;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        buffs[j].clear();
        ob.encodeLong(buffs[j], vals[j]);
        dummy ^= buffs[j].position();
      }
    }
    return dummy;
  }

  @Benchmark
  public int orderedBytesNumeric(int reps) {
    ByteBuffer[] buffs = this.buffs;
    long[] vals = this.vals;
    OrderedNumeric o = this.orderedNumeric;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        buffs[j].clear();
        o.encodeLong(buffs[j], vals[j]);
        dummy ^= buffs[j].position();
      }
    }
    return dummy;
  }

  @Benchmark
  public int phoenixLong(int reps) {
    long[] vals = this.vals;
    ColumnModifier order = this.phoenixOrder;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        dummy ^= System.identityHashCode(PDataType.LONG.toBytes(vals[j], order));
      }
    }
    return dummy;
  }

  @Benchmark
  public int orderlyLongWritable(int reps) throws IOException {
    byte[][] arrays = this.arrays;
    LongWritableRowKey r = orderlyLongWritable;
    LongWritable[] ws = this.ws;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        r.serialize(ws[j], arrays[j], 0);
        dummy ^= arrays[j][0];
      }
    }
    return dummy;
  }

  @Benchmark
  public int orderlyLong(int reps) throws IOException {
    byte[][] arrays = this.arrays;
    long[] vals = this.vals;
    LongRowKey r = orderlyLong;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      for (int j = 0; j < vals.length; j++) {
        r.serialize(vals[j], arrays[j], 0);
        dummy ^= arrays[j][0];
      }
    }
    return dummy;
  }
}
