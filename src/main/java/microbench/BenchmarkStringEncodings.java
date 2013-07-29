package microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.OrderedString;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.StringRowKey;
import com.gotometrics.orderly.UTF8RowKey;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkStringEncodings {

  @Param({ "15", "250", "1024" }) int valueLength;
  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  ByteBuffer buff = ByteBuffer.allocate(1024 * 4);
  byte[] array = buff.array();
  ImmutableBytesWritable w;
  String val;
  byte[] valBytes;
  int valLen = -1;

  OrderedString orderedString;
  ColumnModifier phoenixOrder;
  StringRowKey orderlyString;
  UTF8RowKey orderlyUtf8;

  @BeforeExperiment
  public void setUp() {
    buff.clear();
    w = new ImmutableBytesWritable(array);
    Arrays.fill(array, (byte) 0);

    // initialize val only once per value of valueLength so that all timings
    // are encoding the same value.
    if (null == val || valLen != valueLength) {
      valLen = valueLength;
      Random rand = new Random(System.currentTimeMillis());
      StringBuilder sb = new StringBuilder(valLen);

      for (int i = 0; i < valLen; i++)
        sb.appendCodePoint(rand.nextInt(Character.MAX_CODE_POINT + 1));
      val = sb.toString();
      valBytes = Bytes.toBytes(val);
    }

    orderedString =
        this.order == Order.ASCENDING ? OrderedString.ASCENDING : OrderedString.DESCENDING;
    phoenixOrder = Order.ASCENDING == this.order ? null : ColumnModifier.SORT_DESC;
    orderlyString = new StringRowKey();
    orderlyString.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
    orderlyUtf8 = new UTF8RowKey();
    orderlyUtf8.setOrder(Order.ASCENDING == this.order ?
        com.gotometrics.orderly.Order.ASCENDING :
        com.gotometrics.orderly.Order.DESCENDING);
  }

  @AfterExperiment
  public void tearDown() {
    orderedString = null;
    phoenixOrder = null;
    orderlyString = null;
    orderlyUtf8 = null;
  }

  @Benchmark
  public int bytes(int reps) {
    if (Order.DESCENDING == this.order) throw new SkipThisScenarioException();
    // tutorial video recommends minimizing member variable access overhead from test
    String val = this.val;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= Bytes.toBytes(val)[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderedString(int reps) {
    ByteBuffer buff = this.buff;
    String val = this.val;
    OrderedString ob = this.orderedString;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ob.encode(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  /* disabled; Phoenix CHAR does not support multibyte characters.

  @Benchmark
  public int phoenixString(int reps) {
    String val = this.val;
    ColumnModifier order = this.phoenixOrder;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      dummy ^= PDataType.CHAR.toBytes(val, order)[0];
    }
    return dummy;
  }
  */

  @Benchmark
  public int orderlyString(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    String val = this.val;
    StringRowKey r = this.orderlyString;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }

  @Benchmark
  public int orderlyUtf8(int reps) throws IOException {
    ImmutableBytesWritable w = this.w;
    byte[] valBytes = this.valBytes;
    UTF8RowKey r = this.orderlyUtf8;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      r.serialize(valBytes, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
