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

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.RowKeyUtils;
import com.gotometrics.orderly.StringRowKey;
import com.gotometrics.orderly.UTF8RowKey;
import com.salesforce.phoenix.schema.ColumnModifier;

@VmOptions({ "-server" })
public class BenchmarkStringEncodings {

  @Param({ "15", "250", "1024" }) int valueLength;
  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  ByteRange buff = new ByteRange(1024 * 4);
  byte[] array = buff.getBytes();
  ImmutableBytesWritable w;
  String val;
  byte[] valBytes;
  int valLen = -1;

  ColumnModifier phoenixOrder;
  StringRowKey orderlyString;
  UTF8RowKey orderlyUtf8;

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
      StringBuilder sb = new StringBuilder(valLen);

      for (int i = 0; i < valLen; i++)
        sb.appendCodePoint(rand.nextInt(Character.MAX_CODE_POINT + 1));
      val = sb.toString();
      valBytes = Bytes.toBytes(val);
    }

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
    ByteRange buff = this.buff;
    String val = this.val;
    Order ord = this.order;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ByteRangeUtils.clear(buff);
      OrderedBytes.encodeString(buff, val, ord);
      dummy ^= buff.getPosition();
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
      RowKeyUtils.seek(w, -w.getOffset());
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
      RowKeyUtils.seek(w, -w.getOffset());
      r.serialize(valBytes, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
