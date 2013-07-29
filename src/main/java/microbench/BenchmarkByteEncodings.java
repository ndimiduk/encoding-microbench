package microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.types.OrderedBlob;
import org.apache.hadoop.hbase.types.OrderedBlobVar;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.io.BytesWritable;

import com.google.caliper.AfterExperiment;
import com.google.caliper.BeforeExperiment;
import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.google.caliper.api.SkipThisScenarioException;
import com.google.caliper.api.VmOptions;
import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.FixedBytesWritableRowKey;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import com.gotometrics.orderly.VariableLengthBytesWritableRowKey;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;

@VmOptions({ "-server" })
public class BenchmarkByteEncodings {

  @Param({ "15", "250", "1024" }) int valueLength;
  @Param({ "ASCENDING", "DESCENDING" }) Order order;

  Random rand = new Random(System.currentTimeMillis());
  ByteBuffer buff = ByteBuffer.allocate(1024 * 2);
  byte[] array = buff.array();
  ImmutableBytesWritable w;
  byte[] val;
  int valLen;

  OrderedBlob orderedBlob;
  OrderedBlobVar orderedBlobVar;
  ColumnModifier phoenixOrder;
  FixedBytesWritableRowKey orderlyFixedBytesWritable;
  FixedByteArrayRowKey orderlyFixedByteArray;
  VariableLengthBytesWritableRowKey orderlyVariableBytesWritable;
  VariableLengthByteArrayRowKey orderlyVariableByteArray;

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
      val = new byte[valLen];
      rand.nextBytes(val);

      // honor limitation in simplest encoders
      for (int i = 0; i < val.length; i++) {
        if (val[i] == 0) val[i] += 1;
      }
    }

    orderedBlob = this.order == Order.ASCENDING ? OrderedBlob.ASCENDING : OrderedBlob.DESCENDING;
    orderedBlobVar =
        this.order == Order.ASCENDING ? OrderedBlobVar.ASCENDING : OrderedBlobVar.DESCENDING;
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
    orderedBlob = null;
    orderedBlobVar = null;
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
  public int orderedBlob(int reps) {
    ByteBuffer buff = this.buff;
    byte[] val = this.val;
    OrderedBlob ob = this.orderedBlob;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ob.encode(buff, val);
      dummy ^= buff.position();
    }
    return dummy;
  }

  @Benchmark
  public int orderedBlobVar(int reps) {
    ByteBuffer buff = this.buff;
    byte[] val = this.val;
    OrderedBlobVar ob = this.orderedBlobVar;
    int dummy = 0;

    for (int i = 0; i < reps; i++) {
      ob.encode(buff, val);
      dummy ^= buff.position();
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
      r.serialize(val, w);
      dummy ^= w.get()[0];
    }
    return dummy;
  }
}
