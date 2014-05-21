/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.n10k;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.math.BigDecimal;
import java.util.Random;

@State(Scope.Thread)
public class DecimalEncodingBenchmark {

  static BigDecimal[] DATA;
  static {
    Random rand = new Random(
        Long.getLong(DecimalEncodingBenchmark.class.getSimpleName() + ".seed", System.currentTimeMillis()));
      int dataSize = Integer.getInteger(DecimalEncodingBenchmark.class.getSimpleName() + ".dataSize", 10);
      int maxValue = Integer.getInteger(DecimalEncodingBenchmark.class.getSimpleName() + ".maxValue", 100);
      DATA = new BigDecimal[dataSize];
      for (int i = 0; i < dataSize; i++) {
        DATA[i] = BigDecimal.valueOf(rand.nextGaussian() * maxValue);
      }
  }

  /** Used by {@link #testOrderedBytesNumeric()} */
  PositionedByteRange pbr = new SimplePositionedByteRange(12);

  @GenerateMicroBenchmark
  public void testOrderedBytesNumeric() {
    for (int i = 0; i < DATA.length; i++) {
      pbr.setPosition(0);
      OrderedBytes.encodeNumeric(pbr, DATA[i], Order.ASCENDING);
    }
  }
}
