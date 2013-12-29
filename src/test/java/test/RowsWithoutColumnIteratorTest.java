/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accumulo.RowsWithoutColumnIterator;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

/**
 * 
 */
public class RowsWithoutColumnIteratorTest {

  private static final String password = "password";
  private static File tempdir;
  private static MiniAccumuloCluster mac;

  @BeforeClass
  public static void setupMinicluster() throws Exception {
    tempdir = Files.createTempDir();

    MiniAccumuloConfig config = new MiniAccumuloConfig(tempdir, password);
    config.setNumTservers(1);
    mac = new MiniAccumuloCluster(config);

    mac.start();
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    mac.stop();
    FileUtils.deleteDirectory(tempdir);
  }

  @Test
  public void simpleMapTest() throws Exception {
    TreeMap<Key,Value> data = new TreeMap<Key,Value>();

    Value value = new Value(new byte[0]);
    data.put(new Key("1", "A", ""), value);
    data.put(new Key("2", "A", ""), value);
    data.put(new Key("2", "B", ""), value);
    data.put(new Key("3", "A", ""), value);
    data.put(new Key("3", "B", ""), value);
    data.put(new Key("4", "C", ""), value);

    SortedMapIterator source = new SortedMapIterator(data);

    RowsWithoutColumnIterator filter = new RowsWithoutColumnIterator();

    Map<String,String> options = ImmutableMap.of(RowsWithoutColumnIterator.COLUMNS_TO_IGNORE, "B,D");
    filter.init(source, options, new IteratorEnvironment() {

      @Override
      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public AccumuloConfiguration getConfig() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public IteratorScope getIteratorScope() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public boolean isFullMajorCompaction() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
        // TODO Auto-generated method stub

      }

    });

    filter.seek(new Range(), Collections.<ByteSequence> emptySet(), false);

    Assert.assertTrue(filter.hasTop());
    Key nextKey = filter.getTopKey();
    Value nextValue = filter.getTopValue();

    Assert.assertEquals("1", nextKey.getRow().toString());
    SortedMap<Key,Value> values = WholeRowIterator.decodeRow(nextKey, nextValue);

    filter.next();
    Assert.assertTrue(filter.hasTop());
    nextKey = filter.getTopKey();
    nextValue = filter.getTopValue();

    Assert.assertEquals("4", nextKey.getRow().toString());
    values = WholeRowIterator.decodeRow(nextKey, nextValue);

    filter.next();
    Assert.assertFalse(filter.hasTop());
  }

  @Test
  public void simpleMacTest() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector("root", new PasswordToken(password));

    final String table = "simpleMacTest";
    TableOperations tops = conn.tableOperations();
    if (tops.exists(table)) {
      tops.delete(table);
    }

    tops.create(table);

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

    Mutation m = new Mutation("1");
    m.put("A", "", "");
    bw.addMutation(m);

    m = new Mutation("2");
    m.put("A", "", "");
    m.put("B", "", "");
    bw.addMutation(m);

    m = new Mutation("3");
    m.put("A", "", "");
    m.put("B", "", "");
    bw.addMutation(m);

    m = new Mutation("4");
    m.put("C", "", "");
    bw.addMutation(m);

    bw.close();

    Scanner s = conn.createScanner(table, new Authorizations());
    Map<String,String> options = ImmutableMap.of(RowsWithoutColumnIterator.COLUMNS_TO_IGNORE, "B,D");
    IteratorSetting cfg = new IteratorSetting(50, RowsWithoutColumnIterator.class, options);
    s.addScanIterator(cfg);
    s.setRange(new Range());

    Iterator<Entry<Key,Value>> iter = s.iterator();

    Assert.assertTrue(iter.hasNext());
    Entry<Key,Value> next = iter.next();

    Assert.assertEquals("1", next.getKey().getRow().toString());
    SortedMap<Key,Value> values = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
    Assert.assertEquals(1, values.size());
    Iterator<Entry<Key,Value>> rowValues = values.entrySet().iterator();

    Entry<Key,Value> row = rowValues.next();
    Assert.assertEquals(0, new Key("1", "A", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());

    Assert.assertTrue(iter.hasNext());
    next = iter.next();

    Assert.assertEquals("4", next.getKey().getRow().toString());
    values = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
    Assert.assertEquals(1, values.size());
    rowValues = values.entrySet().iterator();

    row = rowValues.next();
    Assert.assertEquals(0, new Key("4", "C", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());

    Assert.assertFalse(iter.hasNext());
  }


  @Test
  public void simpleMacTest2() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector("root", new PasswordToken(password));

    final String table = "simpleMacTest2";
    TableOperations tops = conn.tableOperations();
    if (tops.exists(table)) {
      tops.delete(table);
    }

    tops.create(table);

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

    Mutation m = new Mutation("1");
    m.put("A", "", "");
    bw.addMutation(m);

    m = new Mutation("2");
    m.put("A", "", "");
    m.put("B", "", "");
    bw.addMutation(m);

    m = new Mutation("3");
    m.put("A", "", "");
    m.put("B", "", "");
    bw.addMutation(m);

    m = new Mutation("4");
    m.put("C", "", "");
    bw.addMutation(m);

    bw.close();

    Scanner s = conn.createScanner(table, new Authorizations());
    Map<String,String> options = ImmutableMap.of(RowsWithoutColumnIterator.COLUMNS_TO_IGNORE, "B,D");
    IteratorSetting cfg = new IteratorSetting(50, RowsWithoutColumnIterator.class, options);
    s.addScanIterator(cfg);
    s.setRange(new Range(new Key("2"), true, new Key("4"), false));

    Iterator<Entry<Key,Value>> iter = s.iterator();

    Assert.assertFalse(iter.hasNext());
  }


  @Test
  public void simpleMacTest3() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
    Connector conn = instance.getConnector("root", new PasswordToken(password));

    final String table = "simpleMacTest3";
    TableOperations tops = conn.tableOperations();
    if (tops.exists(table)) {
      tops.delete(table);
    }

    tops.create(table);

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

    Mutation m = new Mutation("1");
    m.put("A", "", "");
    bw.addMutation(m);

    m = new Mutation("2");
    m.put("A", "", "");
    m.put("B", "", "");
    bw.addMutation(m);

    m = new Mutation("2.5");
    m.put("A", "", "");
    m.put("C", "", "");
    m.put("E", "", "");
    bw.addMutation(m);

    m = new Mutation("3");
    m.put("A", "", "");
    m.put("B", "", "");
    m.put("C", "", "");
    m.put("D", "", "");
    bw.addMutation(m);

    m = new Mutation("3.5");
    m.put("A", "", "");
    m.put("C", "", "");
    m.put("D", "", "");
    bw.addMutation(m);

    m = new Mutation("4");
    m.put("C", "", "");
    bw.addMutation(m);

    bw.close();

    Scanner s = conn.createScanner(table, new Authorizations());
    Map<String,String> options = ImmutableMap.of(RowsWithoutColumnIterator.COLUMNS_TO_IGNORE, "B,D");
    IteratorSetting cfg = new IteratorSetting(50, RowsWithoutColumnIterator.class, options);
    s.addScanIterator(cfg);
    s.setRange(new Range());

    Iterator<Entry<Key,Value>> iter = s.iterator();

    // Row 1
    Assert.assertTrue(iter.hasNext());
    Entry<Key,Value> next = iter.next();

    Assert.assertEquals("1", next.getKey().getRow().toString());
    SortedMap<Key,Value> values = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
    Assert.assertEquals(1, values.size());
    Iterator<Entry<Key,Value>> rowValues = values.entrySet().iterator();

    Entry<Key,Value> row = rowValues.next();
    Assert.assertEquals(0, new Key("1", "A", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());

    // Row 2.5
    Assert.assertTrue(iter.hasNext());
    next = iter.next();

    Assert.assertEquals("2.5", next.getKey().getRow().toString());
    values = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
    Assert.assertEquals(3, values.size());
    rowValues = values.entrySet().iterator();

    row = rowValues.next();
    Assert.assertEquals(0, new Key("2.5", "A", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());
    row = rowValues.next();
    
    Assert.assertEquals(0, new Key("2.5", "C", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());
    row = rowValues.next();
    
    Assert.assertEquals(0, new Key("2.5", "E", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());

    // Row 4
    Assert.assertTrue(iter.hasNext());
    next = iter.next();

    Assert.assertEquals("4", next.getKey().getRow().toString());
    values = WholeRowIterator.decodeRow(next.getKey(), next.getValue());
    Assert.assertEquals(1, values.size());
    rowValues = values.entrySet().iterator();

    row = rowValues.next();
    Assert.assertEquals(0, new Key("4", "C", "").compareTo(row.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
    Assert.assertEquals(new Value(new byte[0]), row.getValue());

    Assert.assertFalse(iter.hasNext());
  }
}
