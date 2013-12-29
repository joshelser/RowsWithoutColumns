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
package accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import com.google.common.base.Splitter;

/**
 * 
 */
public class RowsWithoutColumnIterator extends WrappingIterator {
  public static final String COLUMNS_TO_IGNORE = "columns.to.ignore";
  
  protected LinkedList<Key> keyBuffer;
  protected LinkedList<Value> valueBuffer;
  protected HashSet<Text> columnsToIgnore;
  protected Range seekedRange;
  protected Collection<ByteSequence> seekedColumnFamilies;
  protected boolean seekedColumnFamiliesInclusive;
  
  private final Text rowHolder = new Text();
  private final Text colfamAcceptHolder = new Text();
  
  private Key topKey;
  private Value topValue;
  
  public RowsWithoutColumnIterator() {
    keyBuffer = new LinkedList<Key>();
    valueBuffer = new LinkedList<Value>();
    columnsToIgnore = new HashSet<Text>();
  }
  
  public RowsWithoutColumnIterator(RowsWithoutColumnIterator other, IteratorEnvironment env) {
    // Copy the buffer
    this.keyBuffer = new LinkedList<Key>(other.keyBuffer);
    this.valueBuffer = new LinkedList<Value>(other.valueBuffer);
    
    this.columnsToIgnore = new HashSet<Text>(other.columnsToIgnore);
    this.seekedRange = new Range(other.seekedRange);
    this.seekedColumnFamilies = other.seekedColumnFamilies;
    this.seekedColumnFamiliesInclusive = other.seekedColumnFamiliesInclusive;
    
    // Copy the source
    this.setSource(other.getSource().deepCopy(env));
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    RowsWithoutColumnIterator copy = new RowsWithoutColumnIterator(this, env);
    
    return copy;
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    
    // Get the columns whose rows that contain them we don't want to return
    if (options.containsKey(COLUMNS_TO_IGNORE)) {
      String columnsToIgnoreValue = options.get(COLUMNS_TO_IGNORE);
      
      Iterable<String> splitColumns = Splitter.on(',').split(columnsToIgnoreValue);
      for (String splitColumn : splitColumns) {
        columnsToIgnore.add(new Text(splitColumn));
      }
    }
  }
  
  @Override
  public Key getTopKey() {
    return this.topKey;
  }
  
  @Override
  public Value getTopValue() {
    return this.topValue;
  }
  
  @Override
  public boolean hasTop() {
    return null != this.topKey;
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
    
    seekedRange = range;
    seekedColumnFamilies = columnFamilies;
    seekedColumnFamiliesInclusive = inclusive;

    update();
  }
  
  @Override
  public void next() throws IOException {
    clearBuffers();

    update();
  }
  
  protected void update() throws IOException {
    // If we have data to read, try to find a matching row
    if (getSource().hasTop()) {
      // We found a matching row
      if (bufferRow()) {
        // Set the topKey to bet just the row
        keyBuffer.getFirst().getRow(rowHolder);
        this.topKey = new Key(rowHolder);
        
        this.topValue = WholeRowIterator.encodeRow(keyBuffer, valueBuffer);
      } else {
        this.topKey = null;
        this.topValue = null;
      }
    } else {
      this.topKey = null;
      this.topValue = null;
    }
  }
  
  /**
   * Copy the next valid row into {@link buffer}.
   * @return True if we found a valid row, otherwise false (we're at the end)
   */
  protected boolean bufferRow() throws IOException {
    while (getSource().hasTop()) {
      Key key = getSource().getTopKey();
      Value value = getSource().getTopValue();
      
      // New row
      if (keyBuffer.isEmpty()) {
        // add it or skip to the next row and restart
        aggregate(key, value);
      } else {
        Key lastKey = keyBuffer.getLast();
        
        // Still aggregating the last row
        if (0 == lastKey.compareTo(key, PartialKey.ROW)) {
          // add it or skip to the next row and restart
          aggregate(key, value);
        } else {
          // We found a row that meets the column families criteria
          if (!keyBuffer.isEmpty()) {
            return true;
          }

          clearBuffers();
          
          getSource().next();
        }
      }
    }
    
    return !keyBuffer.isEmpty();
  }
  
  protected void clearBuffers() {
    // Clear the list if it's under a given size, otherwise just make a new object
    // and let the JVM GC clean up the mess so we don't waste a bunch of time
    // iterating over the list just to clear it.
    if (keyBuffer.size() < 10) {
      keyBuffer.clear();
      valueBuffer.clear();
    } else {
      keyBuffer = new LinkedList<Key>();
      valueBuffer = new LinkedList<Value>();
    }
  }
  
  /**
   * Add this key value pair to the buffer if it meets the criteria for acceptance. This method 
   * will also set the underlying source to the next position that needs to be read.
   * @param key
   * @param value
   * @throws IOException
   */
  protected void aggregate(Key key, Value value) throws IOException {
    if (accept(key, value)) {
      keyBuffer.add(key);
      valueBuffer.add(value);
      
      // Move to the next entry
      getSource().next();
    } else {
      clearBuffers();
      
      Key nextKey = key.followingKey(PartialKey.ROW);
      
      if (seekedRange.contains(nextKey)) {
        Range newRange = new Range(nextKey, true, seekedRange.getEndKey(), seekedRange.isEndKeyInclusive());
        
        getSource().seek(newRange, this.seekedColumnFamilies, this.seekedColumnFamiliesInclusive);
      }
    }
  }
  
  /**
   * Determines if the given Key contains a column family which should be accepted
   * @param key
   * @param value
   * @return
   */
  protected boolean accept(Key key, Value value) {
    key.getColumnFamily(colfamAcceptHolder);
    if (columnsToIgnore.contains(colfamAcceptHolder)) {
      return false;
    }
    
    return true;
  }
}
