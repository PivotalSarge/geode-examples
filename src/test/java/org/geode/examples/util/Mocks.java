/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.geode.examples.util;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.ResultCollector;

import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;

public class Mocks {
  private Mocks() {
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Region<K, V> region(String name) throws Exception {
    return region(name, null, null);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Region<K, V> region(String name, CacheWriter cacheWriter,
                                           List<CacheListener> cacheListeners) throws Exception {
    Map<K, V> data = new HashMap<>();
    Region<K, V> region = mock(Region.class);

    when(region.getName()).thenReturn(name);
    when(region.put(any(), any())).then(inv -> {
      final K key = getKey(inv);
      final V oldValue = data.get(key);
      final V newValue = getValue(inv);

      if (!data.containsKey(key)) {
        final TestEntryEvent<K, V>
            entryEvent =
            new TestEntryEvent<K, V>(region, Operation.CREATE, key, oldValue, newValue);
        if (cacheWriter != null) {
          try {
            cacheWriter.beforeCreate(entryEvent);
          } catch (CacheWriterException e) {
            return oldValue;
          }
        }

        if (cacheListeners != null) {
          for (CacheListener cacheListener : cacheListeners) {
            cacheListener.afterCreate(entryEvent);
          }
        }
      } else {
        final TestEntryEvent<K, V>
            entryEvent =
            new TestEntryEvent<K, V>(region, Operation.UPDATE, key, oldValue, newValue);
        if (cacheWriter != null) {
          try {
            cacheWriter.beforeUpdate(entryEvent);
          } catch (CacheWriterException e) {
            return oldValue;
          }
        }

        if (cacheListeners != null) {
          for (CacheListener cacheListener : cacheListeners) {
            cacheListener.afterUpdate(entryEvent);
          }
        }
      }

      data.put(getKey(inv), getValue(inv));
      return oldValue;
    });
    when(region.get(any())).then(inv -> data.get(getKey(inv)));
    when(region.keySet()).thenReturn(data.keySet());
    when(region.values()).thenReturn(data.values());
    when(region.size()).thenReturn(data.size());
    when(region.keySetOnServer()).thenReturn(data.keySet());
    when(region.containsKey(any())).then(inv -> data.containsKey(getKey(inv)));
    when(region.containsKeyOnServer(any())).then(inv -> data.containsKey(getKey(inv)));

    doAnswer(inv -> {
      data.putAll((Map<? extends K, ? extends V>) inv.getArguments()[0]);
      return inv.getArguments();
    }).when(region).putAll(any());

    return region;
  }

  @SuppressWarnings("unchecked")
  public static Execution execution(String functionId, Object result) throws Exception {
    ResultCollector resultCollector = mock(ResultCollector.class);
    when(resultCollector.getResult()).thenReturn(result);

    Execution execution = mock(Execution.class);
    when(execution.execute(functionId)).thenReturn(resultCollector);

    return execution;
  }

  @SuppressWarnings("unchecked")
  private static <K> K getKey(InvocationOnMock inv) {
    return (K) inv.getArguments()[0];
  }

  @SuppressWarnings("unchecked")
  private static <V> V getValue(InvocationOnMock inv) {
    return (V) inv.getArguments()[1];
  }
}
