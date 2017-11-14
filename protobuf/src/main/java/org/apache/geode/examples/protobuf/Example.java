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
package org.apache.geode.examples.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.Server;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.ProtobufUtilities;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

public class Example {
  private ProtobufSerializationService serializationService = new ProtobufSerializationService();
  private final Socket socket;

  public Example(String host, int port)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException, IOException {
    final Server server = getServer(host, port);
    System.out
        .println("Connecting to host " + server.getHostname() + " on port " + server.getPort());
    socket = new Socket(server.getHostname(), server.getPort());
    shakeHands();
  }

  public static void main(String[] args) {
    try {
      Example example = new Example("127.0.0.1", 10334);
      example.insertValues(10);
      example.printValues(example.getValues());
    } catch (Exception e) {
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public Set<Integer> getValues() {
    Set<Integer> values = new HashSet<>();

    try {
      int key = 0;
      while (true) {
        ++key;
        final String value = get(key);
        if (value == null || value.isEmpty()) {
          break;
        }
        values.add(key);
      }
    } catch (Exception e) {
      // GetKeys is not currently supported via Protobuf so this exception just means that there are
      // no more keys.
      e.printStackTrace(System.err);
    } catch (Throwable t) {
      t.printStackTrace(System.err);
    }

    return values;
  }

  public void insertValues(int upperLimit) {
    IntStream.rangeClosed(1, upperLimit).forEach(i -> {
      try {
        put(i, "value" + i);
      } catch (Exception e) {
        e.printStackTrace(System.err);
      }
    });
  }

  public void printValues(Set<Integer> values) {
    values.forEach(key -> {
      try {
        System.out.println(String.format("%d:%s", key, get(key)));
      } catch (Exception e) {
        e.printStackTrace(System.err);
      }
    });
  }

  private Server getServer(String host, int port)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException, IOException {
    final Socket locatorSocket = new Socket(host, port);

    final OutputStream outputStream = locatorSocket.getOutputStream();
    // Once GEODE-4010 is fixed, this can revert to writeMagicBytes().
    writeLocatorMagicBytes(outputStream);

    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetAvailableServersRequest(LocatorAPI.GetAvailableServersRequest.newBuilder()))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = locatorSocket.getInputStream();
    return ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse()
        .getGetAvailableServersResponse().getServers(0);
  }

  private void shakeHands()
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException, IOException {

    final OutputStream outputStream = socket.getOutputStream();
    writeMagicBytes(outputStream);

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setHandshakeRequest(ConnectionAPI.HandshakeRequest.newBuilder()
                .setMajorVersion(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
                .setMinorVersion(ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE)))
        .build().writeDelimitedTo(outputStream);

    if (!ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getHandshakeResponse()
        .getHandshakePassed()) {
      System.out.println("Failed handshake.");
    }
  }

  private void writeLocatorMagicBytes(OutputStream outputStream) throws IOException {
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    writeMagicBytes(outputStream);
  }

  private void writeMagicBytes(OutputStream outputStream) throws IOException {
    outputStream.write(0x6E); // Magic byte
    outputStream.write(0x01); // Major version
  }

  private String get(Integer key)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException, IOException {
    final InputStream inputStream = socket.getInputStream();
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetRequest(RegionAPI.GetRequest.newBuilder().setRegionName("example-region").setKey(
                ProtobufUtilities.createEncodedValue(serializationService, key.toString()))))
        .build().writeDelimitedTo(outputStream);

    return ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getGetResponse()
        .getResult().getStringResult();
  }

  private void put(Integer key, String value)
      throws CodecNotRegisteredForTypeException, UnsupportedEncodingTypeException, IOException {
    final InputStream inputStream = socket.getInputStream();
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder().setRequest(ClientProtocol.Request.newBuilder()
        .setPutRequest(RegionAPI.PutRequest.newBuilder().setRegionName("example-region")
            .setEntry(ProtobufUtilities.createEntry(serializationService, key.toString(), value))))
        .build().writeDelimitedTo(outputStream);

    ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getPutResponse();
  }
}
