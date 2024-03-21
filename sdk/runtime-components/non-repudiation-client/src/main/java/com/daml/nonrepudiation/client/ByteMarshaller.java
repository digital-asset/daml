// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.client;

import com.google.common.io.ByteStreams;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

final class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {

  private ByteMarshaller() {}

  public static final ByteMarshaller INSTANCE = new ByteMarshaller();

  @Override
  public byte[] parse(InputStream value) {
    try {
      return ByteStreams.toByteArray(value);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public InputStream stream(byte[] value) {
    return new ByteArrayInputStream(value);
  }
}
