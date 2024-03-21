// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation;

import io.grpc.Metadata;

public final class Headers {

  private Headers() {}

  public static final Metadata.Key<byte[]> SIGNATURE =
      Metadata.Key.of("signature-bin", Metadata.BINARY_BYTE_MARSHALLER);

  public static final Metadata.Key<String> ALGORITHM =
      Metadata.Key.of("algorithm", Metadata.ASCII_STRING_MARSHALLER);

  public static final Metadata.Key<byte[]> FINGERPRINT =
      Metadata.Key.of("fingerprint-bin", Metadata.BINARY_BYTE_MARSHALLER);
}
