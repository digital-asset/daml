// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.google.protobuf.ByteString;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class GetPackageResponse {

  // Clone of the PackageServiceOuterClass.HashFunction enumeration
  public enum HashFunction {
    SHA256(0),
    UNRECOGNIZED(-1),
    ;

    private final int value;

    private static Map<Integer, GetPackageResponse.HashFunction> valueToEnumMap =
        EnumSet.allOf(GetPackageResponse.HashFunction.class).stream()
            .collect(Collectors.toMap(e -> e.value, Function.identity()));

    private HashFunction(int value) {
      this.value = value;
    }

    public static GetPackageResponse.HashFunction valueOf(int value) {
      return valueToEnumMap.getOrDefault(value, UNRECOGNIZED);
    }
  }

  private final HashFunction hashFunction;
  private final String hash;
  private final ByteString archivePayload;

  public GetPackageResponse(
      HashFunction hashFunction, @NonNull String hash, @NonNull ByteString archivePayload) {
    this.hashFunction = hashFunction;
    this.hash = hash;
    this.archivePayload = archivePayload;
  }

  public HashFunction getHashFunction() {
    return hashFunction;
  }

  public String getHash() {
    return hash;
  }

  public byte[] getArchivePayload() {
    return archivePayload.toByteArray();
  }

  public static GetPackageResponse fromProto(
      com.daml.ledger.api.v1.PackageServiceOuterClass.GetPackageResponse p) {
    return new GetPackageResponse(
        HashFunction.valueOf(p.getHashFunctionValue()), p.getHash(), p.getArchivePayload());
  }
}
