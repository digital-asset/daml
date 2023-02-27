// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.ContractMetadataOuterClass;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

public final class ContractMetadata {

  // Note that we can't use a `com.daml.ledger.javaapi.data.Timestamp` here because
  // it only supports milliseconds-precision and we require lossless conversions through
  // from/toProto.
  public final Instant createdAt;
  public final ByteString driverMetadata;
  public final ByteString contractKeyHash;

  public static ContractMetadata Empty() {
    return new ContractMetadata(Instant.EPOCH, ByteString.EMPTY, ByteString.EMPTY);
  }

  public ContractMetadata(
      @NonNull Instant createdAt,
      @NonNull ByteString contractKeyHash,
      @NonNull ByteString driverMetadata) {
    this.createdAt = createdAt;
    this.contractKeyHash = contractKeyHash;
    this.driverMetadata = driverMetadata;
  }

  @NonNull
  public static ContractMetadata fromProto(ContractMetadataOuterClass.ContractMetadata metadata) {
    return new ContractMetadata(
        Instant.ofEpochSecond(
            metadata.getCreatedAt().getSeconds(), metadata.getCreatedAt().getNanos()),
        metadata.getContractKeyHash(),
        metadata.getDriverMetadata());
  }

  public ContractMetadataOuterClass.ContractMetadata toProto() {
    return ContractMetadataOuterClass.ContractMetadata.newBuilder()
        .setCreatedAt(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(this.createdAt.getEpochSecond())
                .setNanos(this.createdAt.getNano())
                .build())
        .setContractKeyHash(this.contractKeyHash)
        .setDriverMetadata(this.driverMetadata)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ContractMetadata that = (ContractMetadata) o;
    return Objects.equals(createdAt, that.createdAt)
        && Objects.equals(contractKeyHash, that.contractKeyHash)
        && Objects.equals(driverMetadata, that.driverMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdAt, contractKeyHash, driverMetadata);
  }

  @Override
  public String toString() {
    return "ContractMetadata{"
        + "createdAt='"
        + createdAt
        + '\''
        + ", contractKeyHash='"
        + contractKeyHash
        + '\''
        + ", driverMetadata='"
        + driverMetadata
        + '\''
        + '}';
  }
}
