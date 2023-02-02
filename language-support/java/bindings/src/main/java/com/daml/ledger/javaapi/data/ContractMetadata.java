// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

        import com.daml.ledger.api.v1.ContractMetadataOuterClass;
        import java.util.Objects;

        import com.google.protobuf.ByteString;
        import org.checkerframework.checker.nullness.qual.NonNull;

public final class ContractMetadata {

    public final com.google.protobuf.Timestamp createdAt;
    public final ByteString contractKeyHash;
    public final ByteString driverMetadata;


    public ContractMetadata(
            com.google.protobuf.@NonNull Timestamp createdAt, @NonNull ByteString contractKeyHash, @NonNull ByteString driverMetadata) {
        this.createdAt = createdAt;
        this.contractKeyHash = contractKeyHash;
        this.driverMetadata = driverMetadata;
    }

    @NonNull
    public static ContractMetadata fromProto(ContractMetadataOuterClass.ContractMetadata metadata) {
        return new ContractMetadata(metadata.getCreatedAt(), metadata.getContractKeyHash(), metadata.getDriverMetadata());
    }

    public ContractMetadataOuterClass.ContractMetadata toProto() {
        return ContractMetadataOuterClass.ContractMetadata.newBuilder()
                .setCreatedAt(this.createdAt)
                .setContractKeyHash(this.contractKeyHash)
                .setDriverMetadata(this.driverMetadata).build();
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

