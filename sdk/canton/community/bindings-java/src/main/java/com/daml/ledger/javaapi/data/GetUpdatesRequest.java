// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.UpdateServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class GetUpdatesRequest {

  @NonNull private final Long beginExclusive;

  @NonNull private final Optional<Long> endInclusive;

  @NonNull private final UpdateFormat updateFormat;

  // TODO(i23504) remove
  public GetUpdatesRequest(
      @NonNull Long beginExclusive,
      @NonNull Optional<Long> endInclusive,
      @NonNull TransactionFilter transactionFilter,
      boolean verbose) {
    this.beginExclusive = beginExclusive;
    this.endInclusive = endInclusive;
    EventFormat eventFormat =
        new EventFormat(
            transactionFilter.getPartyToFilters(), transactionFilter.getAnyPartyFilter(), verbose);
    Optional<TransactionFormat> transactionFormat =
        Optional.of(new TransactionFormat(eventFormat, TransactionShape.ACS_DELTA));
    Optional<Set<String>> allFilterPartiesO =
        transactionFilter.getAnyPartyFilter().isPresent()
            ?
            // a filter for the wildcard party is defined then we want the topology events for all
            // the parties (denoted by the empty set)
            Optional.of(Set.of())
            : (transactionFilter.getParties().isEmpty()
                ?
                // by-party filters are not defined, do not fetch any topology events
                Optional.empty()
                :
                // by-party filters are not defined, fetch any topology events for the parties
                // specified
                Optional.of(transactionFilter.getParties()));
    Optional<TopologyFormat> topologyFormat =
        Optional.of(
            new TopologyFormat(allFilterPartiesO.map(ParticipantAuthorizationTopologyFormat::new)));
    this.updateFormat =
        new UpdateFormat(transactionFormat, Optional.of(eventFormat), topologyFormat);
  }

  public GetUpdatesRequest(
      @NonNull Long beginExclusive,
      @NonNull Optional<Long> endInclusive,
      @NonNull UpdateFormat updateFormat) {
    this.beginExclusive = beginExclusive;
    this.endInclusive = endInclusive;
    this.updateFormat = updateFormat;
  }

  public static GetUpdatesRequest fromProto(UpdateServiceOuterClass.GetUpdatesRequest request) {
    if (request.hasUpdateFormat()) {
      if (request.hasFilter() || request.getVerbose())
        throw new IllegalArgumentException(
            "Request has both updateFormat and filter/verbose defined.");
      return new GetUpdatesRequest(
          request.getBeginExclusive(),
          request.hasEndInclusive() ? Optional.of(request.getEndInclusive()) : Optional.empty(),
          UpdateFormat.fromProto(request.getUpdateFormat()));
    } else {
      if (!request.hasFilter())
        throw new IllegalArgumentException("Request has neither updateFormat nor filter defined.");
      return new GetUpdatesRequest(
          request.getBeginExclusive(),
          request.hasEndInclusive() ? Optional.of(request.getEndInclusive()) : Optional.empty(),
          TransactionFilter.fromProto(request.getFilter()),
          request.getVerbose());
    }
  }

  public UpdateServiceOuterClass.GetUpdatesRequest toProto() {
    UpdateServiceOuterClass.GetUpdatesRequest.Builder builder =
        UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
            .setBeginExclusive(beginExclusive)
            .setUpdateFormat(this.updateFormat.toProto());

    endInclusive.ifPresent(builder::setEndInclusive);
    return builder.build();
  }

  // TODO(#23504) remove
  public UpdateServiceOuterClass.GetUpdatesRequest toProtoLegacy() {
    UpdateServiceOuterClass.GetUpdatesRequest.Builder builder =
            UpdateServiceOuterClass.GetUpdatesRequest.newBuilder()
                    .setBeginExclusive(beginExclusive)
                    .setVerbose(updateFormat.getIncludeTransactions().map(TransactionFormat::getEventFormat).map(EventFormat::getVerbose).orElse(false));

    updateFormat.getIncludeTransactions().map(TransactionFormat::getEventFormat).ifPresent(t-> builder.setFilter(new TransactionFilter(t.getPartyToFilters(), t.getAnyPartyFilter()).toProto()));

    endInclusive.ifPresent(builder::setEndInclusive);

    return builder.build();
  }

  @NonNull
  public Long getBeginExclusive() {
    return beginExclusive;
  }

  @NonNull
  public Optional<Long> getEndInclusive() {
    return endInclusive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetUpdatesRequest that = (GetUpdatesRequest) o;
    return Objects.equals(beginExclusive, that.beginExclusive)
        && Objects.equals(endInclusive, that.endInclusive)
        && Objects.equals(updateFormat, that.updateFormat);
  }

  @Override
  public int hashCode() {

    return Objects.hash(beginExclusive, endInclusive, updateFormat);
  }

  @Override
  public String toString() {
    return "GetUpdatesRequest{"
        + "beginExclusive="
        + beginExclusive
        + ", endInclusive="
        + endInclusive
        + ", updateFormat="
        + updateFormat
        + '}';
  }
}
