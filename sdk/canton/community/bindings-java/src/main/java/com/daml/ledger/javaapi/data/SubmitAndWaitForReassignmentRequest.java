// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.CommandServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class SubmitAndWaitForReassignmentRequest {

  @NonNull private final ReassignmentCommands reassignmentCommands;

  @NonNull private final Optional<EventFormat> eventFormat;

  public SubmitAndWaitForReassignmentRequest(
      @NonNull ReassignmentCommands reassignmentCommands,
      @NonNull Optional<EventFormat> eventFormat) {
    this.reassignmentCommands = reassignmentCommands;
    this.eventFormat = eventFormat;
  }

  @NonNull
  public ReassignmentCommands getReassignmentCommands() {
    return reassignmentCommands;
  }

  @NonNull
  public Optional<EventFormat> getEventFormat() {
    return eventFormat;
  }

  public CommandServiceOuterClass.SubmitAndWaitForReassignmentRequest toProto() {
    CommandServiceOuterClass.SubmitAndWaitForReassignmentRequest.Builder builder =
        CommandServiceOuterClass.SubmitAndWaitForReassignmentRequest.newBuilder()
            .setReassignmentCommands(reassignmentCommands.toProto());

    eventFormat.ifPresent(eventFormat -> builder.setEventFormat(eventFormat.toProto()));

    return builder.build();
  }

  public static SubmitAndWaitForReassignmentRequest fromProto(
      CommandServiceOuterClass.SubmitAndWaitForReassignmentRequest request) {
    Optional<EventFormat> eventFormat =
        request.hasEventFormat()
            ? Optional.of(EventFormat.fromProto(request.getEventFormat()))
            : Optional.empty();

    return new SubmitAndWaitForReassignmentRequest(
        ReassignmentCommands.fromProto(request.getReassignmentCommands()), eventFormat);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SubmitAndWaitForReassignmentRequest that = (SubmitAndWaitForReassignmentRequest) o;
    return Objects.equals(reassignmentCommands, that.reassignmentCommands)
        && Objects.equals(eventFormat, that.eventFormat);
  }

  @Override
  public String toString() {
    return "SubmitAndWaitForReassignmentRequest{"
        + "reassignmentCommands="
        + reassignmentCommands
        + ", eventFormat="
        + eventFormat
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(reassignmentCommands, eventFormat);
  }
}
