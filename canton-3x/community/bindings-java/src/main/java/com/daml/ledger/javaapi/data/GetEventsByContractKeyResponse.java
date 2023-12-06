// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import java.util.Optional;

public final class GetEventsByContractKeyResponse {
  private final Optional<CreatedEvent> createEvent;
  private final Optional<ArchivedEvent> archiveEvent;
  private final Optional<String> continuationToken;

  public GetEventsByContractKeyResponse(
      Optional<CreatedEvent> createEvent,
      Optional<ArchivedEvent> archiveEvent,
      Optional<String> continuationToken) {
    this.createEvent = createEvent;
    this.archiveEvent = archiveEvent;
    this.continuationToken = continuationToken;
  }

  public Optional<CreatedEvent> getCreateEvent() {
    return createEvent;
  }

  public Optional<ArchivedEvent> getArchiveEvent() {
    return archiveEvent;
  }

  public Optional<String> getContinuationToken() {
    return continuationToken;
  }

  public static GetEventsByContractKeyResponse fromProto(
      com.daml.ledger.api.v1.EventQueryServiceOuterClass.GetEventsByContractKeyResponse response) {
    return new GetEventsByContractKeyResponse(
        response.hasCreateEvent()
            ? Optional.of(CreatedEvent.fromProto(response.getCreateEvent()))
            : Optional.empty(),
        response.hasArchiveEvent()
            ? Optional.of(ArchivedEvent.fromProto(response.getArchiveEvent()))
            : Optional.empty(),
        response.getContinuationToken().isEmpty()
            ? Optional.of(response.getContinuationToken())
            : Optional.empty());
  }
}
