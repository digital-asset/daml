// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.Optional;

public final class GetEventsByContractKeyResponse {
  private final Optional<CreatedEvent> createEvent;
  private final Optional<ArchivedEvent> archiveEvent;
  private final Optional<String> continuationToken;

  public GetEventsByContractKeyResponse(Optional<CreatedEvent> createEvent, Optional<ArchivedEvent> archiveEvent, Optional<String> continuationToken) {
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
    Optional<String> optContinuationToken = Optional.of(response.getContinuationToken()).filter(c -> !c.equals(""));
    if (response.hasCreateEvent()) {
      if (response.hasArchiveEvent()) {
        return new GetEventsByContractKeyResponse(
                Optional.of(CreatedEvent.fromProto(response.getCreateEvent())),
                Optional.of(ArchivedEvent.fromProto(response.getArchiveEvent())),
                optContinuationToken
        );
      } else {
        return new GetEventsByContractKeyResponse(
                Optional.of(CreatedEvent.fromProto(response.getCreateEvent())),
                Optional.empty(),
                optContinuationToken
        );
      }
    } else {
      return new  GetEventsByContractKeyResponse(Optional.empty(), Optional.empty(), optContinuationToken);
    }
  }
}
