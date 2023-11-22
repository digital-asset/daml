// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import java.util.Optional;

public final class GetEventsByContractIdResponse {
  private final Optional<CreatedEvent> createEvent;

  private final Optional<ArchivedEvent> archiveEvent;

  public GetEventsByContractIdResponse(Optional<CreatedEvent> createEvent, Optional<ArchivedEvent> archiveEvent) {
    this.createEvent = createEvent;
    this.archiveEvent = archiveEvent;
  }

  public Optional<CreatedEvent> getCreateEvent() {
    return createEvent;
  }

  public Optional<ArchivedEvent> getArchiveEvent() {
    return archiveEvent;
  }

  public static GetEventsByContractIdResponse fromProto(
          com.daml.ledger.api.v1.EventQueryServiceOuterClass.GetEventsByContractIdResponse response) {
    if (response.hasCreateEvent()) {
      if (response.hasArchiveEvent()) {
        return new GetEventsByContractIdResponse(
                Optional.of(CreatedEvent.fromProto(response.getCreateEvent())),
                Optional.of(ArchivedEvent.fromProto(response.getArchiveEvent()))
        );
      } else {
        return new GetEventsByContractIdResponse(
                Optional.of(CreatedEvent.fromProto(response.getCreateEvent())),
                Optional.empty()
        );
      }
    } else {
      return new  GetEventsByContractIdResponse(Optional.empty(), Optional.empty());
    }
  }
}
