// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.EventQueryServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

public final class GetEventsByContractIdResponse {
  @NonNull private final Optional<Created> created;

  @NonNull private final Optional<Archived> archived;

  public GetEventsByContractIdResponse(
      @NonNull Optional<Created> created, @NonNull Optional<Archived> archived) {
    this.created = created;
    this.archived = archived;
  }

  public Optional<Created> getCreated() {
    return created;
  }

  public Optional<Archived> getArchived() {
    return archived;
  }

  public static GetEventsByContractIdResponse fromProto(
      EventQueryServiceOuterClass.GetEventsByContractIdResponse response) {
    return new GetEventsByContractIdResponse(
        response.hasCreated()
            ? Optional.of(Created.fromProto(response.getCreated()))
            : Optional.empty(),
        response.hasArchived()
            ? Optional.of(Archived.fromProto(response.getArchived()))
            : Optional.empty());
  }

  public EventQueryServiceOuterClass.GetEventsByContractIdResponse toProto() {
    var builder = EventQueryServiceOuterClass.GetEventsByContractIdResponse.newBuilder();
    created.ifPresent(created -> builder.setCreated(created.toProto()));
    archived.ifPresent(archived -> builder.setArchived(archived.toProto()));
    return builder.build();
  }

  @Override
  public String toString() {
    return "GetEventsByContractIdResponse{" + "created=" + created + ", archived=" + archived + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetEventsByContractIdResponse that = (GetEventsByContractIdResponse) o;
    return Objects.equals(created, that.created) && Objects.equals(archived, that.archived);
  }

  @Override
  public int hashCode() {
    return Objects.hash(created, archived);
  }

  public static final class Created {
    @NonNull private final CreatedEvent createdEvent;
    @NonNull private final String synchronizerId;

    public Created(@NonNull CreatedEvent createdEvent, @NonNull String synchronizerId) {
      this.createdEvent = createdEvent;
      this.synchronizerId = synchronizerId;
    }

    @NonNull
    public CreatedEvent getCreateEvent() {
      return createdEvent;
    }

    @NonNull
    public String getSynchronizerId() {
      return synchronizerId;
    }

    public static Created fromProto(EventQueryServiceOuterClass.Created created) {
      return new Created(
          CreatedEvent.fromProto(created.getCreatedEvent()), created.getSynchronizerId());
    }

    public EventQueryServiceOuterClass.Created toProto() {
      return EventQueryServiceOuterClass.Created.newBuilder()
          .setCreatedEvent(createdEvent.toProto())
          .setSynchronizerId(synchronizerId)
          .build();
    }

    @Override
    public String toString() {
      return "Created{"
          + "createdEvent="
          + createdEvent
          + ", synchronizerId='"
          + synchronizerId
          + '\''
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Created that = (Created) o;
      return Objects.equals(createdEvent, that.createdEvent)
          && Objects.equals(synchronizerId, that.synchronizerId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(createdEvent, synchronizerId);
    }
  }

  public static final class Archived {
    @NonNull private final ArchivedEvent archivedEvent;
    @NonNull private final String synchronizerId;

    public Archived(@NonNull ArchivedEvent archivedEvent, @NonNull String synchronizerId) {
      this.archivedEvent = archivedEvent;
      this.synchronizerId = synchronizerId;
    }

    @NonNull
    public ArchivedEvent getArchivedEvent() {
      return archivedEvent;
    }

    @NonNull
    public String getSynchronizerId() {
      return synchronizerId;
    }

    public static Archived fromProto(EventQueryServiceOuterClass.Archived archived) {
      return new Archived(
          ArchivedEvent.fromProto(archived.getArchivedEvent()), archived.getSynchronizerId());
    }

    public EventQueryServiceOuterClass.Archived toProto() {
      return EventQueryServiceOuterClass.Archived.newBuilder()
          .setArchivedEvent(archivedEvent.toProto())
          .setSynchronizerId(synchronizerId)
          .build();
    }

    @Override
    public String toString() {
      return "Archived{"
          + "archivedEvent="
          + archivedEvent
          + ", synchronizerId='"
          + synchronizerId
          + '\''
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Archived that = (Archived) o;
      return Objects.equals(archivedEvent, that.archivedEvent)
          && Objects.equals(synchronizerId, that.synchronizerId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(archivedEvent, synchronizerId);
    }
  }
}
