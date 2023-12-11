// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.EventQueryServiceOuterClass;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Objects;
import java.util.Optional;

// TODO (i15873) Eliminate V2 suffix
public final class GetEventsByContractIdResponseV2 {
  @NonNull private final Optional<Created> created;

  @NonNull private final Optional<Archived> archived;

  public GetEventsByContractIdResponseV2(
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

  public static GetEventsByContractIdResponseV2 fromProto(
      EventQueryServiceOuterClass.GetEventsByContractIdResponse response) {
    return new GetEventsByContractIdResponseV2(
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
    GetEventsByContractIdResponseV2 that = (GetEventsByContractIdResponseV2) o;
    return Objects.equals(created, that.created) && Objects.equals(archived, that.archived);
  }

  @Override
  public int hashCode() {
    return Objects.hash(created, archived);
  }

  public static final class Created {
    @NonNull private final CreatedEvent createdEvent;
    @NonNull private final String domainId;

    public Created(@NonNull CreatedEvent createdEvent, @NonNull String domainId) {
      this.createdEvent = createdEvent;
      this.domainId = domainId;
    }

    @NonNull
    public CreatedEvent getCreateEvent() {
      return createdEvent;
    }

    @NonNull
    public String getDomainId() {
      return domainId;
    }

    public static Created fromProto(EventQueryServiceOuterClass.Created created) {
      return new Created(CreatedEvent.fromProto(created.getCreatedEvent()), created.getDomainId());
    }

    public EventQueryServiceOuterClass.Created toProto() {
      return EventQueryServiceOuterClass.Created.newBuilder()
          .setCreatedEvent(createdEvent.toProto())
          .setDomainId(domainId)
          .build();
    }

    @Override
    public String toString() {
      return "Created{" + "createdEvent=" + createdEvent + ", domainId=" + domainId + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Created that = (Created) o;
      return Objects.equals(createdEvent, that.createdEvent)
          && Objects.equals(domainId, that.domainId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(createdEvent, domainId);
    }
  }

  public static final class Archived {
    @NonNull private final ArchivedEvent archivedEvent;
    @NonNull private final String domainId;

    public Archived(@NonNull ArchivedEvent archivedEvent, @NonNull String domainId) {
      this.archivedEvent = archivedEvent;
      this.domainId = domainId;
    }

    @NonNull
    public ArchivedEvent getArchivedEvent() {
      return archivedEvent;
    }

    @NonNull
    public String getDomainId() {
      return domainId;
    }

    public static Archived fromProto(EventQueryServiceOuterClass.Archived archived) {
      return new Archived(
          ArchivedEvent.fromProto(archived.getArchivedEvent()), archived.getDomainId());
    }

    public EventQueryServiceOuterClass.Archived toProto() {
      return EventQueryServiceOuterClass.Archived.newBuilder()
          .setArchivedEvent(archivedEvent.toProto())
          .setDomainId(domainId)
          .build();
    }

    @Override
    public String toString() {
      return "Archived{" + "archivedEvent=" + archivedEvent + ", domainId=" + domainId + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Archived that = (Archived) o;
      return Objects.equals(archivedEvent, that.archivedEvent)
          && Objects.equals(domainId, that.domainId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(archivedEvent, domainId);
    }
  }
}
