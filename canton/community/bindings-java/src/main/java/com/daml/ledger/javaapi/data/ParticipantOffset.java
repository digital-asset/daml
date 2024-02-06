// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ParticipantOffsetOuterClass;

import java.util.Objects;

// TODO (i15873) Eliminate V2 suffix
public abstract class ParticipantOffset {

  public static final class ParticipantBegin extends ParticipantOffset {
    static ParticipantBegin instance = new ParticipantBegin();

    private ParticipantBegin() {}

    public static ParticipantBegin getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "ParticipantOffset.Begin";
    }
  }

  public static final class ParticipantEnd extends ParticipantOffset {
    static ParticipantEnd instance = new ParticipantEnd();

    private ParticipantEnd() {}

    public static ParticipantEnd getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "ParticipantOffset.End";
    }
  }

  public static final class Absolute extends ParticipantOffset {
    private final String offset;

    public Absolute(String offset) {
      this.offset = offset;
    }

    public String getOffset() {
      return offset;
    }

    @Override
    public String toString() {
      return "ParticipantOffset.Absolute(" + offset + ')';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Absolute absolute = (Absolute) o;
      return Objects.equals(offset, absolute.offset);
    }

    @Override
    public int hashCode() {

      return Objects.hash(offset);
    }
  }

  public static ParticipantOffset fromProto(
      ParticipantOffsetOuterClass.ParticipantOffset ParticipantOffset) {
    switch (ParticipantOffset.getValueCase()) {
      case ABSOLUTE:
        return new Absolute(ParticipantOffset.getAbsolute());
      case BOUNDARY:
        switch (ParticipantOffset.getBoundary()) {
          case PARTICIPANT_BEGIN:
            return ParticipantBegin.instance;
          case PARTICIPANT_END:
            return ParticipantEnd.instance;
          case UNRECOGNIZED:
          default:
            throw new ParticipantBoundaryUnrecognized(ParticipantOffset.getBoundary());
        }
      case VALUE_NOT_SET:
      default:
        throw new ParticipantBoundaryUnset(ParticipantOffset);
    }
  }

  public final ParticipantOffsetOuterClass.ParticipantOffset toProto() {
    if (this instanceof ParticipantBegin) {
      return ParticipantOffsetOuterClass.ParticipantOffset.newBuilder()
          .setBoundary(
              ParticipantOffsetOuterClass.ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN)
          .build();
    } else if (this instanceof ParticipantEnd) {
      return ParticipantOffsetOuterClass.ParticipantOffset.newBuilder()
          .setBoundary(
              ParticipantOffsetOuterClass.ParticipantOffset.ParticipantBoundary.PARTICIPANT_END)
          .build();
    } else if (this instanceof Absolute) {
      Absolute absolute = (Absolute) this;
      return ParticipantOffsetOuterClass.ParticipantOffset.newBuilder()
          .setAbsolute(absolute.offset)
          .build();
    } else {
      throw new ParticipantOffsetUnknown(this);
    }
  }
}

class ParticipantBoundaryUnrecognized extends RuntimeException {
  public ParticipantBoundaryUnrecognized(
      ParticipantOffsetOuterClass.ParticipantOffset.ParticipantBoundary boundary) {
    super("Participant Boundary unrecognized " + boundary.toString());
  }
}

class ParticipantBoundaryUnset extends RuntimeException {
  public ParticipantBoundaryUnset(ParticipantOffsetOuterClass.ParticipantOffset offset) {
    super("Participant Offset unset " + offset.toString());
  }
}

class ParticipantOffsetUnknown extends RuntimeException {
  public ParticipantOffsetUnknown(ParticipantOffset offset) {
    super("Participant offset unknown " + offset.toString());
  }
}
