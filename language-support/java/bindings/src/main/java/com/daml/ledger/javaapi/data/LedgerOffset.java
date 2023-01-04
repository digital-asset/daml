// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v1.LedgerOffsetOuterClass;
import java.util.Objects;

public abstract class LedgerOffset {

  public static final class LedgerBegin extends LedgerOffset {
    static LedgerBegin instance = new LedgerBegin();

    private LedgerBegin() {}

    public static LedgerBegin getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "LedgerOffset.Begin";
    }
  }

  public static final class LedgerEnd extends LedgerOffset {
    static LedgerEnd instance = new LedgerEnd();

    private LedgerEnd() {}

    public static LedgerEnd getInstance() {
      return instance;
    }

    @Override
    public String toString() {
      return "LedgerOffset.End";
    }
  }

  public static final class Absolute extends LedgerOffset {
    private final String offset;

    public Absolute(String offset) {
      this.offset = offset;
    }

    public String getOffset() {
      return offset;
    }

    @Override
    public String toString() {
      return "LedgerOffset.Absolute(" + offset + ')';
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

  public static LedgerOffset fromProto(LedgerOffsetOuterClass.LedgerOffset ledgerOffset) {
    switch (ledgerOffset.getValueCase()) {
      case ABSOLUTE:
        return new Absolute(ledgerOffset.getAbsolute());
      case BOUNDARY:
        switch (ledgerOffset.getBoundary()) {
          case LEDGER_BEGIN:
            return LedgerBegin.instance;
          case LEDGER_END:
            return LedgerEnd.instance;
          case UNRECOGNIZED:
          default:
            throw new LedgerBoundaryUnrecognized(ledgerOffset.getBoundary());
        }
      case VALUE_NOT_SET:
      default:
        throw new LedgerBoundaryUnset(ledgerOffset);
    }
  }

  public final LedgerOffsetOuterClass.LedgerOffset toProto() {
    if (this instanceof LedgerBegin) {
      return LedgerOffsetOuterClass.LedgerOffset.newBuilder()
          .setBoundary(LedgerOffsetOuterClass.LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
          .build();
    } else if (this instanceof LedgerEnd) {
      return LedgerOffsetOuterClass.LedgerOffset.newBuilder()
          .setBoundary(LedgerOffsetOuterClass.LedgerOffset.LedgerBoundary.LEDGER_END)
          .build();
    } else if (this instanceof Absolute) {
      Absolute absolute = (Absolute) this;
      return LedgerOffsetOuterClass.LedgerOffset.newBuilder().setAbsolute(absolute.offset).build();
    } else {
      throw new LedgerOffsetUnknown(this);
    }
  }
}

class LedgerBoundaryUnrecognized extends RuntimeException {
  public LedgerBoundaryUnrecognized(LedgerOffsetOuterClass.LedgerOffset.LedgerBoundary boundary) {
    super("Ledger Boundary unknown " + boundary.toString());
  }
}

class LedgerBoundaryUnset extends RuntimeException {
  public LedgerBoundaryUnset(LedgerOffsetOuterClass.LedgerOffset offset) {
    super("Ledger Offset unset " + offset.toString());
  }
}

class LedgerOffsetUnknown extends RuntimeException {
  public LedgerOffsetUnknown(LedgerOffset offset) {
    super("Ledger offset unkwnown " + offset.toString());
  }
}
