// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.api.v2.ValueOuterClass;
import com.daml.ledger.javaapi.data.CreatedEvent;
import com.daml.ledger.javaapi.data.Identifier;
import com.daml.ledger.javaapi.data.PackageVersion;
import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.ContractCompanion;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.DamlEnum;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;

public class TestHelpers {

  static BigDecimal dec(String s) {
    return new BigDecimal(s);
  }

  static Instant timestampUTC(
      int year, Month month, int day, int hour, int minute, int second, int micros) {
    return LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000)
        .toInstant(ZoneOffset.UTC);
  }

  static Instant timestampUTC(
      int year, Month month, int day, int hour, int minute, int second, int micros, int nanos) {
    return LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000 + nanos)
        .toInstant(ZoneOffset.UTC);
  }

  static LocalDate date(int year, Month month, int day) {
    return LocalDate.of(year, month, day);
  }

  static class SomeRecord {
    final List<Long> i;
    final Boolean b;

    public SomeRecord(List<Long> i, Boolean b) {
      this.i = i;
      this.b = b;
    }

    public String toString() {
      return String.format("SomeRecord{i=%s,b=%s}", i, b);
    }

    @Override
    public boolean equals(Object o) {
      return o != null
          && (o instanceof SomeRecord)
          && ((SomeRecord) o).i.equals(i)
          && (((SomeRecord) o).b.equals(b));
    }

    @Override
    public int hashCode() {
      return Objects.hash(i, b);
    }
  }

  abstract static class SomeVariant {
    static class Bar extends SomeVariant {
      final Long x;

      public Bar(Long x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Bar) && x == ((Bar) o).x;
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Bar(%s)", x);
      }
    }

    static class Baz extends SomeVariant {
      final Unit x;

      public Baz(Unit x) {
        this.x = x;
      }

      // All units are the same, and thus so are all Baz's
      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Baz);
      }

      @Override
      public int hashCode() {
        return 1;
      }

      @Override
      public String toString() {
        return "Baz()";
      }
    }

    static class Quux extends SomeVariant {
      final Optional<Long> x;

      public Quux(Optional<Long> x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Quux) && x.equals(((Quux) o).x);
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Quux(%s)", x);
      }
    }

    static class Flarp extends SomeVariant {
      final List<Long> x;

      public Flarp(List<Long> x) {
        this.x = x;
      }

      @Override
      public boolean equals(Object o) {
        return o != null && (o instanceof Flarp) && x.equals(((Flarp) o).x);
      }

      @Override
      public int hashCode() {
        return Objects.hash(x);
      }

      @Override
      public String toString() {
        return String.format("Flarp(%s)", x);
      }
    }
  }

  public static class Tmpl {
    public static final class Cid extends ContractId<Tmpl> {
      public Cid(String id) {
        super(id);
      }
    }

    public static Identifier templateId =
        Identifier.fromProto(
            ValueOuterClass.Identifier.newBuilder()
                .setPackageId("#pkgname")
                .setModuleName("mod")
                .setEntityName("tmpl")
                .build());

    public static Identifier interfaceId =
        Identifier.fromProto(
            ValueOuterClass.Identifier.newBuilder()
                .setPackageId("#pkgname")
                .setModuleName("mod")
                .setEntityName("iface")
                .build());

    public static final class TmplCompanion extends ContractCompanion<Tmpl, Cid, Tmpl> {
      public TmplCompanion() {
        super(
            new ContractTypeCompanion.Package(
                "pkg", "pkgname", new PackageVersion(new int[] {1, 0, 0})),
            Tmpl.class.getSimpleName(),
            Tmpl.templateId,
            Tmpl.Cid::new,
            s -> new Tmpl(),
            js -> new Tmpl(),
            Collections.emptyList());
      }

      @Override
      public Tmpl fromCreatedEvent(CreatedEvent event) throws IllegalArgumentException {
        // Dummy
        return new Tmpl();
      }
    }

    public static final class IfaceCompanion
        extends com.daml.ledger.javaapi.data.codegen.InterfaceCompanion<String, String, String> {
      public IfaceCompanion() {
        super(
            new ContractTypeCompanion.Package(
                "pkg", "pkgname", new PackageVersion(new int[] {1, 0, 0})),
            Tmpl.class.getSimpleName(),
            Tmpl.interfaceId,
            Function.identity(),
            v -> "dummy",
            js -> "dummy",
            Collections.emptyList());
      }
    }
  }

  static enum Suit implements DamlEnum<Suit> {
    HEARTS,
    DIAMONDS,
    CLUBS,
    SPADES;

    String toDamlName() {
      switch (this) {
        case HEARTS:
          return "Hearts";
        case DIAMONDS:
          return "Diamonds";
        case CLUBS:
          return "Clubs";
        case SPADES:
          return "Spades";
        default:
          return null;
      }
    }

    static final Map<String, Suit> damlNames =
        new HashMap<>() {
          {
            put("Hearts", HEARTS);
            put("Diamonds", DIAMONDS);
            put("Clubs", CLUBS);
            put("Spades", SPADES);
          }
        };

    @Override
    public com.daml.ledger.javaapi.data.DamlEnum toValue() {
      return null;
    }

    public JsonLfEncoder jsonEncoder() {
      return null;
    }
  }
}
