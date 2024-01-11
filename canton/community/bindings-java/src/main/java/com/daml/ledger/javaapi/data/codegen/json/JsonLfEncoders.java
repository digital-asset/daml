// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data.codegen.json;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;

import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.DamlEnum;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/// Encoders for built-in LF types. ///
public class JsonLfEncoders {
  private static final JsonStringEncoder stringEncoder = JsonStringEncoder.getInstance();

  public static JsonLfEncoder unit(Unit _unit) {
    return w -> {
      w.writeStartObject();
      w.writeEndObject();
    };
  }

  public static JsonLfEncoder bool(Boolean value) {
    return w -> w.write(String.valueOf(value));
  }

  public static JsonLfEncoder int64(Long value) {
    return w -> w.writeInt64(value);
  }

  public static JsonLfEncoder text(String value) {
    return w -> {
      String escaped = String.valueOf(stringEncoder.quoteAsString(value));
      w.write("\"" + escaped + "\"");
    };
  }

  public static JsonLfEncoder numeric(BigDecimal value) {
    return w -> w.writeNumeric(value);
  }

  public static JsonLfEncoder timestamp(Instant t) {
    return w -> {
      String subsecPat = isRoundTo(t, SECONDS) ? "" : (isRoundTo(t, MILLIS) ? ".SSS" : ".SSSSSS");
      var formatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss" + subsecPat + "X")
              .withZone(ZoneOffset.UTC);
      w.write("\"" + formatter.format(t) + "\"");
    };
  }

  public static JsonLfEncoder date(LocalDate d) {
    return text(d.toString());
  }

  public static JsonLfEncoder party(String value) {
    return text(value);
  }

  public static <Cid extends ContractId<?>> JsonLfEncoder contractId(Cid value) {
    return text(value.toValue().asContractId().get().getValue());
  }

  public static <E extends DamlEnum<E>> Function<E, JsonLfEncoder> enumeration(
      Function<E, String> toDamlName) {
    return value -> text(toDamlName.apply(value));
  }

  public static <T> Function<List<T>, JsonLfEncoder> list(Function<T, JsonLfEncoder> itemEncoder) {
    return items -> {
      return w -> {
        w.writeStartArray();
        boolean first = true;
        for (var item : items) {
          if (!first) w.writeComma();
          itemEncoder.apply(item).encode(w);
          first = false;
        }
        w.writeEndArray();
      };
    };
  }

  public static <T> Function<Map<String, T>, JsonLfEncoder> textMap(
      Function<T, JsonLfEncoder> valueEncoder) {
    return map -> {
      return w -> {
        w.writeStartObject();
        boolean first = true;
        for (var entry : map.entrySet()) {
          if (!first) w.writeComma();
          w.writeFieldName(entry.getKey());
          valueEncoder.apply(entry.getValue()).encode(w);
          first = false;
        }
        w.writeEndObject();
      };
    };
  }

  public static <K, V> Function<Map<K, V>, JsonLfEncoder> genMap(
      Function<K, JsonLfEncoder> keyEncoder, Function<V, JsonLfEncoder> valueEncoder) {
    return map -> {
      return w -> {
        w.writeStartArray();
        boolean first = true;
        for (var entry : map.entrySet()) {
          if (!first) w.writeComma();
          w.writeStartArray();
          keyEncoder.apply(entry.getKey()).encode(w);
          w.writeComma();
          valueEncoder.apply(entry.getValue()).encode(w);
          w.writeEndArray();
          first = false;
        }
        w.writeEndArray();
      };
    };
  }

  public static <T> Function<Optional<T>, JsonLfEncoder> optional(
      Function<T, JsonLfEncoder> valueEncoder) {
    return opt -> {
      return w -> {
        if (opt.isEmpty()) w.write("null");
        else {
          T value = opt.get();
          assert (!(value instanceof Optional))
              : "Using `optional` to encode a "
                  + value.getClass()
                  + " but `optionalNested` must be used for the outer encoders of nested"
                  + " Optional";
          valueEncoder.apply(value).encode(w);
        }
      };
    };
  }

  public static <T> Function<Optional<Optional<T>>, JsonLfEncoder> optionalNested(
      Function<Optional<T>, JsonLfEncoder> valueEncoder) {
    return optOpt -> {
      return w -> {
        if (optOpt.isEmpty()) w.write("null");
        else {
          var opt = optOpt.get();
          w.writeStartArray();
          if (!opt.isEmpty()) valueEncoder.apply(opt).encode(w);
          w.writeEndArray();
        }
      };
    };
  }

  public static <T> Function<T, JsonLfEncoder> variant(Function<T, Field> getField) {
    return v -> {
      return w -> {
        var field = getField.apply(v);
        assert field != null;
        w.writeStartObject();
        w.writeFieldName("tag");
        w.write("\"" + field.name + "\"");
        w.writeComma();
        w.writeFieldName("value");
        field.encoder.encode(w);
        w.writeEndObject();
      };
    };
  }

  public static class Field {
    public final String name;
    public final JsonLfEncoder encoder;

    private Field(String name, JsonLfEncoder encoder) {
      this.name = name;
      this.encoder = encoder;
    }

    public static Field of(String name, JsonLfEncoder encoder) {
      return new Field(name, encoder);
    }
  }

  public static JsonLfEncoder record(Field... fields) {
    return w -> {
      w.writeStartObject();
      boolean first = true;
      for (var field : fields) {
        if (!first) w.writeComma();
        w.writeFieldName(field.name);
        field.encoder.encode(w);
        first = false;
      }
      w.writeEndObject();
    };
  }

  // This function is used as a static import within code-gen encoder implementations.
  // It assists the type checker with type inference and unification,
  // and unifies the call syntax by accepting both method references and Function's as f
  public static <I, O> O apply(Function<I, O> f, I x) {
    return f.apply(x);
  }

  private static boolean isRoundTo(Instant value, ChronoUnit unit) {
    return value.truncatedTo(unit).equals(value);
  }
}
