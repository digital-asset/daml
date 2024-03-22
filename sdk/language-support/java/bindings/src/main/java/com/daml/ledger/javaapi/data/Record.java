// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import static java.util.Collections.unmodifiableList;

import com.daml.ledger.api.v1.ValueOuterClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

// FIXME When removing this after the deprecation period is over, make DamlRecord final
/** @deprecated Use {@link DamlRecord} instead. */
@Deprecated
public final class Record extends DamlRecord {

  public Record(@NonNull Identifier recordId, @NonNull Field... fields) {
    this(recordId, Arrays.asList(fields));
  }

  public Record(@NonNull Field... fields) {
    super(Arrays.asList(fields));
  }

  public Record(@NonNull Identifier recordId, @NonNull List<@NonNull Field> fields) {
    super(recordId, unmodifiableList(fields));
  }

  public Record(@NonNull List<@NonNull Field> fields) {
    super(unmodifiableList(fields));
  }

  public Record(
      @NonNull Optional<Identifier> recordId,
      @NonNull List<@NonNull Field> fields,
      Map<String, Value> fieldsMap) {
    super(recordId, unmodifiableList(fields), fieldsMap);
  }

  /** @deprecated Use {@link DamlRecord#fromProto(ValueOuterClass.Record)} instead */
  @Deprecated
  @NonNull
  public static Record fromProto(ValueOuterClass.Record record) {
    ArrayList<Field> fields = new ArrayList<>(record.getFieldsCount());
    HashMap<String, Value> fieldsMap = new HashMap<>(record.getFieldsCount());
    for (ValueOuterClass.RecordField recordField : record.getFieldsList()) {
      Field field = Field.fromProto(recordField);
      fields.add(field);
      if (field.getLabel().isPresent()) {
        fieldsMap.put(field.getLabel().get(), field.getValue());
      }
    }
    if (record.hasRecordId()) {
      Identifier recordId = Identifier.fromProto(record.getRecordId());
      return new Record(Optional.of(recordId), fields, fieldsMap);
    } else {
      return new Record(Optional.empty(), fields, fieldsMap);
    }
  }

  // FIXME When removing this after the deprecation period is over, make DamlTextMap.Field final
  /** @deprecated Use {@link DamlRecord.Field} instead. */
  @Deprecated
  public static final class Field extends DamlRecord.Field {

    public Field(@NonNull String label, @NonNull Value value) {
      super(label, value);
    }

    public Field(@NonNull Value value) {
      super(value);
    }

    /** @deprecated Use {@link DamlRecord.Field#fromProto(ValueOuterClass.Record)} instead */
    @Deprecated
    public static Field fromProto(ValueOuterClass.RecordField field) {
      String label = field.getLabel();
      Value value = Value.fromProto(field.getValue());
      return label.isEmpty() ? new Field(value) : new Field(label, value);
    }
  }
}
