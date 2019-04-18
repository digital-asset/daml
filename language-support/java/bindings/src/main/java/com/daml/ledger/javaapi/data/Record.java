// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Objects;

public final class Record extends Value {

    private final Optional<Identifier> recordId;

    private final Map<String, Value> fieldsMap;

    private final List<Field> fields;

    public Record(Identifier recordId, Field... fields) {
        this(recordId, Arrays.asList(fields));
    }

    public Record(Field... fields) {
        this(Arrays.asList(fields));
    }

    public Record(Identifier recordId, List<Field> fields) {
        this(Optional.of(recordId), fields, fieldsListToHashMap(fields));
    }

    public Record(List<Field> fields) {
        this(Optional.empty(), fields, fieldsListToHashMap(fields));
    }

    /**
     * @since 2.2.0
     */
    public Record(Optional<Identifier> recordId, List<Field> fields, Map<String, Value> fieldsMap) {
        this.recordId = recordId;
        this.fields = fields;
        this.fieldsMap = fieldsMap;
    }

    private static Map<String, Value> fieldsListToHashMap(List<Field> fields) {
        if (fields.isEmpty() || !fields.get(0).getLabel().isPresent()) {
            return Collections.emptyMap();
        } else {
            HashMap<String, Value> fieldsMap = new HashMap<>(fields.size());
            for (Field field : fields) {
                fieldsMap.put(field.getLabel().get(), field.getValue());
            }
            return fieldsMap;
        }
    }

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

    @Override
    public ValueOuterClass.Value toProto() {
        return ValueOuterClass.Value.newBuilder().setRecord(this.toProtoRecord()).build();
    }

    public ValueOuterClass.Record toProtoRecord() {
        ValueOuterClass.Record.Builder recordBuilder = ValueOuterClass.Record.newBuilder();
        this.recordId.ifPresent(recordId -> recordBuilder.setRecordId(recordId.toProto()));
        for (Field field : this.fields) {
            recordBuilder.addFields(field.toProto());
        }
        return recordBuilder.build();
    }

    public Optional<Identifier> getRecordId() {
        return recordId;
    }

    public List<Field> getFields() {
        return fields;
    }

    /**
     * @return the Map of this Record fields containing the records that have the label
     * @since 2.2.0
     */
    public Map<String, Value> getFieldsMap() {
        return fieldsMap;
    }

    @Override
    public String toString() {
        return "Record{" +
                "recordId=" + recordId +
                ", fields=" + fields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(recordId, record.recordId) &&
                Objects.equals(fields, record.fields);
    }

    @Override
    public int hashCode() {

        return Objects.hash(recordId, fields);
    }

    public static final class Field {

        private final Optional<String> label;

        private final Value value;

        public Field(String label, Value value) {
            this.label = Optional.of(label);
            this.value = value;
        }

        public Field(Value value) {
            this.label = Optional.empty();
            this.value = value;
        }

            public Optional<String> getLabel() {
            return label;
        }

            public Value getValue() {
            return value;
        }

        public static Field fromProto(ValueOuterClass.RecordField field) {
            String label = field.getLabel();
            Value value = Value.fromProto(field.getValue());
            return label.isEmpty() ? new Field(value) : new Field(label, value);
        }

        public ValueOuterClass.RecordField toProto() {
            ValueOuterClass.RecordField.Builder builder = ValueOuterClass.RecordField.newBuilder();
            this.label.ifPresent(builder::setLabel);
            builder.setValue(this.value.toProto());
            return builder.build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return Objects.equals(label, field.label) &&
                    Objects.equals(value, field.value);
        }

        @Override
        public int hashCode() {

            return Objects.hash(label, value);
        }

        @Override
        public String toString() {
            return "Field{" +
                    "label=" + label +
                    ", value=" + value +
                    '}';
        }
    }
}
