// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.javaapi.data;

import com.daml.ledger.api.v2.ValueOuterClass;
import java.util.Optional;

public abstract class Value {

  public static Value fromProto(ValueOuterClass.Value value) {
    switch (value.getSumCase()) {
      case RECORD:
        return DamlRecord.fromProto(value.getRecord());
      case VARIANT:
        return Variant.fromProto(value.getVariant());
      case ENUM:
        return DamlEnum.fromProto(value.getEnum());
      case CONTRACT_ID:
        return new ContractId(value.getContractId());
      case LIST:
        return DamlList.fromProto(value.getList());
      case INT64:
        return new Int64(value.getInt64());
      case NUMERIC:
        return Numeric.fromProto(value.getNumeric());
      case TEXT:
        return new Text(value.getText());
      case TIMESTAMP:
        return new Timestamp(value.getTimestamp());
      case PARTY:
        return new Party(value.getParty());
      case BOOL:
        return Bool.of(value.getBool());
      case UNIT:
        return Unit.getInstance();
      case DATE:
        return new Date(value.getDate());
      case OPTIONAL:
        return DamlOptional.fromProto(value.getOptional());
      case TEXT_MAP:
        return DamlTextMap.fromProto(value.getTextMap());
      case GEN_MAP:
        return DamlGenMap.fromProto(value.getGenMap());
      case SUM_NOT_SET:
        throw new SumNotSetException(value);
      default:
        throw new UnknownValueException(value);
    }
  }

  public final Optional<Bool> asBool() {
    return (this instanceof Bool) ? Optional.of((Bool) this) : Optional.empty();
  }

  public final Optional<DamlRecord> asRecord() {
    return (this instanceof DamlRecord) ? Optional.of((DamlRecord) this) : Optional.empty();
  }

  public final Optional<Variant> asVariant() {
    return (this instanceof Variant) ? Optional.of((Variant) this) : Optional.empty();
  }

  public final Optional<DamlEnum> asEnum() {
    return (this instanceof DamlEnum) ? Optional.of((DamlEnum) this) : Optional.empty();
  }

  public final Optional<ContractId> asContractId() {
    return (this instanceof ContractId) ? Optional.of((ContractId) this) : Optional.empty();
  }

  public final Optional<DamlList> asList() {
    return (this instanceof DamlList) ? Optional.of((DamlList) this) : Optional.empty();
  }

  public final Optional<Int64> asInt64() {
    return (this instanceof Int64) ? Optional.of((Int64) this) : Optional.empty();
  }

  public final Optional<Numeric> asNumeric() {
    return (this instanceof Numeric) ? Optional.of((Numeric) this) : Optional.empty();
  }

  public final Optional<Text> asText() {
    return (this instanceof Text) ? Optional.of((Text) this) : Optional.empty();
  }

  public final Optional<Timestamp> asTimestamp() {
    return (this instanceof Timestamp) ? Optional.of((Timestamp) this) : Optional.empty();
  }

  public final Optional<Party> asParty() {
    return (this instanceof Party) ? Optional.of((Party) this) : Optional.empty();
  }

  public final Optional<Unit> asUnit() {
    return (this instanceof Unit) ? Optional.of((Unit) this) : Optional.empty();
  }

  public final Optional<Date> asDate() {
    return (this instanceof Date) ? Optional.of((Date) this) : Optional.empty();
  }

  public final Optional<DamlOptional> asOptional() {
    return (this instanceof DamlOptional) ? Optional.of((DamlOptional) this) : Optional.empty();
  }

  public final Optional<DamlTextMap> asTextMap() {
    return (this instanceof DamlTextMap) ? Optional.of((DamlTextMap) this) : Optional.empty();
  }

  public final Optional<DamlGenMap> asGenMap() {
    return (this instanceof DamlGenMap) ? Optional.of((DamlGenMap) this) : Optional.empty();
  }

  public abstract ValueOuterClass.Value toProto();
}

class SumNotSetException extends RuntimeException {
  public SumNotSetException(ValueOuterClass.Value value) {
    super("Sum not set for value " + value.toString());
  }
}

class UnknownValueException extends RuntimeException {
  public UnknownValueException(ValueOuterClass.Value value) {
    super("value unknown " + value.toString());
  }
}

class InvalidKeyValue extends RuntimeException {
  public InvalidKeyValue(ValueOuterClass.Value value) {
    super("invalid key value, expected TEXT, found " + value.getSumCase().name());
  }
}
