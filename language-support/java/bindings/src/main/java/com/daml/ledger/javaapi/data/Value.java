// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data;

import com.digitalasset.ledger.api.v1.ValueOuterClass;

import java.util.HashMap;
import java.util.Optional;

public abstract class Value {

    public static Value fromProto(ValueOuterClass.Value value) {
        switch (value.getSumCase()) {
            case RECORD:
                return Record.fromProto(value.getRecord());
            case VARIANT:
                return Variant.fromProto(value.getVariant());
            case CONTRACT_ID:
                return new ContractId(value.getContractId());
            case LIST:
                return DamlList.fromProto(value.getList());
            case INT64:
                return new Int64(value.getInt64());
            case DECIMAL:
                return Decimal.fromProto(value.getDecimal());
            case TEXT:
                return new Text(value.getText());
            case TIMESTAMP:
                return new Timestamp(value.getTimestamp());
            case PARTY:
                return new Party(value.getParty());
            case BOOL:
                return new Bool(value.getBool());
            case UNIT:
                return Unit.getInstance();
            case DATE:
                return new Date(value.getDate());
            case OPTIONAL:
                if (value.getOptional().hasValue()) {
                    Value inner = fromProto(value.getOptional().getValue());
                    return DamlOptional.of(inner);
                }
                else {
                    return DamlOptional.empty();
                }
            case MAP:
                HashMap<String, Value> map = new HashMap<String, Value>();
                for(ValueOuterClass.Map.Entry e: value.getMap().getEntriesList()){
                    map.put(e.getKey(), fromProto(e.getValue()));
                }
                return new DamlMap(map);
            case SUM_NOT_SET:
                throw new SumNotSetException(value);
            default:
                throw new UnknownValueException(value);
        }
    }

    public final Optional<Bool> asBool() {
        return (this instanceof Bool) ? Optional.of((Bool) this) : Optional.empty();
    }

    public final Optional<Record> asRecord() {
        return (this instanceof Record) ? Optional.of((Record) this) : Optional.empty();
    }

    public final Optional<Variant> asVariant() {
        return (this instanceof Variant) ? Optional.of((Variant) this) : Optional.empty();
    }

    public final Optional<DamlEnum> asEnum() {
        return (this instanceof DamlEnum) ? Optional.of((DamlEnum) this): Optional.empty();
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

    public final Optional<Decimal> asDecimal() {
        return (this instanceof Decimal) ? Optional.of((Decimal) this) : Optional.empty();
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
        return (this instanceof DamlOptional) ?
                Optional.of((DamlOptional) this) :
                Optional.empty();
    }

    public final Optional<DamlMap> asMap() {
        return (this instanceof DamlMap) ? Optional.of((DamlMap) this) : Optional.empty();
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

class InvalidKeyValue extends RuntimeException{
    public InvalidKeyValue(ValueOuterClass.Value value) {
        super("invalid key value, expected TEXT, found " + value.getSumCase().name());
    }
}
