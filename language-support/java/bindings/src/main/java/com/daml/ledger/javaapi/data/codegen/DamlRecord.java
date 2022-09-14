package com.daml.ledger.javaapi.data.codegen;

public abstract class DamlRecord<T> extends DefinedDataType<T> {
    public abstract com.daml.ledger.javaapi.data.DamlRecord toValue();
}

