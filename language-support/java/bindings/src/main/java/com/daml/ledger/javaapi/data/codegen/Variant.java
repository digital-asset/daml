package com.daml.ledger.javaapi.data.codegen;

public abstract class Variant<T> extends DefinedDataType<T> {
    public abstract com.daml.ledger.javaapi.data.Variant toValue();
}
