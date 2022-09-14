package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;

public abstract class DefinedDataType<T> {
    public abstract Value toValue();
}
