package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreateCommand;

public class CreateUpdate<R> extends Update<R> {
    private final CreateCommand createCommand;

    public CreateUpdate(CreateCommand createCommand) {
        this.createCommand = createCommand;
    }

    public CreateCommand getCreateCommand() {
        return createCommand;
    }
}
