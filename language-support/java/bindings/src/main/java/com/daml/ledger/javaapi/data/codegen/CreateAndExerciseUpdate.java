package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.CreateAndExerciseCommand;

public class CreateAndExerciseUpdate<R> extends Update<R> {
    private final CreateAndExerciseCommand createAndExerciseCommand;

    public CreateAndExerciseUpdate(CreateAndExerciseCommand createAndExerciseCommand) {
        this.createAndExerciseCommand = createAndExerciseCommand;
    }

    public CreateAndExerciseCommand getCreateAndExerciseUpdate() {
        return createAndExerciseCommand;
    }
}
