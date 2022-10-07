package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseCommand;

public class ExerciseUpdate<R> extends Update<R> {
    private final ExerciseCommand exerciseCommand;

    public ExerciseUpdate(ExerciseCommand exerciseCommand) {
        this.exerciseCommand = exerciseCommand;
    }

    public ExerciseCommand getExerciseCommand() {
        return exerciseCommand;
    }
}
