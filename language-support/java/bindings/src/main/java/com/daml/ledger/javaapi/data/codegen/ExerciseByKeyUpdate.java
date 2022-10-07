package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.ExerciseByKeyCommand;

public class ExerciseByKeyUpdate<R> extends Update<R> {
    private final ExerciseByKeyCommand exerciseByKeyCommand;

    public ExerciseByKeyUpdate(ExerciseByKeyCommand exerciseByKeyCommand) {
        this.exerciseByKeyCommand = exerciseByKeyCommand;
    }

    public ExerciseByKeyCommand getExerciseByKeyCommand() {
        return exerciseByKeyCommand;
    }
}
