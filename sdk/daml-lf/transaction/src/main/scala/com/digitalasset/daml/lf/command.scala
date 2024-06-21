// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

package object command {

  @deprecated("use ApiCommand.Create", since = "2.1.0")
  type CreateCommand = ApiCommand.Create
  @deprecated("use ApiCommand.Create", since = "2.1.0")
  val CreateCommand = ApiCommand.Create

  @deprecated("use ApiCommand.Exercise", since = "2.1.0")
  type ExerciseCommand = ApiCommand.Exercise
  @deprecated("use ApiCommand.Exercise", since = "2.1.0")
  val ExerciseCommand = ApiCommand.Exercise

  @deprecated("use ApiCommand.ExerciseByKey", since = "2.1.0")
  type ExerciseByKeyCommand = ApiCommand.ExerciseByKey
  @deprecated("use ApiCommand.ExerciseKey", since = "2.1.0")
  val ExerciseByKeyCommand = ApiCommand.ExerciseByKey

  @deprecated("use ApiCommand.CreateAndExercise", since = "2.1.0")
  type CreateAndExerciseCommand = ApiCommand.CreateAndExercise
  @deprecated("use ApiCommand.ExerciseKey", since = "2.1.0")
  val CreateAndExerciseCommand = ApiCommand.CreateAndExercise

  @deprecated("use ApiCommands", since = "2.1.0")
  type Commands = ApiCommands
  @deprecated("use ApiCommands", since = "2.1.0")
  val Commands = ApiCommands

}
