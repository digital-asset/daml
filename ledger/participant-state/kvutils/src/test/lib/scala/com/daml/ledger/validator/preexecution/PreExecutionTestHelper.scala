// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.validator.HasDamlStateValue
import com.daml.ledger.validator.reading.StateReader
import com.daml.logging.LoggingContext

import scala.collection.compat._
import scala.concurrent.{ExecutionContext, Future}

object PreExecutionTestHelper {

  final case class TestValue(value: Option[DamlStateValue])

  final case class TestReadSet(keys: Set[DamlStateKey])

  final case class TestWriteSet(value: String)

  def createLedgerStateReader(
      inputState: Map[DamlStateKey, Option[DamlStateValue]]
  ): StateReader[DamlStateKey, TestValue] = {
    val wrappedInputState = inputState.view.mapValues(TestValue(_)).toMap
    new StateReader[DamlStateKey, TestValue] {
      override def read(
          keys: Iterable[DamlStateKey]
      )(implicit
          executionContext: ExecutionContext,
          loggingContext: LoggingContext,
      ): Future[Seq[TestValue]] =
        Future.successful(keys.view.map(wrappedInputState).toVector)
    }
  }

  object TestValue {
    implicit object `TestValue has DamlStateValue` extends HasDamlStateValue[TestValue] {
      override def damlStateValue(value: TestValue): Option[DamlStateValue] = value.value
    }
  }
}
