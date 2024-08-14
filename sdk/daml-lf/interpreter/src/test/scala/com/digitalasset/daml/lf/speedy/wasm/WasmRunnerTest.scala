// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.bazeltools.BazelRunfiles
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.speedy.NoopAuthorizationChecker
import com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.WasmExpr
import com.digitalasset.daml.lf.transaction.TransactionVersion
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.nio.file.{Files, Paths}

class WasmRunnerTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with BeforeAndAfterAll {

  private val languages: Map[String, String] = Map("rust" -> "rs")
  private val mockLogger = mock[Logger]

  override def beforeAll(): Unit = {
    val _ = when(mockLogger.isInfoEnabled()).thenReturn(true)
  }

  "hello-world" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(s"daml-lf/interpreter/src/test/resources/hello-world.${languages("rust")}.wasm")
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val initialSeeding = InitialSeeding.NoSeed
    val wasm = new WasmRunner(
      Set.empty,
      Set.empty,
      initialSeeding,
      authorizationChecker = NoopAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
    )(LoggingContext.ForTesting)
    val wexpr = WasmExpr(wasmModule, "main")
    val result = wasm.evaluateWasmExpression(
      wexpr,
      ledgerTime = submissionTime,
      packageResolution = Map.empty,
    )

    inside(result) {
      case Right(
            UpdateMachine.Result(
              tx,
              locationInfo,
              nodeSeeds,
              globalKeyMapping,
              disclosedCreateEvents,
            )
          ) =>
        tx.nodes shouldBe empty
        tx.roots shouldBe empty
        tx.version shouldBe TransactionVersion.V31
        locationInfo shouldBe empty
        nodeSeeds shouldBe empty
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
        verify(mockLogger).info("hello-world")
    }
  }

  private def createContextualizedLogger(logger: Logger): ContextualizedLogger = {
    val method = classOf[ContextualizedLogger.type].getDeclaredMethod("createFor", classOf[Logger])
    method.setAccessible(true)
    method.invoke(ContextualizedLogger, logger).asInstanceOf[ContextualizedLogger]
  }
}
