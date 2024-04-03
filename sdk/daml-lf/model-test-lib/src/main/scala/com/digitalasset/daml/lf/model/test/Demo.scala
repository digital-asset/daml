// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package model
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref
import com.daml.lf.engine.script.v2.ledgerinteraction.IdeLedgerClient
import com.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.SubmitFailure
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.model.test.Ledgers.Ledger
import com.daml.lf.speedy.{Compiler, RingBufferTraceLog, WarningLog}
import com.daml.logging.ContextualizedLogger
import com.digitalasset.canton.logging.NamedLoggerFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.scalacheck.Gen

import java.io.File
import scala.concurrent.{Await, ExecutionContext}

object Demo {

  private val universalTemplateDar =
    UniversalArchiveDecoder
      .assertReadFile(new File(rlocation("daml-lf/model-test-lib/universal.dar")))

  private val universalTemplatePkgId: Ref.PackageId =
    universalTemplateDar.main._1

  private val compiledPkgs =
    PureCompiledPackages.assertBuild(
      universalTemplateDar.all.toMap,
      Compiler.Config.Default(LanguageMajorVersion.V2),
    )

  private val ledgerClient = new IdeLedgerClient(
    originalCompiledPackages = compiledPkgs,
    traceLog = new RingBufferTraceLog(ContextualizedLogger.createFor("model.test.trace"), 1000),
    warningLog = new WarningLog(ContextualizedLogger.createFor("model.test.warnings")),
    canceled = () => false,
    namedLoggerFactory = NamedLoggerFactory("model.test", "model-based testing profile"),
  )

  private val interpreter = new Interpreter(universalTemplatePkgId, ledgerClient)

  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("RunnerMain")
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val materializer: Materializer = Materializer(system)

    while (true) {
      Gen
        .resize(5, new Generators(3).ledgerGen)
        .sample
        .foreach(execute)
    }

    val _ = Await.ready(system.terminate(), scala.concurrent.duration.Duration.Inf)
  }

  private def execute(l: Ledger)(implicit
      ec: ExecutionContext,
      mat: Materializer,
  ): Unit = {
    println("==== ledger ====")
    println(Pretty.prettyLedger(l))
    println("==== result ====")
    try {
      val result = Await.result(interpreter.runLedger(l), scala.concurrent.duration.Duration.Inf)
      result match {
        case Right(value) => println(s"success=$value")
        case Left(SubmitFailure(statusError, _)) =>
          println("ledger error")
          statusError match {
            case e: com.daml.lf.scenario.Error =>
              println(com.daml.lf.scenario.Pretty.prettyError(e).render(80))
            case _ => println(statusError)
          }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace(System.out)
    }
  }
}
