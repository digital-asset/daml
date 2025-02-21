// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.discard.Implicits.*

/** To make the [[ConsoleEnvironment]] functionality conveniently available in ammonite we stash it
  * in a implicit variable included as a predef before any script or REPL commands are run.
  */
class ConsoleEnvironmentBinding {

  protected def consoleMacrosImport: String =
    "import com.digitalasset.canton.console.ConsoleMacros._"

  /** The predef code itself which is executed before any script or repl command */
  def predefCode(interactive: Boolean, noTty: Boolean = false): String = {
    val consoleEnvClassName = objectClassNameWithoutSuffix(ConsoleEnvironment.Implicits.getClass)

    def addToBuilder(builder: StringBuilder, s: String): Unit = {
      builder ++= System.lineSeparator()
      (builder ++= s).discard
    }

    // this is the magic which allows us to use extensions such as `all start` on a sequence of instance references
    // and those extensions to still obtain an implicit reference to the [[ConsoleEnvironment]] instance (where state like packages is kept)
    val builder = new StringBuilder(s"""
       |interp.configureCompiler(_.settings.processArgumentString("-Xsource:2.13"))
       |import $consoleEnvClassName._
       |import com.digitalasset.canton.crypto._
       |import com.digitalasset.canton.config._
       |import com.digitalasset.canton.admin.api.client.data._
       |import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping
       |$consoleMacrosImport
       |import ${classOf[com.digitalasset.canton.console.BootstrapScriptException].getName}
       |import com.digitalasset.canton.config.RequireTypes._
       |import com.digitalasset.canton.participant.admin.ResourceLimits
       |import ch.qos.logback.classic.Level
       |implicit val consoleEnvironment = ${ConsoleEnvironmentBinding.BindingName}
       |def help = consoleEnvironment.help
       |def help(s: String) = consoleEnvironment.help(s)
       |def health = consoleEnvironment.health
       |def logger = consoleEnvironment.consoleLogger
     """.stripMargin)

    val importsSynchronizer =
      """
        |import com.digitalasset.canton.version.ProtocolVersion
        |import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
        |import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
        |import com.digitalasset.canton.console.commands.SynchronizerChoice
        |""".stripMargin

    val importStandard =
      """
         |import java.time.Instant
         |import scala.concurrent.ExecutionContextExecutor
         |implicit val ec: ExecutionContextExecutor = consoleEnvironment.environment.executionContext
         |import scala.concurrent.duration._
         |import scala.language.postfixOps
         |import scala.jdk.CollectionConverters._
         |""".stripMargin

    val importsTopology =
      """
         |import com.digitalasset.canton.topology.store.TimeQuery
         |import com.digitalasset.canton.topology.{store => _, _}
         |import com.digitalasset.canton.topology.transaction._
         |import com.digitalasset.canton.topology.admin.grpc._
         |import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Authorized
         |""".stripMargin

    val importsSequencing =
      """
         |import com.digitalasset.canton.SequencerAlias
         |import com.digitalasset.canton.sequencing.SequencerConnection
         |import com.digitalasset.canton.sequencing.SequencerConnections
         |import com.digitalasset.canton.sequencing.SequencerConnectionValidation._
         |import com.digitalasset.canton.sequencing.GrpcSequencerConnection
         |""".stripMargin

    addToBuilder(builder, importStandard)
    addToBuilder(builder, importsSynchronizer)
    addToBuilder(builder, importsSequencing)
    addToBuilder(builder, importsTopology)

    // if we don't have a tty available switch the ammonite frontend to a dumb terminal
    if (noTty) {
      addToBuilder(
        builder,
        "repl.frontEnd() = new ammonite.repl.FrontEnds.JLineUnix(ammonite.compiler.Parsers)",
      )
    }

    if (interactive) {
      addToBuilder(
        builder,
        "repl.pprinter() = repl.pprinter().copy(additionalHandlers = { case p: com.digitalasset.canton.logging.pretty.PrettyPrinting => import com.digitalasset.canton.logging.pretty.Pretty._; p.toTree }, defaultHeight = 100)",
      )
    }

    builder.result()
  }

}

object ConsoleEnvironmentBinding {

  /** where we hide the value of the active environment instance within the scope of our repl
    * *******
    */
  private[console] val BindingName = "__replEnvironmentValue"

}
