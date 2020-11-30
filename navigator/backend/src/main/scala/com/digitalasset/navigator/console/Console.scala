// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import java.nio.file.Paths

import akka.actor.ActorRef
import com.daml.navigator.{ApplicationInfo, GraphQLHandler}
import com.daml.navigator.console.{commands => Cmd}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.config.{Arguments, Config}
import org.jline.reader.impl.DefaultParser
import org.jline.reader.impl.history.DefaultHistory
import org.jline.reader.{LineReader, LineReaderBuilder}
import org.jline.terminal.TerminalBuilder

import scala.collection.immutable.Nil
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
object Console {

  final val appName = "Navigator"

  final val commands = CommandSet(
    List(
      Cmd.Choice,
      Cmd.Command,
      Cmd.Commands,
      Cmd.Contract,
      Cmd.Create,
      Cmd.DiffContracts,
      Cmd.Event,
      Cmd.Exercise,
      Cmd.Help,
      Cmd.GraphQL,
      Cmd.GraphQLExamples,
      Cmd.GraphQLSchema,
      Cmd.Info,
      Cmd.Package,
      Cmd.Packages,
      Cmd.Parties,
      Cmd.Party,
      Cmd.Quit,
      Cmd.SetTime,
      Cmd.Templates,
      Cmd.Template,
      Cmd.Time,
      Cmd.Transaction,
      Cmd.Version,
      Cmd.SqlSchema,
      Cmd.Sql
    ))

  private def leftPrompt(state: State): String = s"${state.party}>"
  private def rightPrompt(state: State): String = {
    val offset = state.getPartyState
      .flatMap(ps => ps.ledger.latestTransaction(ps.packageRegistry))
      .map(tx => tx.offset)
      .getOrElse("")
    offset.toString
  }

  private def initialState(
      arguments: Arguments,
      config: Config,
      store: ActorRef,
      graphQL: GraphQLHandler,
      applicationInfo: ApplicationInfo
  ): State = rebuildReader(
    State(
      terminal = TerminalBuilder.builder
        .system(true)
        .build(),
      reader = LineReaderBuilder.builder
        .build(),
      history = new DefaultHistory(),
      quit = false,
      rebuildLineReader = false,
      party = config.users.values.headOption.map(ps => ps.party).getOrElse(ApiTypes.Party("???")),
      arguments = arguments,
      config = config,
      store = store,
      ec = ExecutionContext.global,
      graphQL = graphQL,
      applicationInfo = applicationInfo
    )
  )

  private def rebuildReader(state: State): State = {
    val reader = LineReaderBuilder.builder
      .appName(appName)
      .completer(commands.completer(state))
      .parser(new DefaultParser().quoteChars(Array()))
      .variable(LineReader.HISTORY_FILE, Paths.get(".", "navigator.history"))
      .history(state.history)
      .build()

    state.copy(reader = reader, rebuildLineReader = false)
  }

  private def dispatch(state: State, args: List[String]): State =
    args match {
      case Nil => state
      case "" :: _ => state
      case cmdName :: cmdArgs =>
        commands
          .eval(state, cmdName, cmdArgs)
          .fold(
            e => {
              println(e.message + e.reason.map(r => s" Details:\n$r").getOrElse(""))
              state
            },
            r => {
              println(r._2)
              r._1
            }
          )
    }

  def run(
      arguments: Arguments,
      config: Config,
      store: ActorRef,
      graphQL: GraphQLHandler,
      applicationInfo: ApplicationInfo
  ): Unit = {
    var state = initialState(arguments, config, store, graphQL, applicationInfo)
    state.history.load()
    println("Welcome to the console. Type 'help' to see a list of commands.")
    try {
      while (!state.quit) {
        val oldRegistry = state.getPartyState.map(ps => ps.packageRegistry)

        // Note: this call is blocking (until the user enters a line)
        state.reader.readLine(leftPrompt(state), rightPrompt(state), null: Character, null: String)
        val words = List(state.reader.getParsedLine.words().asScala: _*)

        // Note: this call is blocking (some commands wait for asynchronous results)
        state = dispatch(state, words)

        // Rebuild the line reader if the package registry has changed,
        // to update completers for template names
        val newRegistry = state.getPartyState.map(ps => ps.packageRegistry)
        if (state.rebuildLineReader || oldRegistry != newRegistry) {
          state = rebuildReader(state)
        }
      }
    } catch {
      case _: org.jline.reader.EndOfFileException => println("EndOfFileException")
      case _: org.jline.reader.UserInterruptException => println("UserInterruptException")
      case e: Throwable => println(e.getMessage)
    }
    state.history.save()

    Thread.sleep(100)
    System.exit(0)
  }

}
