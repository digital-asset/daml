// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import com.daml.ledger.api.refinements.ApiTypes
import org.jline.reader.Completer
import org.jline.reader.impl.completer.{ArgumentCompleter, NullCompleter, StringsCompleter}

import scala.collection.JavaConverters._

/**
  * Definition of a command parameter
  * Used for tab completion and help text generation
  */
sealed trait Parameter {
  def paramName: String
  def description: String
  def completer(state: State): Completer
}

/** A literal expected at this position */
final case class ParameterLiteral(value: String) extends Parameter {
  def paramName: String = value
  def description: String = value
  def completer(state: State): Completer = new StringsCompleter(value)
}

/** One of a given list of literals */
final case class ParameterLiterals(name: String, values: List[String]) extends Parameter {
  def paramName: String = s"<$name>"
  def description: String = s"One of: ${values.map(s => s"'$s'").mkString(", ")}"
  def completer(state: State): Completer = new StringsCompleter(values.asJava)
}

/** An arbitrary string */
final case class ParameterString(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

/** A GraphQL query string */
final case class ParameterGraphQL(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

/** A SQL query string */
final case class ParameterSQL(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

final case class ParameterParty(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer =
    new StringsCompleter(state.config.parties.toList.map(p => ApiTypes.Party.unwrap(p.name)).asJava)
}

final case class ParameterTemplateId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = {
    val templateNames: List[String] = state.getPartyState
      .map(s => s.packageRegistry.allTemplates().toList.map(t => t.idString))
      .getOrElse(List.empty)
    new StringsCompleter(templateNames.asJava)
  }
}

final case class ParameterPackageId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = {
    val packageNames: List[String] = state.getPartyState
      .map(s => s.packageRegistry.allPackages().toList.map(p => p.id))
      .getOrElse(List.empty)
    new StringsCompleter(packageNames.asJava)
  }
}

final case class ParameterContractId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = {
    // TODO: Build a proper contract ID completer, that does not need to be rebuilt with each new contract
    state.getPartyState.map(_ => NullCompleter.INSTANCE).getOrElse(NullCompleter.INSTANCE)
  }
}

final case class ParameterCommandId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = {
    // TODO: Build a proper command ID completer, that does not need to be rebuilt with each new command
    state.getPartyState.map(_ => NullCompleter.INSTANCE).getOrElse(NullCompleter.INSTANCE)
  }
}

final case class ParameterChoiceId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = {
    // Currently, the completer returns all choice names
    // TODO: Write a custom completer, suggestions depend on template
    val choiceNames: List[String] = state.getPartyState
      .map(
        s =>
          s.packageRegistry
            .allTemplates()
            .toList
            .flatMap(t => t.choices.map(c => ApiTypes.Choice.unwrap(c.name))))
      .getOrElse(List.empty)
    new StringsCompleter(choiceNames.asJava)
  }
}

final case class ParameterTransactionId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  // TODO: Build a proper transaction ID completer, that does not need to be rebuilt with each new transaction
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

final case class ParameterEventId(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  // TODO: Build a proper transaction ID completer, that does not need to be rebuilt with each new transaction
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

final case class ParameterDamlValue(name: String, description: String) extends Parameter {
  def paramName: String = s"<$name>"
  def completer(state: State): Completer = NullCompleter.INSTANCE
}

object Parameter {
  def completer(state: State, params: List[Parameter]): ArgumentCompleter = {
    val completer = new ArgumentCompleter(
      (params.map(_.completer(state)) :+ NullCompleter.INSTANCE).asJava)
    completer.setStrict(true)
    completer
  }

  def isLiteral(param: Parameter): Boolean = param match {
    case _: ParameterLiteral => true
    case _ => false
  }
}
