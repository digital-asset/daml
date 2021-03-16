// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Graphs
import scalaz.std.option._
import scalaz.std.iterable._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.collection.compat._
import scala.collection.mutable.ListBuffer

object TreeUtils {
  final case class Selector(i: Int)

  def contractsReferences(contracts: Iterable[CreatedEvent]): Set[PackageId] = {
    contracts
      .foldMap(ev => valueRefs(Sum.Record(ev.getCreateArguments)))
      .map(i => PackageId.assertFromString(i.packageId))
  }

  def treesReferences(transactions: Iterable[TransactionTree]): Set[PackageId] = {
    transactions
      .foldMap(treeRefs(_))
      .map(i => PackageId.assertFromString(i.packageId))
  }

  /** Sort the active contract set topologically,
    *  such that a contract at a given position in the list
    *  is only referenced by other contracts later in the list.
    *
    * Throws [[IllegalArgumentException]] if it encounters cyclic dependencies.
    */
  def topoSortAcs(acs: Map[ContractId, CreatedEvent]): List[CreatedEvent] = {
    val graph: Graphs.Graph[ContractId] = acs.view.mapValues(createdReferencedCids).toMap
    Graphs.topoSort(graph) match {
      case Left(cycle) =>
        throw new IllegalArgumentException(s"Encountered cyclic contract dependencies: $cycle")
      case Right(sorted) => sorted.map(cid => acs.get(cid).get)
    }
  }

  def traverseTree(tree: TransactionTree)(f: (List[Selector], TreeEvent.Kind) => Unit): Unit = {
    tree.rootEventIds.map(tree.eventsById(_)).zipWithIndex.foreach { case (ev, i) =>
      traverseEventInTree(ev.kind, tree) { case (path, ev) => f(Selector(i) :: path, ev) }
    }
  }

  def traverseEventInTree(event: TreeEvent.Kind, tree: TransactionTree)(
      f: (List[Selector], TreeEvent.Kind) => Unit
  ): Unit = {
    event match {
      case Kind.Empty =>
      case created @ Kind.Created(_) =>
        f(List(), created)
      case exercised @ Kind.Exercised(value) =>
        f(List(), exercised)
        value.childEventIds.map(x => tree.eventsById(x).kind).zipWithIndex.foreach { case (ev, i) =>
          traverseEventInTree(ev, tree) { case (path, ev) => f(Selector(i) :: path, ev) }
        }
    }
  }

  def partiesInContracts(contracts: Iterable[CreatedEvent]): Set[Party] = {
    contracts.foldMap(ev => valueParties(Value.Sum.Record(ev.getCreateArguments)))
  }

  def partiesInTree(tree: TransactionTree): Set[Party] = {
    var parties: Set[Party] = Set()
    traverseTree(tree) { case (_, ev) =>
      ev match {
        case Kind.Empty =>
        case Kind.Created(value) =>
          parties = parties.union(valueParties(Value.Sum.Record(value.getCreateArguments)))
        case Kind.Exercised(value) =>
          parties = parties.union(valueParties(value.getChoiceArgument.sum))
      }
    }
    parties
  }

  private def valueParties(v: Value.Sum): Set[Party] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) =>
      value.fields.map(v => valueParties(v.getValue.sum)).foldLeft(Set[Party]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Variant(value) => valueParties(value.getValue.sum)
    case Sum.ContractId(_) => Set()
    case Sum.List(value) =>
      value.elements.map(v => valueParties(v.sum)).foldLeft(Set[Party]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) => Set()
    case Sum.Party(value) => Set(Party(value))
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set()
    case Sum.Optional(value) => value.value.fold(Set[Party]())(v => valueParties(v.sum))
    case Sum.Map(value) =>
      value.entries.map(e => valueParties(e.getValue.sum)).foldLeft(Set[Party]()) { case (x, xs) =>
        x.union(xs)
      }
    case Sum.Enum(_) => Set[Party]()
    case Sum.GenMap(value) =>
      value.entries.map(e => valueParties(e.getValue.sum)).foldLeft(Set[Party]()) { case (x, xs) =>
        x.union(xs)
      }
  }

  case class CreatedContract(cid: ContractId, tplId: Identifier, path: List[Selector])

  def treeCreatedCids(tree: TransactionTree): Seq[CreatedContract] = {
    var cids: Seq[CreatedContract] = Seq()
    traverseTree(tree) { case (selectors, kind) =>
      kind match {
        case Kind.Empty =>
        case Kind.Exercised(_) =>
        case Kind.Created(value) =>
          cids ++= Seq(
            CreatedContract(ContractId(value.contractId), value.getTemplateId, selectors)
          )
      }
    }
    cids
  }

  def treeEventCreatedCids(event: TreeEvent.Kind, tree: TransactionTree): Seq[ContractId] = {
    val creates = ListBuffer.empty[ContractId]
    traverseEventInTree(event, tree) {
      case (_, Kind.Created(value)) => creates += ContractId(value.contractId)
      case _ =>
    }
    creates.toSeq
  }

  def treeReferencedCids(tree: TransactionTree): Set[ContractId] = {
    tree.rootEventIds.map(tree.eventsById(_).kind).foldMap {
      case Kind.Empty => Set.empty[ContractId]
      case Kind.Created(value) =>
        createdReferencedCids(value)
      case Kind.Exercised(value) =>
        value.choiceArgument.foldMap(arg => valueCids(arg.sum)) + ContractId(value.contractId)
    }
  }

  def createdReferencedCids(ev: CreatedEvent): Set[ContractId] =
    ev.createArguments.foldMap(args => args.fields.foldMap(f => valueCids(f.getValue.sum)))

  def cmdReferencedCids(cmd: Command): Set[ContractId] = {
    cmd match {
      case CreateCommand(createdEvent) =>
        createdReferencedCids(createdEvent)
      case ExerciseCommand(exercisedEvent) =>
        exercisedEvent.choiceArgument.foldMap(arg => valueCids(arg.sum)) + ContractId(
          exercisedEvent.contractId
        )
      case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
        createdReferencedCids(createdEvent) ++ exercisedEvent.choiceArgument.foldMap(arg =>
          valueCids(arg.sum)
        )
    }
  }

  sealed trait Command
  final case class CreateCommand(createdEvent: CreatedEvent) extends Command
  final case class ExerciseCommand(exercisedEvent: ExercisedEvent) extends Command
  final case class CreateAndExerciseCommand(
      createdEvent: CreatedEvent,
      exercisedEvent: ExercisedEvent,
  ) extends Command

  object Command {
    def fromTree(tree: TransactionTree): Seq[Command] = {
      val rootEvents = tree.rootEventIds.map(tree.eventsById(_).kind)
      val commands = ListBuffer.empty[Command]
      rootEvents.foreach {
        case Kind.Empty =>
        case Kind.Created(createdEvent) =>
          commands += CreateCommand(createdEvent)
        case Kind.Exercised(exercisedEvent) =>
          commands.lastOption match {
            case Some(CreateCommand(createdEvent))
                if createdEvent.contractId == exercisedEvent.contractId =>
              commands.update(
                commands.length - 1,
                CreateAndExerciseCommand(createdEvent, exercisedEvent),
              )
            case _ =>
              commands += ExerciseCommand(exercisedEvent)
          }
      }
      commands.toSeq
    }
  }

  /** A simple command causes the creation of a single contract and returns its contract id.
    *
    * A create command is a simple command. An exercise command can be a simple command.
    */
  case class SimpleCommand(command: Command, contractId: ContractId)

  object SimpleCommand {
    def fromCommand(command: Command, tree: TransactionTree): Option[SimpleCommand] = {
      def simpleExercise(exercisedEvent: ExercisedEvent): Option[ContractId] = {
        val result = exercisedEvent.exerciseResult.flatMap {
          _.sum match {
            case Sum.ContractId(value) => Some(value)
            case _ => None
          }
        }
        val creates = treeEventCreatedCids(Kind.Exercised(exercisedEvent), tree)
        (result, creates) match {
          case (Some(cid), Seq(createdCid)) if cid == createdCid =>
            Some(ContractId(cid))
          case _ => None
        }
      }
      command match {
        case CreateCommand(createdEvent) =>
          Some(SimpleCommand(command, ContractId(createdEvent.contractId)))
        case ExerciseCommand(exercisedEvent) =>
          simpleExercise(exercisedEvent).map(SimpleCommand(command, _))
        case CreateAndExerciseCommand(_, exercisedEvent) =>
          if (exercisedEvent.consuming) {
            // If the choice is not consuming then we have two resulting contracts:
            // The created one and the result of the exercise. I.e. not a simple command.
            simpleExercise(exercisedEvent).map(SimpleCommand(command, _))
          } else {
            None
          }
      }
    }

    def fromCommands(commands: Seq[Command], tree: TransactionTree): Option[Seq[SimpleCommand]] = {
      import scalaz.Scalaz._
      commands.toList.traverse(SimpleCommand.fromCommand(_, tree))
    }
  }

  def evParties(ev: TreeEvent.Kind): Seq[Party] = ev match {
    case TreeEvent.Kind.Created(create) => Party.subst(create.signatories)
    case TreeEvent.Kind.Exercised(exercised) => Party.subst(exercised.actingParties)
    case TreeEvent.Kind.Empty => Seq()
  }

  def cmdParties(cmd: Command): Seq[Party] = {
    cmd match {
      case CreateCommand(createdEvent) => evParties(Kind.Created(createdEvent))
      case ExerciseCommand(exercisedEvent) => evParties(Kind.Exercised(exercisedEvent))
      case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
        evParties(Kind.Created(createdEvent)) ++ evParties(Kind.Exercised(exercisedEvent))
    }
  }

  def treeRefs(t: TransactionTree): Set[Identifier] =
    t.eventsById.values.foldMap(e => evRefs(e.kind))

  def evRefs(e: TreeEvent.Kind): Set[Identifier] = e match {
    case Kind.Empty => Set()
    case Kind.Created(value) => valueRefs(Sum.Record(value.getCreateArguments))
    case Kind.Exercised(value) => valueRefs(value.getChoiceArgument.sum)
  }

  def valueRefs(v: Value.Sum): Set[Identifier] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) =>
      Set(value.getRecordId).union(value.fields.foldMap(f => valueRefs(f.getValue.sum)))
    case Sum.Variant(value) => Set(value.getVariantId).union(valueRefs(value.getValue.sum))
    case Sum.ContractId(_) => Set()
    case Sum.List(value) => value.elements.foldMap(v => valueRefs(v.sum))
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) => Set()
    case Sum.Party(_) => Set()
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set()
    case Sum.Optional(value) => value.value.foldMap(v => valueRefs(v.sum))
    case Sum.Map(value) => value.entries.foldMap(e => valueRefs(e.getValue.sum))
    case Sum.Enum(value) => Set(value.getEnumId)
    case Sum.GenMap(value) =>
      value.entries.foldMap(e => valueRefs(e.getKey.sum).union(valueRefs(e.getValue.sum)))
  }

  def valueCids(v: Value.Sum): Set[ContractId] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) => value.fields.foldMap(f => valueCids(f.getValue.sum))
    case Sum.Variant(value) => valueCids(value.getValue.sum)
    case Sum.ContractId(cid) => Set(ContractId(cid))
    case Sum.List(value) => value.elements.foldMap(v => valueCids(v.sum))
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) => Set()
    case Sum.Party(_) => Set()
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set()
    case Sum.Optional(value) => value.value.foldMap(v => valueCids(v.sum))
    case Sum.Map(value) => value.entries.foldMap(e => valueCids(e.getValue.sum))
    case Sum.Enum(_) => Set()
    case Sum.GenMap(value) =>
      value.entries.foldMap(e => valueCids(e.getKey.sum).union(valueCids(e.getValue.sum)))
  }

}
