// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import java.time.Instant
import com.daml.ledger.api.refinements.ApiTypes.{Choice, ContractId, InterfaceId, Party, TemplateId}
import com.daml.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent.Kind
import com.daml.ledger.api.v1.value.{Identifier, Value}
import com.daml.ledger.api.v1.value.Value.Sum
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.{Graphs, StablePackagesV1}
import com.daml.scalautil.Statement.discard
import scalaz.std.option._
import scalaz.std.iterable._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object TreeUtils {
  sealed trait Selector
  final case class CreatedSelector(
      templateId: TemplateId,
      index: Int,
  ) extends Selector
  final case class ExercisedSelector(
      templateId: TemplateId,
      interfaceId: Option[InterfaceId],
      choice: Choice,
      index: Int,
  ) extends Selector

  def contractsReferences(contracts: Iterable[CreatedEvent]): Set[PackageId] = {
    contracts
      .foldMap(ev => valueRefs(Sum.Record(ev.getCreateArguments)))
      .filter(i => i.packageId.nonEmpty)
      .map(i => PackageId.assertFromString(i.packageId))
  }

  def treesReferences(transactions: Iterable[TransactionTree]): Set[PackageId] = {
    transactions
      .foldMap(treeRefs(_))
      .filter(i => i.packageId.nonEmpty)
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
      case Right(sorted) => sorted.collect(Function.unlift(cid => acs.get(cid)))
    }
  }

  private def foreachEventWithSelector(
      events: Seq[TreeEvent.Kind]
  )(f: (TreeEvent.Kind, Selector) => Unit): Unit = {
    val created = mutable.HashMap.empty[TemplateId, Int].withDefaultValue(0)
    val exercised = mutable.HashMap.empty[(TemplateId, Choice), Int].withDefaultValue(0)
    events.foreach {
      case Kind.Empty =>
      case event @ Kind.Created(value) =>
        val templateId = TemplateId(value.getTemplateId)
        val index = created(templateId)
        val selector = CreatedSelector(templateId, index)
        created(templateId) += 1
        f(event, selector)
      case event @ Kind.Exercised(value) =>
        val templateId = TemplateId(value.getTemplateId)
        val interfaceId = InterfaceId.subst(value.interfaceId)
        val choice = Choice(value.choice)
        val index = exercised((templateId, choice))
        val selector = ExercisedSelector(templateId, interfaceId, choice, index)
        exercised((templateId, choice)) += 1
        f(event, selector)
    }
  }

  def traverseTree(tree: TransactionTree)(f: (List[Selector], TreeEvent.Kind) => Unit): Unit = {
    val rootEvents = tree.rootEventIds.map(id => tree.eventsById(id).kind)
    foreachEventWithSelector(rootEvents) { case (ev, selector) =>
      traverseEventInTree(ev, tree) { case (path, ev) => f(selector :: path, ev) }
    }
  }

  def traverseEventInTree(event: TreeEvent.Kind, tree: TransactionTree)(
      f: (List[Selector], TreeEvent.Kind) => Unit
  ): Unit = {
    event match {
      case Kind.Empty =>
      case created @ Kind.Created(_) =>
        f(Nil, created)
      case exercised @ Kind.Exercised(value) =>
        f(Nil, exercised)
        val childEvents = value.childEventIds.map(id => tree.eventsById(id).kind)
        foreachEventWithSelector(childEvents) { case (ev, selector) =>
          traverseEventInTree(ev, tree) { case (path, ev) => f(selector :: path, ev) }
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

  private[export] def valueParties(v: Value.Sum): Set[Party] = v match {
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
      value.entries
        .map(e => valueParties(e.getKey.sum) union valueParties(e.getValue.sum))
        .foldLeft(Set[Party]()) { case (x, xs) =>
          x.union(xs)
        }
  }

  sealed abstract class CreatedContract extends Product with Serializable {
    def cid: ContractId
    def tplId: TemplateId
  }

  object CreatedContract {
    final case class SimpleCreatedContract(cid: ContractId, tplId: TemplateId)
        extends CreatedContract

    def apply(cid: ContractId, tplId: TemplateId): CreatedContract =
      SimpleCreatedContract(cid, tplId)
  }

  final case class CreatedContractWithPath(cid: ContractId, tplId: TemplateId, path: List[Selector])
      extends CreatedContract

  def treeCreatedCids(tree: TransactionTree): List[CreatedContractWithPath] = {
    var cids: List[CreatedContractWithPath] = List()
    traverseTree(tree) { case (selectors, kind) =>
      kind match {
        case Kind.Empty =>
        case Kind.Exercised(_) =>
        case Kind.Created(value) =>
          cids ::=
            CreatedContractWithPath(
              ContractId(value.contractId),
              TemplateId(value.getTemplateId),
              selectors,
            )
      }
    }
    cids.reverse
  }

  def treeEventCreatedConsumedCids(
      event: TreeEvent.Kind,
      tree: TransactionTree,
  ): (List[CreatedContract], Set[ContractId]) = {
    var created = List.empty[CreatedContract]
    var consumed = Set.empty[ContractId]
    traverseEventInTree(event, tree) {
      case (_, Kind.Created(value)) =>
        created ::= CreatedContract(
          ContractId(value.contractId),
          TemplateId(value.getTemplateId),
        )
      case (_, Kind.Exercised(value)) if value.consuming =>
        consumed += ContractId(value.contractId)
      case _ =>
    }
    (created.reverse, consumed)
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
      case ExerciseByKeyCommand(exercisedEvent, _, _) =>
        exercisedEvent.choiceArgument.foldMap(arg => valueCids(arg.sum))
      case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
        createdReferencedCids(createdEvent) ++ exercisedEvent.choiceArgument.foldMap(arg =>
          valueCids(arg.sum)
        )
    }
  }

  sealed trait Command
  final case class CreateCommand(createdEvent: CreatedEvent) extends Command
  final case class ExerciseCommand(exercisedEvent: ExercisedEvent) extends Command
  final case class ExerciseByKeyCommand(
      exercisedEvent: ExercisedEvent,
      templateId: Identifier,
      contractKey: Value,
  ) extends Command
  final case class CreateAndExerciseCommand(
      createdEvent: CreatedEvent,
      exercisedEvent: ExercisedEvent,
  ) extends Command

  object Command {
    def fromTree(tree: TransactionTree): Seq[Command] = {
      val contractKeys = mutable.HashMap.empty[ContractId, Value]
      def addContractKey(createdEvent: CreatedEvent): Unit = {
        createdEvent.contractKey.foreach { contractKey =>
          contractKeys += ContractId(createdEvent.contractId) -> contractKey
        }
      }
      def removeArchivedContractKey(exercisedEvent: ExercisedEvent): Unit = {
        if (exercisedEvent.consuming) {
          discard(contractKeys -= ContractId(exercisedEvent.contractId))
        }
      }
      def updateContractKeys(ev: Kind): Unit = {
        traverseEventInTree(ev, tree) {
          case (_, Kind.Empty) => ()
          case (_, Kind.Created(createdEvent)) => addContractKey(createdEvent)
          case (_, Kind.Exercised(exercisedEvent)) => removeArchivedContractKey(exercisedEvent)
        }
      }
      val rootEvents = tree.rootEventIds.map(tree.eventsById(_).kind)
      val commands = ListBuffer.empty[Command]
      rootEvents.foreach {
        case Kind.Empty =>
        case Kind.Created(createdEvent) =>
          discard(commands += CreateCommand(createdEvent))
          addContractKey(createdEvent)
        case ev @ Kind.Exercised(exercisedEvent) =>
          val optCreateAndExercise = commands.lastOption.flatMap {
            case CreateCommand(createdEvent)
                if createdEvent.contractId == exercisedEvent.contractId =>
              Some(CreateAndExerciseCommand(createdEvent, exercisedEvent))
            case _ => None
          }
          lazy val optExerciseByKey =
            for {
              contractKey <- contractKeys.get(ContractId(exercisedEvent.contractId))
              templateId <- exercisedEvent.templateId
            } yield ExerciseByKeyCommand(exercisedEvent, templateId, contractKey)
          optCreateAndExercise match {
            case Some(command) => commands.update(commands.length - 1, command)
            case None =>
              optExerciseByKey match {
                case Some(command) => discard(commands += command)
                case None => discard(commands += ExerciseCommand(exercisedEvent))
              }
          }
          updateContractKeys(ev)
      }
      commands.toSeq
    }
  }

  /** A simple command causes the creation of a single contract and returns its contract id.
    *
    * A create command is a simple command. An exercise command can be a simple command.
    */
  case class SimpleCommand(command: Command, contractId: CreatedContract)

  object SimpleCommand {
    def fromCommand(command: Command, tree: TransactionTree): Option[SimpleCommand] = {
      def simpleExercise(
          exercisedEvent: ExercisedEvent,
          extraCreated: List[CreatedContract],
      ): Option[CreatedContract] = {
        val result = exercisedEvent.exerciseResult.flatMap {
          _.sum match {
            case Sum.ContractId(value) => Some(value)
            case _ => None
          }
        }
        val (created, consumed) = treeEventCreatedConsumedCids(Kind.Exercised(exercisedEvent), tree)
        val netCreated = (extraCreated ++ created).filterNot(c => consumed.contains(c.cid))
        (result, netCreated) match {
          case (Some(cid), List(created)) if cid == created.cid =>
            Some(created)
          case _ => None
        }
      }
      command match {
        case CreateCommand(createdEvent) =>
          Some(
            SimpleCommand(
              command,
              CreatedContract(
                ContractId(createdEvent.contractId),
                TemplateId(createdEvent.getTemplateId),
              ),
            )
          )
        case ExerciseCommand(exercisedEvent) =>
          simpleExercise(exercisedEvent, List.empty).map(SimpleCommand(command, _))
        case ExerciseByKeyCommand(exercisedEvent, _, _) =>
          simpleExercise(exercisedEvent, List.empty).map(SimpleCommand(command, _))
        case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
          // We count the contract created by the created event into the set of created contracts.
          // The command is only considered simple if it only creates one contract overall and returns its id.
          simpleExercise(
            exercisedEvent,
            List(
              CreatedContract(
                ContractId(createdEvent.contractId),
                TemplateId(createdEvent.getTemplateId),
              )
            ),
          )
            .map(SimpleCommand(command, _))
      }
    }

    def fromCommands(commands: Seq[Command], tree: TransactionTree): Option[Seq[SimpleCommand]] = {
      import scalaz.Scalaz._
      commands.toList.traverse(SimpleCommand.fromCommand(_, tree))
    }
  }

  sealed trait Action

  final case class SetTime(timestamp: Timestamp) extends Action

  sealed trait Submit extends Action {
    def submitters: Set[Party]
    def commands: Seq[Command]
  }

  sealed trait SubmitSingle extends Submit {
    def submitter: Party
    override def submitters: Set[Party] = Set(submitter)
  }
  sealed trait SubmitMulti extends Submit

  sealed trait SubmitSimple extends Submit {
    def simpleCommands: Seq[SimpleCommand]
    def commands: Seq[Command] = simpleCommands.map(_.command)
  }
  sealed trait SubmitTree extends Submit {
    def tree: TransactionTree
  }

  final case class SubmitSimpleSingle(simpleCommands: Seq[SimpleCommand], submitter: Party)
      extends Submit
      with SubmitSimple
      with SubmitSingle
  final case class SubmitSimpleMulti(simpleCommands: Seq[SimpleCommand], submitters: Set[Party])
      extends Submit
      with SubmitSimple
      with SubmitMulti
  final case class SubmitTreeSingle(commands: Seq[Command], tree: TransactionTree, submitter: Party)
      extends Submit
      with SubmitTree
      with SubmitSingle
  final case class SubmitTreeMulti(
      commands: Seq[Command],
      tree: TransactionTree,
      submitters: Set[Party],
  ) extends Submit
      with SubmitTree
      with SubmitMulti

  object Action {
    def fromACS(acs: Seq[CreatedEvent], batchSize: Int): Seq[Action] = {
      acs
        .grouped(batchSize)
        .map(SubmitSimple.fromCreatedEvents)
        .toSeq
    }
    def fromTrees(trees: Seq[TransactionTree], setTime: Boolean): Seq[Action] = {
      if (setTime) {
        var currentTime = Timestamp.MinValue
        val result = ArrayBuffer.newBuilder[Action]
        trees.foreach { tree =>
          val timestamp = timestampFromTree(tree)
          val submit = Submit.fromTree(tree)
          if (timestamp > currentTime) {
            currentTime = timestamp
            discard(result += SetTime(timestamp))
          }
          result += submit
        }
        result.result().toSeq
      } else {
        trees.map(Submit.fromTree)
      }
    }
  }

  object Submit {
    def fromTree(tree: TransactionTree): Submit = {
      val commands = Command.fromTree(tree)
      val submitters = commands.foldMap(cmdParties)
      val optSimpleCommands = SimpleCommand.fromCommands(commands, tree)
      (submitters.toSeq, optSimpleCommands) match {
        case (Seq(submitter), Some(simpleCommands)) =>
          SubmitSimpleSingle(simpleCommands, submitter)
        case (_, Some(simpleCommands)) =>
          SubmitSimpleMulti(simpleCommands, submitters)
        case (Seq(submitter), None) =>
          SubmitTreeSingle(commands, tree, submitter)
        case (_, None) =>
          SubmitTreeMulti(commands, tree, submitters)
      }
    }
  }

  object SubmitSimple {
    def fromCreatedEvents(createdEvents: Seq[CreatedEvent]): SubmitSimple = {
      val simpleCommands =
        createdEvents.map(ev =>
          SimpleCommand(
            CreateCommand(ev),
            CreatedContract(
              ContractId(ev.contractId),
              TemplateId(ev.getTemplateId),
            ),
          )
        )
      val submitters = Party.subst(createdEvents.foldMap(_.signatories.toSet))
      submitters.toSeq match {
        case Seq(submitter) => SubmitSimpleSingle(simpleCommands, submitter)
        case _ => SubmitSimpleMulti(simpleCommands, submitters)
      }
    }
  }

  def evParties(ev: TreeEvent.Kind): Set[Party] = ev match {
    case TreeEvent.Kind.Created(create) => Party.subst(create.signatories).toSet
    case TreeEvent.Kind.Exercised(exercised) => Party.subst(exercised.actingParties).toSet
    case TreeEvent.Kind.Empty => Set.empty[Party]
  }

  def cmdParties(cmd: Command): Set[Party] = {
    cmd match {
      case CreateCommand(createdEvent) => evParties(Kind.Created(createdEvent))
      case ExerciseCommand(exercisedEvent) => evParties(Kind.Exercised(exercisedEvent))
      case ExerciseByKeyCommand(exercisedEvent, _, _) => evParties(Kind.Exercised(exercisedEvent))
      case CreateAndExerciseCommand(createdEvent, exercisedEvent) =>
        evParties(Kind.Created(createdEvent)) ++ evParties(Kind.Exercised(exercisedEvent))
    }
  }

  def timestampFromTree(tree: TransactionTree): Timestamp = {
    Timestamp.assertFromInstant(
      Instant.ofEpochSecond(tree.getEffectiveAt.seconds, tree.getEffectiveAt.nanos.toLong)
    )
  }

  def treeRefs(t: TransactionTree): Set[Identifier] =
    t.eventsById.values.foldMap(e => evRefs(e.kind))

  def evRefs(e: TreeEvent.Kind): Set[Identifier] = e match {
    case Kind.Empty => Set()
    case Kind.Created(value) => valueRefs(Sum.Record(value.getCreateArguments))
    case Kind.Exercised(value) => valueRefs(value.getChoiceArgument.sum)
  }

  /** All identifiers referenced by the given value that require a module import.
    *
    * Note, the package-id component of the identifier may be empty for stable packages
    * as those are imported via daml-stdlib/daml-prim. E.g. DA.Time.Time incurred by Timestamp values.
    */
  def valueRefs(v: Value.Sum): Set[Identifier] = v match {
    case Sum.Empty => Set()
    case Sum.Record(value) =>
      val fieldRefs = value.fields.foldMap(f => valueRefs(f.getValue.sum))
      if (isTupleId(value.getRecordId)) {
        fieldRefs
      } else {
        fieldRefs + value.getRecordId
      }
    case Sum.Variant(value) => valueRefs(value.getValue.sum) + value.getVariantId
    case Sum.ContractId(_) => Set()
    case Sum.List(value) => value.elements.foldMap(v => valueRefs(v.sum))
    case Sum.Int64(_) => Set()
    case Sum.Numeric(_) => Set()
    case Sum.Text(_) => Set()
    case Sum.Timestamp(_) =>
      Set(
        Identifier().withModuleName("DA.Date").withEntityName("Date"),
        Identifier().withModuleName("DA.Time").withEntityName("Time"),
      )
    case Sum.Party(_) => Set()
    case Sum.Bool(_) => Set()
    case Sum.Unit(_) => Set()
    case Sum.Date(_) => Set(Identifier().withModuleName("DA.Date").withEntityName("Date"))
    case Sum.Optional(value) => value.value.foldMap(v => valueRefs(v.sum))
    case Sum.Map(value) =>
      value.entries.foldMap(e => valueRefs(e.getValue.sum)) + Identifier()
        .withModuleName("DA.TextMap")
        .withEntityName("TextMap")
    case Sum.Enum(value) => Set(value.getEnumId)
    case Sum.GenMap(value) =>
      value.entries.foldMap(e =>
        valueRefs(e.getKey.sum).union(valueRefs(e.getValue.sum))
      ) + Identifier().withModuleName("DA.Map").withEntityName("Map")
  }

  def isTupleId(id: Identifier): Boolean = {
    id.packageId == StablePackagesV1.DA_Types.packageId &&
    id.moduleName == StablePackagesV1.DA_Types.moduleName.toString &&
    id.entityName.startsWith("Tuple")
  }

  def isTupleRefId(name: Ref.Identifier): Boolean = {
    isTupleId(
      Identifier()
        .withPackageId(name.packageId)
        .withModuleName(name.qualifiedName.module.dottedName)
        .withEntityName(name.qualifiedName.name.dottedName)
    )
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
