// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.testing

import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, QualifiedName}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.Event.Events
import com.digitalasset.daml.lf.engine._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.speedy.{ScenarioRunner, Speedy}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction => Tx}
import com.digitalasset.daml.lf.types.{Ledger => L}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  RelativeContractId
}
import com.digitalasset.daml.lf.transaction.VersionTimeline

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

/** Scenario tester.
  *
  * @constructor Creates new tester.
  * @param partyNameMangler allows to amend party names defined in scenarios,
  *        before they are executed against ledger created with {@code createLedger}.
  *        See {@code ScenarioRunner.partyNameMangler} for details.
  * @param commandIdMangler allows to compute command identifiers based on scenario
  *                         name, scenario step identifier and scenario node identifier.
  *                         It can be used to ensure that generated command identifiers
  *                         are unique between runs of two scenarios.
  */
class SemanticTester(
    createLedger: Set[Party] => SemanticTester.GenericLedger,
    packageToTest: PackageId,
    packages: Map[PackageId, Package],
    partyNameMangler: (String => String) = identity,
    partyNameUnmangler: (String => String) = identity,
    commandIdMangler: ((QualifiedName, Int, L.ScenarioNodeId) => String) =
      (scenario, stepId, nodeId) => s"semantic-testing-$scenario-$stepId-$nodeId"
)(implicit ec: ExecutionContext) {
  import SemanticTester._

  // result ledgers from all scenarios found in packages
  private lazy val allScenarioLedgers: Map[QualifiedName, L.Ledger] = {
    val modules = packages(packageToTest).modules.values
    val buildMachine =
      Speedy.Machine
        .newBuilder(PureCompiledPackages(packages).right.get)
        .fold(err => sys.error(err.toString), identity)

    modules.foldLeft(Map.empty[QualifiedName, L.Ledger]) {
      case (scenarios, module) =>
        scenarios ++ module.definitions.collect {
          // Keep in sync with `scenarios` method in SemanticTester object
          case (name, DValue(_, _, body, isTest)) if isTest =>
            val qualifiedName = QualifiedName(module.name, name)
            val machine = buildMachine(
              VersionTimeline.checkSubmitterInMaintainers(module.languageVersion),
              body)
            ScenarioRunner(machine, partyNameMangler = partyNameMangler).run() match {
              case Left((err, _ledger @ _)) =>
                sys.error(s"error running scenario $err in scenario: $qualifiedName")
              case Right((_time @ _, _steps @ _, ledger)) =>
                qualifiedName -> ledger
            }
        }
    }
  }

  // collect all parties for scenarios in a package
  // this is to be used for initializing platform,
  // and setting listeners on the ledger-api calls
  lazy val packageParties: Set[Party] =
    allScenarioLedgers.flatMap {
      case (_, ledger) =>
        ledger.ledgerData.nodeInfos.values.flatMap {
          _.observingSince.keys
        }
    }.toSet

  // test a given scenario
  // returns future Unit or a failed future with an exception
  def testScenario(scenario: QualifiedName): Future[Unit] = {
    val scenarioLedger = allScenarioLedgers(scenario)
    val ledger: GenericLedger = createLedger(packageParties)

    //
    // Wrapper for the NodeId and a flag used for handling ExerciseNodes.
    //
    // The children of the exercise node need to be processed before the testing the exercise node as testing
    // the result value of the exercise requires knowing the contract IDs of child contracts and records.
    //
    case class StackNode(
        /** Identifier for the Node */
        nid: L.ScenarioNodeId,
        /** True if the children of the exercise node has been added to the stack */
        exerciseAddedChildren: Boolean
    )

    case class TestScenarioState(
        /** Mapping between scenario contracts and ledger contracts */
        scenarioCoidToLedgerCoid: Map[AbsoluteContractId, AbsoluteContractId],
        /** Stack of remaining scenario nodes to visit */
        remainingScenarioNodeIds: FrontStack[StackNode],
        /** Stack of remaining ledger events to visit */
        remainingLedgerEventIds: FrontStack[ledger.EventNodeId])

    // check ledger events and pairs up contract ids
    def checkEvents(
        reference: String,
        scenarioWitnesses: Relation[L.ScenarioNodeId, Party],
        scenarioTransaction: GenTransaction[
          L.ScenarioNodeId,
          AbsoluteContractId,
          Tx.Value[AbsoluteContractId]],
        ledgerEvents: Events[ledger.EventNodeId, AbsoluteContractId, Tx.Value[AbsoluteContractId]],
        scenarioToLedgerMap: Map[AbsoluteContractId, AbsoluteContractId])
      : Map[AbsoluteContractId, AbsoluteContractId] = {

      // expects an event of the given type
      // returns the matched event and the tail of the eventIds
      def popEvent[EvTyp: ClassTag](
          what: String,
          scenarioNode: GenNode.WithTxValue[L.ScenarioNodeId, AbsoluteContractId],
          remainingLedgerEventIds: FrontStack[ledger.EventNodeId])
        : (EvTyp, FrontStack[ledger.EventNodeId]) =
        remainingLedgerEventIds match {
          case FrontStackCons(evId, remainingLedgerEventIds_) =>
            val ev = ledgerEvents.events(evId)
            if (classTag[EvTyp].runtimeClass.isInstance(ev)) {
              (ev.asInstanceOf[EvTyp], remainingLedgerEventIds_)
            } else {
              throw SemanticTesterError(reference, s"Expected $what, but got $ev")
            }
          case FrontStack() =>
            throw SemanticTesterError(
              reference,
              s"Expected $what to match scenario node $scenarioNode but got none.")
        }

      // walk through the scenario nodes and ledger events
      @tailrec
      def go(state: TestScenarioState): Map[AbsoluteContractId, AbsoluteContractId] = {
        state.remainingScenarioNodeIds match {
          // we're done with scenario nodes, we should also be done with ledger events
          case FrontStack() =>
            if (state.remainingLedgerEventIds.nonEmpty) {
              throw SemanticTesterError(
                reference,
                s"Leftover ledger events after finishing scenario nodes: ${state.remainingLedgerEventIds}"
              )
            } else {
              state.scenarioCoidToLedgerCoid
            }

          // we still have nodes...
          case FrontStackCons(
              StackNode(scenarioNodeId, exerciseAddedChildren),
              remainingScenarioNodeIds) =>
            val scenarioNode = scenarioTransaction.nodes(scenarioNodeId)

            // utility to assert that we still have a ledger event,
            // and which also updates the remaining events
            val nextState =
              scenarioNode match {

                case _: NodeFetch[AbsoluteContractId] =>
                  // skip fetch nodes -- they do not get translated to events
                  state.copy(remainingScenarioNodeIds = remainingScenarioNodeIds)

                case _: NodeLookupByKey[_, _] =>
                  // skip lookup by key nodes -- they do not get translated to events
                  state.copy(remainingScenarioNodeIds = remainingScenarioNodeIds)

                case scenarioCreateNode: NodeCreate[
                      AbsoluteContractId,
                      Tx.Value[AbsoluteContractId]] =>
                  val (ledgerCreateEvent, remainingLedgerEventIds) =
                    popEvent[CreateEvent[AbsoluteContractId, Tx.Value[AbsoluteContractId]]](
                      "create event",
                      scenarioNode,
                      state.remainingLedgerEventIds)
                  // store the mapping between the scenario coid and ledger coid
                  val nextScenarioCoidToLedgerCoid = state.scenarioCoidToLedgerCoid
                    .updated(scenarioCreateNode.coid, ledgerCreateEvent.contractId)
                  // create a synthetic create event from the scenario node, by rewriting the
                  // coids in the scenario create to the engine coids
                  val scenarioCreateEvent = CreateEvent(
                    nextScenarioCoidToLedgerCoid(scenarioCreateNode.coid),
                    scenarioCreateNode.coinst.template,
                    scenarioCreateNode.key,
                    scenarioCreateNode.coinst.arg.mapContractId(nextScenarioCoidToLedgerCoid),
                    scenarioCreateNode.coinst.agreementText,
                    scenarioCreateNode.signatories,
                    (scenarioCreateNode.stakeholders diff scenarioCreateNode.signatories),
                    scenarioWitnesses(scenarioNodeId),
                  )
                  val ledgerCreateEventToCompare =
                    ledgerCreateEvent.copy(signatories = Set.empty, observers = Set.empty)
                  val scenarioCreateEventToCompare =
                    scenarioCreateEvent.copy(signatories = Set.empty, observers = Set.empty)
                  // check that they're the same
                  if (scenarioCreateEventToCompare != ledgerCreateEventToCompare) {
                    throw SemanticTesterError(
                      reference,
                      s"Expected create event $scenarioCreateEventToCompare but got $ledgerCreateEventToCompare")
                  }
                  TestScenarioState(
                    scenarioCoidToLedgerCoid = nextScenarioCoidToLedgerCoid,
                    remainingLedgerEventIds = remainingLedgerEventIds,
                    remainingScenarioNodeIds = remainingScenarioNodeIds
                  )

                case scenarioExercisesNode: NodeExercises[
                      L.ScenarioNodeId,
                      AbsoluteContractId,
                      Tx.Value[AbsoluteContractId]] if exerciseAddedChildren =>
                  val (ledgerExerciseEvent, remainingLedgerEventIds) =
                    popEvent[ExerciseEvent[
                      ledger.EventNodeId,
                      AbsoluteContractId,
                      Tx.Value[AbsoluteContractId]]](
                      "exercise event",
                      scenarioNode,
                      state.remainingLedgerEventIds)

                  // create synthetic exercise event, again rewriting the appropriate bits. note that we intentionally
                  // blank the children because we compare them in the recursive call anyway.
                  val scenarioExerciseEvent = ExerciseEvent(
                    state.scenarioCoidToLedgerCoid(scenarioExercisesNode.targetCoid),
                    scenarioExercisesNode.templateId,
                    scenarioExercisesNode.choiceId,
                    scenarioExercisesNode.chosenValue.mapContractId(state.scenarioCoidToLedgerCoid),
                    scenarioExercisesNode.actingParties,
                    scenarioExercisesNode.consuming,
                    ImmArray.empty,
                    scenarioExercisesNode.stakeholders intersect scenarioWitnesses(scenarioNodeId),
                    scenarioWitnesses(scenarioNodeId),
                    scenarioExercisesNode.exerciseResult.map(
                      _.mapContractId(state.scenarioCoidToLedgerCoid))
                  )
                  val ledgerExerciseEventToCompare =
                    ledgerExerciseEvent.copy(children = ImmArray.empty, stakeholders = Set.empty)
                  val comparedScenarioExerciseEvent =
                    scenarioExerciseEvent.copy(stakeholders = Set.empty)
                  if (comparedScenarioExerciseEvent != ledgerExerciseEventToCompare) {
                    throw SemanticTesterError(
                      reference,
                      s"Expected exercise event $comparedScenarioExerciseEvent but got $ledgerExerciseEventToCompare"
                    )
                  }
                  state.copy(
                    remainingScenarioNodeIds = remainingScenarioNodeIds,
                    remainingLedgerEventIds = remainingLedgerEventIds,
                  )

                case scenarioExercisesNode: NodeExercises[
                      L.ScenarioNodeId,
                      AbsoluteContractId,
                      Tx.Value[AbsoluteContractId]] if !exerciseAddedChildren =>
                  val exerciseChildren =
                    popEvent[ExerciseEvent[
                      ledger.EventNodeId,
                      AbsoluteContractId,
                      Tx.Value[AbsoluteContractId]]](
                      "exercise event",
                      scenarioNode,
                      state.remainingLedgerEventIds)._1.children

                  state.copy(
                    remainingLedgerEventIds = exerciseChildren ++: state.remainingLedgerEventIds,
                    remainingScenarioNodeIds = scenarioExercisesNode.children.map(
                      StackNode(_, false)) ++:
                      StackNode(scenarioNodeId, true) +: remainingScenarioNodeIds,
                  )

              }
            // keep looping
            go(nextState)
        }
      }

      // GO GO GO
      // walk from the roots, return the updated contract id mapping
      go(
        TestScenarioState(
          remainingLedgerEventIds = FrontStack(ledgerEvents.roots),
          remainingScenarioNodeIds = FrontStack(scenarioTransaction.roots.map(StackNode(_, false))),
          scenarioCoidToLedgerCoid = scenarioToLedgerMap
        ))
    }
    scenarioLedger.scenarioSteps.toList
      .foldLeft(Future(Map.empty[AbsoluteContractId, AbsoluteContractId])) {
        case (initialMap, (stepId, L.Commit(txId @ _, richTransaction, optLocation @ _))) =>
          richTransaction.roots.foldLeft(initialMap) {
            case (previousMap, nodeId) =>
              val reference: String =
                commandIdMangler(scenario, stepId, nodeId)
              def submitCommandCheckAndUpdateMap(
                  submitterName: Party,
                  cmd: Command,
                  scenarioToLedgerCoidMap: Map[AbsoluteContractId, AbsoluteContractId])
                : Future[Map[AbsoluteContractId, AbsoluteContractId]] = {
                for {
                  currentTime <- ledger.currentTime
                  events <- ledger.submit(
                    partyNameUnmangler(submitterName.toString),
                    submitterName,
                    Commands(submitterName, ImmArray(cmd), currentTime, reference),
                    opDescription = s"scenario ${scenario} step ${stepId} node ${nodeId}"
                  )
                } yield
                  checkEvents(
                    reference,
                    richTransaction.explicitDisclosure,
                    GenTransaction(richTransaction.nodes, ImmArray(nodeId), Set.empty),
                    events,
                    scenarioToLedgerCoidMap)
              }

              val node = richTransaction.nodes(nodeId)

              node match {
                case nc: NodeCreate.WithTxValue[AbsoluteContractId] =>
                  previousMap.flatMap(m => {
                    val engineArg =
                      nc.coinst.arg.mapContractId(m)
                    val cmd = CreateCommand(nc.coinst.template, engineArg)
                    submitCommandCheckAndUpdateMap(richTransaction.committer, cmd, m)
                  })

                case ne: NodeExercises.WithTxValue[L.ScenarioNodeId, AbsoluteContractId] =>
                  previousMap.flatMap(m => {

                    val engineTargetCoid = m(ne.targetCoid)
                    val engineChosenValue =
                      ne.chosenValue.mapContractId(m)
                    val cmd = ExerciseCommand(
                      ne.templateId,
                      engineTargetCoid.coid,
                      ne.choiceId,
                      engineChosenValue)
                    submitCommandCheckAndUpdateMap(richTransaction.committer, cmd, m)
                  })

                case _: NodeFetch[_] | _: NodeLookupByKey[_, _] =>
                  // nothing to do for fetches or lookup by key
                  previousMap
              }
          }

        case (initialMap, (stepId @ _, L.PassTime(dtMicros))) =>
          initialMap.flatMap(x => ledger.passTime(dtMicros).map(_ => x))

        case (initialMap, (stepId @ _, _: L.AssertMustFail)) =>
          // TODO what can we do here? we do not have the transaction to compare.
          initialMap
      }
      .map(_ => ())

  }

  def testAllScenarios(): Future[Unit] =
    Future
      .sequence(allScenarioLedgers.map {
        case (scenarioName, _) => testScenario(scenarioName)
      })
      .map(_ => ())

}

object SemanticTester {
  case class SemanticTesterError(reference: String, msg: String)
      extends RuntimeException(s"Error in $reference: $msg", null, true, false)

  // Keep in sync with scenario extracting code in SemanticTester class
  def scenarios(packages: Map[PackageId, Package]) = packages.map {
    case (pkgId, lfPackage) =>
      pkgId -> lfPackage.modules.flatMap {
        case (moduleName, module) =>
          module.definitions.collect {
            case (k, d: DValue) if d.isTest => QualifiedName(moduleName, k)
          }
      }
  }

  trait GenericLedger {
    type EventNodeId

    // create commands deliberately do NOT contain submitter,
    // but in general for tests we should pass in the submitter for the commands
    def submit(submitterText: String, submitterName: Party, cmds: Commands, opDescription: String)
      : Future[Events[EventNodeId, AbsoluteContractId, Tx.Value[AbsoluteContractId]]]

    def passTime(dtMicros: Long): Future[Unit]

    def currentTime: Future[Time.Timestamp]
  }

  // a GenericLedger on top of Engine
  class EngineLedger(packages: Map[PackageId, Package])(implicit ec: ExecutionContext)
      extends GenericLedger {
    private val engine = Engine()

    // data we need to store to drive execution
    var ledgerTime = Time.Timestamp.Epoch
    // TODO(Leo): replace with com.digitalasset.daml.lf.engine.InMemoryPrivateContractStore
    val pcs = scala.collection.mutable
      .Map[AbsoluteContractId, ContractInst[Tx.Value[AbsoluteContractId]]]()
    var submitCounter = 0 // to generate absolute contract ids

    type EventNodeId = Tx.NodeId

    // eating up results
    private[this] def consumeResult[A](reference: String, res: Result[A]): A = {
      res.consume(pcs.get, packages.get, { _ =>
        sys.error(s"TODO contract keys + semantic tests")
      }) match {
        case Left(err) =>
          throw SemanticTesterError(reference, s"Error when consuming result: ${err.toString}")
        case Right(x) => x
      }
    }

    // TODO: this function defined twice: in SemanticTester and in EngineTest
    private[this] def makeAbsoluteContractId(coid: ContractId): AbsoluteContractId =
      coid match {
        case rcoid: RelativeContractId =>
          AbsoluteContractId(
            Ref.ContractIdString.assertFromString(
              submitCounter.toString + "-" + rcoid.txnid.index.toString))
        case acoid: AbsoluteContractId => acoid
      }

    // TODO: this function defined twice: in SemanticTester and in EngineTest
    private[this] def makeValueWithAbsoluteContractId(
        v: Tx.Value[ContractId]): Tx.Value[AbsoluteContractId] =
      v.mapContractId(makeAbsoluteContractId)

    private[this] def updatePcs(
        tx: GenTransaction.WithTxValue[Tx.NodeId, AbsoluteContractId]): Unit = {
      // traverse in topo order and add / remove
      @tailrec
      def go(remaining: FrontStack[Tx.NodeId]): Unit = remaining match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, nodeIds) =>
          val node = tx.nodes(nodeId)
          node match {
            case _: NodeFetch[_] | _: NodeLookupByKey[_, _] =>
              go(nodeIds)
            case nc: NodeCreate.WithTxValue[AbsoluteContractId] =>
              pcs += (nc.coid -> nc.coinst)
              go(nodeIds)
            case ne: NodeExercises.WithTxValue[Tx.NodeId, AbsoluteContractId] =>
              // Note: leaking some memory here; we cannot remove consumed contracts,
              // because later post-commit validation needs to find it
              go(ne.children ++: nodeIds)
          }
      }
      go(FrontStack(tx.roots))
    }

    override def submit(
        submitterText: String,
        submitterName: Party,
        cmds: Commands,
        opDescription: String)
      : Future[Events[Tx.NodeId, AbsoluteContractId, Tx.Value[AbsoluteContractId]]] = Future {
      assert(
        cmds.submitter == submitterName,
        s"submitter and the commands submitter don't match: $submitterName, ${cmds.submitter}")
      val tx = consumeResult(cmds.commandsReference, engine.submit(cmds))
      val blindingInfo =
        Blinding
          .checkAuthorizationAndBlind(tx, Set(submitterName))
          .toOption
          .getOrElse(sys.error(s"authorization failed for ${cmds.commandsReference}"))
      val absTx = tx.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId)

      updatePcs(absTx)

      val evts = Event.collectEvents(absTx, blindingInfo.explicitDisclosure)
      submitCounter += 1
      evts
    }

    override def passTime(dtMicros: Long): Future[Unit] =
      Future { ledgerTime = ledgerTime.addMicros(dtMicros) }

    override def currentTime: Future[Time.Timestamp] = Future { ledgerTime }
  }
}
