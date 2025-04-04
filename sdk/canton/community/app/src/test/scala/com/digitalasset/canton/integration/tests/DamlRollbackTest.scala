// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.javaapi.data.Identifier
import com.digitalasset.canton.ComparesLfTransactions.{TxTree, buildLfTransaction}
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.DamlRollbackTest.TbContext
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ComparesLfTransactions, LfPackageName, LfPackageVersion}
import com.digitalasset.daml.lf.data.{FrontStack, Ref}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.{
  CreateKey,
  CreateTransactionVersion,
}
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.value.Value as LfValue
import monocle.Monocle.toAppliedFocusOps

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.*

@nowarn("msg=match may not be exhaustive")
trait DamlRollbackTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with ComparesLfTransactions {

  import TransactionBuilder.Implicits.*

  def cantonTestsPath: String

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.updateTestingConfig(
      _.focus(_.enableInMemoryTransactionStoreForParticipants).replace(true)
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var initialized = false

  private val Seq(aliceS, bobS, carolS) = Seq("Alice", "Bob", "Carol")

  // Placing this helper in every test enables running any subset of tests in any order and provides syntactic sugar
  // providing parties Alice and Bob to every test.
  protected def withParticipantInitialized[A](
      test: (PartyId, PartyId, PartyId) => A
  )(implicit env: TestConsoleEnvironment): A = {
    import env.*

    if (!initialized) {
      Seq(participant1, participant2).foreach { p =>
        p.synchronizers.connect_local(sequencer1, alias = daName)
        p.dars.upload(cantonTestsPath)
      }
      eventually() {
        forAll(synchronizerOwners1) {
          _.health.initialized() shouldBe true
        }
        forAll(Seq(participant1, participant2)) {
          _.synchronizers.list_connected().map(_.synchronizerAlias).loneElement shouldBe daName
        }
      }
      participant1.parties.enable(aliceS)
      participant1.parties.enable(bobS)
      participant2.parties.enable(carolS)
      initialized = true
    }

    val Seq(alice, bob, carol) =
      Seq((aliceS, participant1), (bobS, participant1), (carolS, participant2)).map {
        case (party, participant) => party.toPartyId(participant)
      }
    test(alice, bob, carol)
  }

  /** Helper to initialize an lf transaction builder along with a preorder sequence of contract ids
    * of a provided lf transaction.
    */
  protected def txBuilderContextFrom[A](tx: LfVersionedTransaction)(code: TbContext => A): A =
    code(
      TbContext(
        txVersion = CreateTransactionVersion.Version(tx.version),
        contractIds = contractIdsInPreorder(tx),
      )
    )

  /** Helper to extract the contract-ids of a transaction in pre-order.
    *
    * Useful to build expected transaction in terms of "expect the second contract id of the actual
    * transaction in this create node".
    */
  private def contractIdsInPreorder(tx: LfVersionedTransaction): Seq[LfContractId] = {

    val contractIds = scala.collection.mutable.ListBuffer.empty[LfContractId]

    def add(coid: LfContractId): Unit = if (!contractIds.contains(coid)) contractIds += coid

    tx.foldInExecutionOrder(())(
      exerciseBegin = (_, _, en) => (add(en.targetCoid), LfTransaction.ChildrenRecursion.DoRecurse),
      rollbackBegin = (_, _, _) => ((), LfTransaction.ChildrenRecursion.DoRecurse),
      leaf = {
        case (_, _, cn: LfNodeCreate) => add(cn.coid)
        case (_, _, fn: LfNodeFetch) => add(fn.coid)
        case (_, _, ln: LfNodeLookupByKey) => ln.result.foreach(add)
      },
      exerciseEnd = (_, _, _) => (),
      rollbackEnd = (_, _, _) => (),
    )

    contractIds.result()
  }

  protected def lookUpTransactionById(
      updateId: String
  )(participant: LocalParticipantReference): LfVersionedTransaction =
    eventually() {
      participant.testing
        .lookup_transaction(updateId)
        .getOrElse(fail())
    }

  protected def exercise(
      contract: LfNodeCreate,
      choice: String,
      actors: Set[PartyId],
      arg: LfValue = args(),
      consuming: Boolean = false,
      failed: Boolean = false,
      choiceObservers: Set[PartyId] = Set.empty,
      result: LfValue = LfValue.ValueUnit,
      byKey: Boolean = false,
  ): Node.Exercise =
    TestNodeBuilder.exercise(
      contract = contract,
      choice = choice,
      consuming = consuming,
      actingParties = actors.map(_.toLf),
      argument = arg,
      result = if (!failed) Some(result) else None,
      choiceObservers = choiceObservers.map(_.toLf),
      byKey = byKey,
    )

}

object DamlRollbackTest {
  final case class TbContext(txVersion: CreateTransactionVersion, contractIds: Seq[LfContractId])
}

trait DamlRollbackTestStableLf extends DamlRollbackTest {
  import com.digitalasset.canton.damltests.java.exceptionstester.ExceptionsTester
  import com.digitalasset.canton.damltests.java.exceptionstester

  override def cantonTestsPath: String = CantonTestsPath

  private def createExceptionsTester(
      signatory: PartyId,
      participant: ParticipantReference,
  ): ExceptionsTester.ContractId = {
    val createCmd =
      new exceptionstester.ExceptionsTester(signatory.toProtoPrimitive).create
        .commands()
        .asScala
        .toSeq

    val createTx = participant.ledger_api.javaapi.commands.submit_flat(
      Seq(signatory),
      createCmd,
    )
    JavaDecodeUtil
      .decodeAllCreated(exceptionstester.ExceptionsTester.COMPANION)(createTx)
      .loneElement
      .id
  }

  private def createInformeesContract(
      signatories: List[PartyId],
      observers: List[PartyId],
      participant: ParticipantReference,
  ): exceptionstester.Informees.ContractId = {
    val createCmd =
      new exceptionstester.Informees(
        signatories.map(_.toProtoPrimitive).asJava,
        observers.map(_.toProtoPrimitive).asJava,
      ).create.commands.asScala.toSeq

    val createTx = participant.ledger_api.javaapi.commands.submit_flat(
      signatories,
      createCmd,
    )
    JavaDecodeUtil
      .decodeAllCreated(exceptionstester.Informees.COMPANION)(createTx)
      .loneElement
      .id
  }

  private def signOn(
      informees: exceptionstester.Informees.ContractId,
      observer: PartyId,
      participant: ParticipantReference,
  ): exceptionstester.Informees.ContractId = {
    val cmdSignOn = informees.exerciseSignOn(observer.toProtoPrimitive).commands.asScala.toSeq
    val createTx =
      participant.ledger_api.javaapi.commands.submit_flat(Seq(observer), cmdSignOn)
    JavaDecodeUtil
      .decodeAllCreated(exceptionstester.Informees.COMPANION)(createTx)
      .loneElement
      .id
  }

  private def create[T](
      id: Int,
      template: Identifier,
      sig: Seq[PartyId],
      obs: Seq[PartyId] = Seq.empty, // additional observers in addition to signatories
      arg: LfValue = notUsed,
  )(implicit tbCtx: TbContext): LfNodeCreate = {
    val signatories = sig.map(_.toLf)
    val observers = signatories ++ obs.map(_.toLf)

    val templateId = templateIdFromIdentifier(template)

    TestNodeBuilder.create(
      id = tbCtx.contractIds(id),
      templateId = templateId,
      argument = template match {
        case exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID =>
          require(
            arg == notUsed,
            "For informees, this function figures out the sig and obs parameters by itself",
          )
          args(
            seq(signatories.map(LfValue.ValueParty.apply)*),
            seq(observers.map(LfValue.ValueParty.apply)*),
          )
        case _ => arg
      },
      signatories = signatories.toSet,
      observers = observers.toSet,
      key = CreateKey.NoKey,
      version = tbCtx.txVersion,
      packageName = LfPackageName.assertFromString("CantonTests"),
      packageVersion = Some(LfPackageVersion.assertFromString("3.1.0")),
    )
  }

  "Able to submit command with simple create rollback" in { implicit env =>
    withParticipantInitialized { (alice, _, _) =>
      import env.*

      val et = createExceptionsTester(alice, participant1)

      val txActual = lookUpTransactionById(
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            et.exerciseSimpleRollbackCreate().commands.asScala.toSeq,
          )
          .getUpdateId
      )(participant1)

      txBuilderContextFrom(txActual) { implicit tbCtx =>
        val txExpected = TxTree(
          exercise(
            contract = create(
              0,
              exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
              sig = Seq(alice),
            ),
            choice = "SimpleRollbackCreate",
            actors = Set(alice),
          ),
          TxTree(
            TestNodeBuilder.rollback(),
            TxTree(
              create(1, exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID, sig = Seq(alice))
            ),
          ),
        ).lfTransaction

        assertTransactionsMatch(txExpected, txActual)
      }
    }
  }

  "Able to submit command with exception hierarchy" in { implicit env =>
    withParticipantInitialized { (alice, _, _) =>
      import env.*

      val et = createExceptionsTester(alice, participant1)

      val txActual = lookUpTransactionById(
        participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), et.exerciseRollbackCreate().commands.asScala.toSeq)
          .getUpdateId
      )(participant1)

      txBuilderContextFrom(txActual) { implicit tbCtx =>
        val txExpected =
          TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "RollbackCreate",
              actors = Set(alice),
            ),
            TxTree(
              TestNodeBuilder.rollback(),
              TxTree(
                create(1, exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID, sig = Seq(alice))
              ),
            ),
            TxTree(
              TestNodeBuilder.rollback(),
              TxTree(
                create(2, exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID, sig = Seq(alice))
              ),
              TxTree(
                TestNodeBuilder.rollback(),
                TxTree(
                  create(
                    3,
                    exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice),
                  )
                ),
              ),
              TxTree(
                create(4, exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID, sig = Seq(alice))
              ),
            ),
          ).lfTransaction

        assertTransactionsMatch(txExpected, txActual)
      }
    }
  }

  "Able to submit command with new view under rollback node with all parties local" in {
    implicit env =>
      withParticipantInitialized { (alice, bob, _) =>
        import env.*

        val informees = createInformeesContract(List(alice), List(bob), participant1)
        val et = createExceptionsTester(alice, participant1)

        val txActual = lookUpTransactionById(
          participant1.ledger_api.javaapi.commands
            .submit(
              Seq(alice),
              et.exerciseRollbackExercise(informees).commands.asScala.toSeq,
            )
            .getUpdateId
        )(participant1)

        txBuilderContextFrom(txActual) { implicit tbCtx =>
          val txExpected = TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "RollbackExercise",
              actors = Set(alice),
              arg = args(LfValue.ValueContractId(tbCtx.contractIds(1))),
            ),
            TxTree(
              TestNodeBuilder.rollback(),
              TxTree(
                TestNodeBuilder.fetch(
                  contract = create(
                    1,
                    exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice),
                    obs = Seq(bob),
                  ),
                  byKey = false,
                )
              ),
              TxTree(
                exercise(
                  contract = create(
                    1,
                    exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice),
                    obs = Seq(bob),
                  ),
                  choice = "Close",
                  actors = Set(alice),
                  arg = args(LfValue.ValueParty(alice.toLf)),
                  consuming = true,
                )
              ),
              TxTree(
                create(2, exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID, sig = Seq(alice))
              ),
              TxTree(
                exercise(
                  contract = create(
                    2,
                    exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice),
                  ),
                  choice = "Close",
                  actors = Set(alice),
                  arg = args(LfValue.ValueParty(alice.toLf)),
                  consuming = true,
                )
              ),
            ),
          ).lfTransaction

          assertTransactionsMatch(txExpected, txActual)
        }
      }
  }

  "Able to submit command with new view under rollback node with remote party" in { implicit env =>
    withParticipantInitialized { (alice, _, carol) =>
      import env.*

      val informees = createInformeesContract(List(alice), List(carol), participant1)
      val informeesSignedOn = signOn(informees, carol, participant2)

      val et = createExceptionsTester(alice, participant1)

      val txId = participant1.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          et.exerciseRollbackExercise(informeesSignedOn).commands.asScala.toSeq,
        )
        .getUpdateId

      val txActualP1 = lookUpTransactionById(txId)(participant1)
      val txActualP2 = lookUpTransactionById(txId)(participant2)

      val txExpectedP2Nested = txBuilderContextFrom(txActualP2) { implicit tbCtx =>
        val txExpectedP2Nested = TxTree(
          TestNodeBuilder.rollback(),
          TxTree(
            TestNodeBuilder
              .fetch(
                contract = create(
                  0,
                  exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice, carol),
                ),
                byKey = false,
              )
              .copy(actingParties = Set(alice.toLf))
          ),
          TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice, carol),
              ),
              choice = "Close",
              actors = Set(alice),
              arg = args(LfValue.ValueParty(alice.toLf)),
              consuming = true,
            )
          ),
          TxTree(
            create(
              1,
              exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
              sig = Seq(alice),
              obs = Seq(carol),
            )
          ),
          TxTree(
            exercise(
              contract = create(
                1,
                exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
                obs = Seq(carol),
              ),
              choice = "Close",
              actors = Set(alice),
              arg = args(LfValue.ValueParty(alice.toLf)),
              consuming = true,
            )
          ),
        )

        assertTransactionsMatch(txExpectedP2Nested.lfTransaction, txActualP2)

        txExpectedP2Nested // Return the portion of the transaction shared among P1 and P2
      }

      // Participant1 sees the top-level exercise as well in addition to the entire transaction seen by participant2
      txBuilderContextFrom(txActualP1) { implicit tbCtx =>
        val txExpectedP1 = TxTree(
          exercise(
            contract = create(
              0,
              exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
              sig = Seq(alice),
            ),
            choice = "RollbackExercise",
            actors = Set(alice),
            arg = args(LfValue.ValueContractId(tbCtx.contractIds(1))),
          ),
          txExpectedP2Nested,
        ).lfTransaction

        assertTransactionsMatch(txExpectedP1, txActualP1)
      }
    }

  }

  "Able to submit command with new view under rollback node with remote party and nested throw" in {
    implicit env =>
      withParticipantInitialized { (alice, _, carol) =>
        import env.*

        val informees = createInformeesContract(List(alice), List(carol), participant1)
        val informeesSignedOn = signOn(informees, carol, participant2)

        val et = createExceptionsTester(alice, participant1)

        val txId = participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            et.exerciseRollbackExerciseWithNestedThrow(informeesSignedOn).commands.asScala.toSeq,
          )
          .getUpdateId

        val txActualP1 = lookUpTransactionById(txId)(participant1)
        val txActualP2 = lookUpTransactionById(txId)(participant2)

        val txExpectedP2Nested = txBuilderContextFrom(txActualP2) { implicit tbCtx =>
          val txExpectedP2Nested = TxTree(
            TestNodeBuilder.rollback(),
            TxTree(
              TestNodeBuilder
                .fetch(
                  contract = create(
                    0,
                    exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice, carol),
                  ),
                  byKey = false,
                )
                .copy(actingParties = Set(alice.toLf))
            ),
            TxTree(
              create(
                1,
                exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
                obs = Seq(carol),
              )
            ),
            TxTree(
              exercise(
                contract = create(
                  1,
                  exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice),
                  obs = Seq(carol),
                ),
                choice = "Close",
                actors = Set(alice),
                arg = args(LfValue.ValueParty(alice.toLf)),
                consuming = true,
              )
            ),
            TxTree(
              exercise(
                contract = create(
                  0,
                  exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice, carol),
                ),
                choice = "Throw",
                actors = Set(alice),
                arg = args(LfValue.ValueParty(alice.toLf)),
                consuming = true,
                failed = true,
              )
            ),
          )

          assertTransactionsMatch(txExpectedP2Nested.lfTransaction, txActualP2)

          txExpectedP2Nested // Return the portion of the transaction shared among P1 and P2
        }

        // Participant1 sees the top-level exercise as well in addition to the entire transaction seen by participant2
        txBuilderContextFrom(txActualP1) { implicit tbCtx =>
          val txExpectedP1 = TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "RollbackExerciseWithNestedThrow",
              actors = Set(alice),
              arg = args(LfValue.ValueContractId(tbCtx.contractIds(1))),
            ),
            txExpectedP2Nested,
          ).lfTransaction

          assertTransactionsMatch(txExpectedP1, txActualP1)
        }
      }
  }
}

trait DamlRollbackTestDevLf extends DamlRollbackTest {
  import com.digitalasset.canton.damltestsdev.java.exceptionstester.ExceptionsTester
  import com.digitalasset.canton.damltestsdev.java.{exceptionstester, simplekeys}

  override def cantonTestsPath: String = CantonTestsDevPath

  private def createExceptionsTester(
      signatory: PartyId,
      participant: ParticipantReference,
  ): ExceptionsTester.ContractId = {
    val createCmd =
      new exceptionstester.ExceptionsTester(signatory.toProtoPrimitive).create
        .commands()
        .asScala
        .toSeq

    val createTx = participant.ledger_api.javaapi.commands.submit_flat(
      Seq(signatory),
      createCmd,
    )
    JavaDecodeUtil
      .decodeAllCreated(exceptionstester.ExceptionsTester.COMPANION)(createTx)
      .loneElement
      .id
  }

  private def create[T](
      id: Int,
      template: Identifier,
      sig: Seq[PartyId],
      obs: Seq[PartyId] = Seq.empty, // additional observers in addition to signatories
      arg: LfValue = notUsed,
  )(implicit tbCtx: TbContext): LfNodeCreate = {
    val signatories = sig.map(_.toLf)
    val observers = signatories ++ obs.map(_.toLf)

    val templateId = templateIdFromIdentifier(template)

    TestNodeBuilder.create(
      id = tbCtx.contractIds(id),
      templateId = templateId,
      argument = template match {
        case exceptionstester.Informees.TEMPLATE_ID_WITH_PACKAGE_ID =>
          require(
            arg == notUsed,
            "For informees, this function figures out the sig and obs parameters by itself",
          )
          args(
            seq(signatories.map(LfValue.ValueParty.apply)*),
            seq(observers.map(LfValue.ValueParty.apply)*),
          )
        case _ => arg
      },
      signatories = signatories.toSet,
      observers = observers.toSet,
      key = CreateKey.NoKey,
      version = tbCtx.txVersion,
      packageName = LfPackageName.assertFromString("CantonTestsDev"),
      packageVersion = Some(
        Ref.PackageVersion.assertFromString(exceptionstester.Informees.PACKAGE_VERSION.toString)
      ),
    )
  }

  private def createWithKey[T](
      id: Int,
      template: Identifier,
      party: PartyId,
      observers: Seq[PartyId] = Seq.empty,
  )(implicit tbCtx: TbContext): LfNodeCreate = {
    val lfParty = party.toLf
    val lfObservers = observers.map(_.toLf)
    val templateId = templateIdFromIdentifier(template)

    TestNodeBuilder.create(
      id = tbCtx.contractIds(id),
      templateId = templateId,
      argument = args(
        LfValue.ValueParty(lfParty),
        LfValue.ValueList(FrontStack.from(lfObservers.map(LfValue.ValueParty.apply))),
      ),
      signatories = Set(lfParty),
      observers = Set(lfParty) ++ lfObservers,
      key = CreateKey.SignatoryMaintainerKey(LfValue.ValueParty(lfParty)),
      version = tbCtx.txVersion,
      packageName = LfPackageName.assertFromString("CantonTestsDev"),
      packageVersion = Some(LfPackageVersion.assertFromString("3.3.0")),
    )
  }

  if (testedProtocolVersion == ProtocolVersion.dev) {

    "Able to fetch a contract after a rolled back archival" in { implicit env =>
      withParticipantInitialized { (alice, _, _) =>
        import env.*

        val et = createExceptionsTester(alice, participant1)

        val txActual = lookUpTransactionById(
          participant1.ledger_api.javaapi.commands
            .submit(
              Seq(alice),
              et.exerciseTestArchivalUnderRollbackKeepsContractActive().commands.asScala.toSeq,
            )
            .getUpdateId
        )(participant1)

        txBuilderContextFrom(txActual) { implicit tbCtx =>
          val keyContractCreate =
            createWithKey(1, simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID, party = alice)
          val txExpected = TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "TestArchivalUnderRollbackKeepsContractActive",
              actors = Set(alice),
            ),
            TxTree(keyContractCreate),
            TxTree(
              TestNodeBuilder.rollback(),
              TxTree(
                exercise(
                  contract = keyContractCreate,
                  choice = "Archive",
                  actors = Set(alice),
                  consuming = true,
                  byKey = true,
                )
              ),
            ),
            TxTree(TestNodeBuilder.fetch(contract = keyContractCreate, byKey = true)),
          ).lfTransaction

          assertTransactionsMatch(txExpected, txActual)
        }
      }
    }

    "Able to create and exercise a contract with key within the same rollback" in { implicit env =>
      withParticipantInitialized { (alice, _, _) =>
        import env.*

        val et = createExceptionsTester(alice, participant1)

        val txActual = lookUpTransactionById(
          participant1.ledger_api.javaapi.commands
            .submit(
              Seq(alice),
              et.exerciseTestRollbackCreateAndExerciseByKey().commands.asScala.toSeq,
            )
            .getUpdateId
        )(participant1)

        txBuilderContextFrom(txActual) { implicit tbCtx =>
          val keyContractCreate =
            createWithKey(1, simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID, party = alice)
          val txExpected = TxTree(
            exercise(
              contract = create(
                0,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "TestRollbackCreateAndExerciseByKey",
              actors = Set(alice),
            ),
            TxTree(
              TestNodeBuilder.rollback(),
              TxTree(keyContractCreate),
              TxTree(
                exercise(
                  contract = keyContractCreate,
                  choice = "Archive",
                  actors = Set(alice),
                  consuming = true,
                  byKey = true,
                )
              ),
            ),
          ).lfTransaction

          assertTransactionsMatch(txExpected, txActual)
        }
      }
    }

    "Able to create and exercise a contract with key within rollback and nested rollback archival" in {
      implicit env =>
        withParticipantInitialized { (alice, _, _) =>
          import env.*

          val et = createExceptionsTester(alice, participant1)

          val txActual = lookUpTransactionById(
            participant1.ledger_api.javaapi.commands
              .submit(
                Seq(alice),
                et.exerciseTestRollbackCreateNestedRolledBackedArchivalAndExerciseByKey(
                ).commands
                  .asScala
                  .toSeq,
              )
              .getUpdateId
          )(participant1)

          txBuilderContextFrom(txActual) { implicit tbCtx =>
            val keyContractCreate =
              createWithKey(1, simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID, party = alice)
            val keyContractExercise = exercise(
              contract = keyContractCreate,
              choice = "Archive",
              actors = Set(alice),
              consuming = true,
              byKey = true,
            )
            val txExpected = TxTree(
              exercise(
                contract = create(
                  0,
                  exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice),
                ),
                choice = "TestRollbackCreateNestedRolledBackedArchivalAndExerciseByKey",
                actors = Set(alice),
              ),
              TxTree(
                TestNodeBuilder.rollback(),
                TxTree(keyContractCreate),
                TxTree(
                  TestNodeBuilder.rollback(),
                  TxTree(keyContractExercise),
                ),
                TxTree(keyContractExercise),
              ),
            ).lfTransaction

            assertTransactionsMatch(txExpected, txActual)
          }
        }
    }

    "Able to roll back nested exercise with action nodes both in same and different view" in {
      implicit env =>
        withParticipantInitialized { (alice, _, carol) =>
          import env.*

          val et = createExceptionsTester(alice, participant1)

          val txId = participant1.ledger_api.javaapi.commands
            .submit(
              Seq(alice),
              et.exerciseTestMultiLevelRollbackMultipleViewsOuter(
                carol.toProtoPrimitive
              ).commands
                .asScala
                .toSeq,
            )
            .getUpdateId

          val txActualP1 = lookUpTransactionById(txId)(participant1)
          val txActualP2 = lookUpTransactionById(txId)(participant2)

          val (subTxTree1, subTxTree2, subTxTree3) = txBuilderContextFrom(txActualP2) {
            implicit tbCtx =>
              val outerKey =
                createWithKey(
                  0,
                  simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                  party = alice,
                  observers = Seq(carol),
                )
              val innerKeyFirst =
                createWithKey(
                  2,
                  simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                  party = alice,
                  observers = Seq(),
                )
              val innerKeySecond =
                createWithKey(
                  3,
                  simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                  party = alice,
                  observers = Seq(carol),
                )

              val subTxTree1 = TxTree(outerKey)
              val subTxTree2 = TxTree(
                TestNodeBuilder.rollback(),
                TxTree(
                  exercise(
                    contract = create(
                      1, // Note that participant2 sees ExceptionsTester after outerKey; hence index is 1 not 0
                      exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                      sig = Seq(alice),
                    ),
                    choice = "TestMultiLevelRollbackMultipleViewsInner",
                    actors = Set(alice),
                    failed = true,
                    choiceObservers = Set(carol),
                    arg = args(LfValue.ValueParty(carol.toLf)),
                  ),
                  TxTree(
                    exercise(
                      contract = outerKey,
                      choice = "Archive",
                      actors = Set(alice),
                      consuming = true,
                      byKey = true,
                    )
                  ),
                  TxTree(
                    TestNodeBuilder.rollback(),
                    TxTree(innerKeyFirst),
                    TxTree(
                      exercise(
                        contract = innerKeyFirst,
                        choice = "FetchWithObservers",
                        actors = Set(alice),
                        choiceObservers = Set(carol),
                        arg = args(LfValue.ValueParty(carol.toLf)),
                        result =
                          args(LfValue.ValueParty(alice.toLf), LfValue.ValueList(FrontStack.empty)),
                        byKey = true,
                      )
                    ),
                  ),
                  TxTree(innerKeySecond),
                  TxTree(TestNodeBuilder.fetch(contract = innerKeySecond, byKey = true)),
                ),
              )
              val subTxTree3 = TxTree(
                exercise(
                  contract = outerKey,
                  choice = "Archive",
                  actors = Set(alice),
                  consuming = true,
                )
              )

              val txExpectedP2 = buildLfTransaction(subTxTree1, subTxTree2, subTxTree3)

              // If this test fails and the diff is truncated, temporarily increase Pretty.DefaultHeight to 200.
              assertTransactionsMatch(txExpectedP2, txActualP2)

              // Return sub-transaction-trees for reuse
              (subTxTree1, subTxTree2, subTxTree3)
          }
          txBuilderContextFrom(txActualP1) { implicit tbCtx =>
            val txExpectedP1 = TxTree(
              exercise(
                contract = create(
                  0,
                  exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice),
                ),
                choice = "TestMultiLevelRollbackMultipleViewsOuter",
                actors = Set(alice),
                arg = args(LfValue.ValueParty(carol.toLf)),
              ),
              subTxTree1,
              subTxTree2,
              subTxTree3,
            ).lfTransaction

            assertTransactionsMatch(txExpectedP1, txActualP1)
          }
        }
    }

    "Able to create a contract key after a rolled back same contract key create" in {
      implicit env =>
        withParticipantInitialized { (alice, _, _) =>
          import env.*

          val et = createExceptionsTester(alice, participant1)

          val txActual = lookUpTransactionById(
            participant1.ledger_api.javaapi.commands
              .submit(
                Seq(alice),
                et.exerciseTestCreateKeyUnderRollbackAllowsKeyRecreate().commands.asScala.toSeq,
              )
              .getUpdateId
          )(participant1)

          txBuilderContextFrom(txActual) { implicit tbCtx =>
            val txExpected = TxTree(
              exercise(
                contract = create(
                  0,
                  exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                  sig = Seq(alice),
                ),
                choice = "TestCreateKeyUnderRollbackAllowsKeyRecreate",
                actors = Set(alice),
              ),
              TxTree(
                TestNodeBuilder.rollback(),
                TxTree(
                  createWithKey(1, simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID, party = alice)
                ),
              ),
              TxTree(
                createWithKey(2, simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID, party = alice)
              ),
            ).lfTransaction

            assertTransactionsMatch(txExpected, txActual)
          }
        }
    }

    "Able to roll back create in multi-level rollback at correct level" in { implicit env =>
      withParticipantInitialized { (alice, _, carol) =>
        import env.*

        val et = createExceptionsTester(alice, participant1)

        val txId = participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            // Add a separate create node with both stakeholders that does not get rolled back. This ensures
            // that LedgerApiAdministration.involvedParticipants (that only sees ledger api events and thus does not
            // include waiting for participants only involved via rolled-back activity) also waits for Carol's participant2
            // thus avoiding flaky test failures.
            new simplekeys.SimpleKey(
              alice.toProtoPrimitive,
              List(carol.toProtoPrimitive).asJava,
            ).create.commands().asScala.toSeq
              ++ et
                .exerciseTestMultiLevelRollbackCreateSingleInformee(
                  carol.toProtoPrimitive
                )
                .commands()
                .asScala
                .toSeq,
          )
          .getUpdateId

        val txActualP1 = lookUpTransactionById(txId)(participant1)
        val txActualP2 = lookUpTransactionById(txId)(participant2)

        val (command1TxTree, command2TxSubTree) = txBuilderContextFrom(txActualP2) {
          implicit tbCtx =>
            val command1TxTree =
              TxTree(
                createWithKey(
                  0,
                  simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                  party = alice,
                  observers = Seq(carol),
                )
              )

            val command2TxTree = TxTree(
              TestNodeBuilder.rollback(),
              TxTree(
                exercise(
                  contract = create(
                    1,
                    exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                    sig = Seq(alice),
                  ),
                  choice = "TestMultiLevelRollbackCreateMultiInformees",
                  actors = Set(alice),
                  failed = true,
                  choiceObservers = Set(carol),
                  arg = args(LfValue.ValueParty(carol.toLf)),
                ),
                TxTree(
                  TestNodeBuilder.rollback(),
                  TxTree(
                    createWithKey(
                      2,
                      simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                      party = alice,
                      observers = Seq(carol),
                    )
                  ),
                ),
                TxTree(
                  createWithKey(
                    3,
                    simplekeys.SimpleKey.TEMPLATE_ID_WITH_PACKAGE_ID,
                    party = alice,
                    observers = Seq(carol),
                  )
                ),
              ),
            )

            val txExpectedP2 = buildLfTransaction(command1TxTree, command2TxTree)

            assertTransactionsMatch(txExpectedP2, txActualP2)

            // returns transaction trees for reuse verifying participant1
            (command1TxTree, command2TxTree)
        }

        txBuilderContextFrom(txActualP1) { implicit tbCtx =>
          val exerciseSubTree = TxTree(
            exercise(
              contract = create(
                1,
                exceptionstester.ExceptionsTester.TEMPLATE_ID_WITH_PACKAGE_ID,
                sig = Seq(alice),
              ),
              choice = "TestMultiLevelRollbackCreateSingleInformee",
              actors = Set(alice),
              arg = args(LfValue.ValueParty(carol.toLf)),
            ),
            command2TxSubTree,
          )

          val txExpectedP1 = buildLfTransaction(command1TxTree, exerciseSubTree)

          assertTransactionsMatch(txExpectedP1, txActualP1)

          (command1TxTree, command2TxSubTree)
        }

      }
    }
  }
}

trait DamlRollbackReferenceSequencerPostgresTest {
  self: SharedEnvironment =>
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[Postgres](loggerFactory))
}

trait DamlRollbackBftSequencerPostgresTest {
  self: SharedEnvironment =>
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class DamlRollbackReferenceIntegrationTestPostgresStableLf
    extends DamlRollbackTestStableLf
    with DamlRollbackReferenceSequencerPostgresTest

class DamlRollbackBftOrderingIntegrationTestPostgresStableLf
    extends DamlRollbackTestStableLf
    with DamlRollbackBftSequencerPostgresTest

class DamlRollbackReferenceIntegrationTestPostgresDevLf
    extends DamlRollbackTestDevLf
    with DamlRollbackReferenceSequencerPostgresTest

class DamlRollbackBftOrderingIntegrationTestPostgresDevLf
    extends DamlRollbackTestDevLf
    with DamlRollbackBftSequencerPostgresTest
