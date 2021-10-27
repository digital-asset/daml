// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.util
import java.io.File
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.transaction.{
  ContractKeyUniquenessMode,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  SubmittedTransaction,
  VersionedTransaction,
  GenTransaction => GenTx,
  Transaction => Tx,
  TransactionVersion => TxVersions,
}
import com.daml.lf.transaction.{Normalization, Validation, ReplayMismatch}
import com.daml.lf.value.Value
import Value._
import com.daml.lf.speedy.{InitialSeeding, SValue, svalue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.command._
import com.daml.lf.engine.Error.Interpretation
import com.daml.lf.transaction.Node.{GenActionNode, GenNode}
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import org.scalactic.Equality
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class EngineTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import EngineTest._

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private val (basicTestsPkgId, basicTestsPkg, allPackages) = loadPackage(
    "daml-lf/tests/BasicTests.dar"
  )

  val basicTestsSignatures = language.PackageInterface(Map(basicTestsPkgId -> basicTestsPkg))

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: VersionedContractInstance =
    assertAsVersionedContract(
      ContractInstance(
        TypeConName(basicTestsPkgId, withKeyTemplate),
        ValueRecord(
          Some(BasicTests_WithKey),
          ImmArray(
            (Some[Ref.Name]("p"), ValueParty(alice)),
            (Some[Ref.Name]("k"), ValueInt64(42)),
          ),
        ),
        "",
      )
    )

  val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("BasicTests:Simple:1") ->
        assertAsVersionedContract(
          ContractInstance(
            TypeConName(basicTestsPkgId, "BasicTests:Simple"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
              ImmArray((Some[Name]("p"), ValueParty(party))),
            ),
            "",
          )
        ),
      toContractId("BasicTests:CallablePayout:1") ->
        assertAsVersionedContract(
          ContractInstance(
            TypeConName(basicTestsPkgId, "BasicTests:CallablePayout"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some[Ref.Name]("giver"), ValueParty(alice)),
                (Some[Ref.Name]("receiver"), ValueParty(bob)),
              ),
            ),
            "",
          )
        ),
      toContractId("BasicTests:WithKey:1") ->
        withKeyContractInst,
    )

  val defaultKey = Map(
    GlobalKey.assertBuild(
      TypeConName(basicTestsPkgId, withKeyTemplate),
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
    )
      ->
        toContractId("BasicTests:WithKey:1")
  )

  private[this] val lookupContract = defaultContracts.get(_)

  private[this] def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  private[this] def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
    (key.globalKey.templateId, key.globalKey.key) match {
      case (
            BasicTests_WithKey,
            ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ) =>
        Some(toContractId("BasicTests:WithKey:1"))
      case _ =>
        None
    }

  private[this] val suffixLenientEngine = newEngine()
  private[this] val suffixStrictEngine = newEngine(requireCidSuffixes = true)
  private[this] val preprocessor =
    new preprocessing.Preprocessor(
      ConcurrentCompiledPackages(suffixLenientEngine.config.getCompilerConfig)
    )

  "minimal create command" should {
    val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val let = Time.Timestamp.now()
    val command =
      CreateCommand(id, ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party)))))
    val submissionSeed = hash("minimal create command")
    val submitters = Set(party)
    val readAs = (Set.empty: Set[Party])
    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]
    val interpretResult = suffixLenientEngine
      .submit(
        submitters,
        readAs,
        Commands(ImmArray(command), let, "test"),
        participant,
        submissionSeed,
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      interpretResult shouldBe a[Right[_, _]]
    }

    "reinterpret to the same result" in {
      val Right((tx, txMeta)) = interpretResult
      val stx = suffix(tx)

      val Right((rtx, newMeta)) =
        reinterpret(
          suffixStrictEngine,
          Set(party),
          stx.roots,
          stx,
          txMeta,
          let,
          lookupPackage,
        )
      isReplayedBy(stx, rtx) shouldBe Right(())
      txMeta.nodeSeeds shouldBe newMeta.nodeSeeds
    }

    "be validated" in {
      val Right((tx, meta)) = interpretResult
      val Right(submitter) = tx.guessSubmitter
      val submitters = Set(submitter)
      val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
      val validated = suffixLenientEngine
        .validate(submitters, ntx, let, participant, meta.submissionTime, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.message)
        case Right(()) => ()
      }
    }

    "cause used package to show up in transaction" in {
      // NOTE(JM): Other packages are pulled in by BasicTests.daml, e.g. daml-prim, but we
      // don't know the package ids here.
      interpretResult.map(_._2.usedPackages.contains(basicTestsPkgId)) shouldBe Right(true)
    }

    "not mark any node as byKey" in {
      interpretResult.map { case (tx, _) => byKeyNodes(tx).size } shouldBe Right(0)
    }

  }

  "multi-party create command" should {
    val multiPartyTemplate = "BasicTests:SimpleMultiParty"

    val cases = Table(
      ("templateId", "signatories", "submitters"),
      (multiPartyTemplate, Set("p1" -> alice, "p2" -> bob), Set(alice, bob)),
      (multiPartyTemplate, Set("p1" -> alice, "p2" -> bob), Set(alice, bob, clara)),
    )

    def id(templateId: String) = Identifier(basicTestsPkgId, templateId)
    def command(templateId: String, signatories: Set[(String, Party)]) = {
      val templateArgs: Set[(Some[Name], ValueParty)] = signatories.map { case (label, party) =>
        Some[Name](label) -> ValueParty(party)
      }
      CreateCommand(id(templateId), ValueRecord(Some(id(templateId)), templateArgs.to(ImmArray)))
    }

    val let = Time.Timestamp.now()
    val submissionSeed = hash("multi-party create command2")

    def interpretResult(
        templateId: String,
        signatories: Set[(String, Party)],
        actAs: Set[Party],
    ) = {
      val readAs = (Set.empty: Set[Party])
      val cmd = command(templateId, signatories)
      val res = preprocessor
        .preprocessCommands(ImmArray(cmd))
        .consume(lookupContract, lookupPackage, lookupKey)
      withClue("Preprocessing result: ")(res shouldBe a[Right[_, _]])

      suffixLenientEngine
        .submit(actAs, readAs, Commands(ImmArray(cmd), let, "test"), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
    }

    "be translated" in {
      forAll(cases) { case (templateId, signatories, submitters) =>
        interpretResult(templateId, signatories, submitters) shouldBe a[Right[_, _]]
      }
    }

    "reinterpret to the same result" in {
      forAll(cases) { case (templateId, signatories, submitters) =>
        val Right((tx, txMeta)) = interpretResult(templateId, signatories, submitters)
        val stx = suffix(tx)

        val Right((rtx, _)) =
          reinterpret(
            suffixStrictEngine,
            signatories.map(_._2),
            stx.roots,
            stx,
            txMeta,
            let,
            lookupPackage,
          )
        isReplayedBy(stx, rtx) shouldBe Right(())
      }
    }

    "be validated" in {
      forAll(cases) { case (templateId, signatories, submitters) =>
        val Right((tx, meta)) = interpretResult(templateId, signatories, submitters)
        val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
        val validated = suffixLenientEngine
          .validate(submitters, ntx, let, participant, meta.submissionTime, submissionSeed)
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )
        validated match {
          case Left(e) =>
            fail(e.message)
          case Right(()) => succeed
        }
      }
    }

    "allow replay with a superset of submitters" in {
      forAll(cases) { case (templateId, signatories, submitters) =>
        val Right((tx, _)) = interpretResult(templateId, signatories, submitters)

        val replaySubmitters = submitters + party
        val replayResult = suffixLenientEngine.replay(
          submitters = replaySubmitters,
          tx = tx,
          ledgerEffectiveTime = let,
          participantId = participant,
          submissionTime = let,
          submissionSeed = submissionSeed,
        )

        replayResult shouldBe a[ResultDone[_]]
      }
    }

    "not allow replay with a subset of submitters" in {
      forAll(cases) { case (templateId, signatories, submitters) =>
        val Right((tx, _)) = interpretResult(templateId, signatories, submitters)

        val replaySubmitters = submitters.drop(1)
        val replayResult = suffixLenientEngine.replay(
          submitters = replaySubmitters,
          tx = tx,
          ledgerEffectiveTime = let,
          participantId = participant,
          submissionTime = let,
          submissionSeed = submissionSeed,
        )

        replayResult shouldBe a[ResultError]
      }
    }
  }

  "exercise command" should {
    val submissionSeed = hash("exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid = toContractId("BasicTests:Simple:1")
    val command =
      ExerciseCommand(templateId, cid, "Hello", ValueRecord(Some(hello), ImmArray.Empty))
    val submitters = Set(party)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]
    val interpretResult =
      res
        .flatMap { cmds =>
          suffixLenientEngine
            .interpretCommands(
              validating = false,
              submitters = submitters,
              readAs = readAs,
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = seeding,
            )
            .consume(
              lookupContract,
              lookupPackage,
              lookupKey,
            )
        }
    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      val Right((rtx, _)) = suffixLenientEngine
        .submit(
          Set(party),
          readAs,
          Commands(ImmArray(command), let, "test"),
          participant,
          submissionSeed,
        )
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
      isReplayedBy(tx, rtx) shouldBe Right(())
    }

    "reinterpret to the same result" in {
      val stx = suffix(tx)

      val Right((rtx, _)) =
        reinterpret(
          suffixStrictEngine,
          Set(party),
          stx.roots,
          stx,
          txMeta,
          let,
          lookupPackage,
          defaultContracts,
        )
      isReplayedBy(stx, rtx) shouldBe Right(())
    }

    "be validated" in {
      val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
      val validated = suffixLenientEngine
        .validate(Set(submitter), ntx, let, participant, let, submissionSeed)
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
      validated match {
        case Left(e) =>
          fail(e.message)
        case Right(()) => ()
      }
    }
  }

  "exercise-by-key command with missing key" should {
    val submissionSeed = hash("exercise-by-key command with missing key")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val command = ExerciseByKeyCommand(
      templateId,
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(43)))),
      "SumToK",
      ValueRecord(None, ImmArray((None, ValueInt64(5)))),
    )
    val submitters = Set(alice)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]

    "fail at submission" in {
      val submitResult = suffixLenientEngine
        .submit(
          submitters,
          readAs,
          Commands(ImmArray(command), let, "test"),
          participant,
          submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      inside(submitResult) { case Left(Error.Interpretation(err, _)) =>
        err shouldBe Interpretation.DamlException(
          interpretation.Error.ContractKeyNotFound(
            GlobalKey.assertBuild(
              BasicTests_WithKey,
              ValueRecord(
                Some(BasicTests_WithKey),
                ImmArray(
                  (Some[Ref.Name]("p"), ValueParty(alice)),
                  (Some[Ref.Name]("k"), ValueInt64(43)),
                ),
              ),
            )
          )
        )
      }
    }
  }

  "exercise-by-key command with existing key" should {
    val submissionSeed = hash("exercise-by-key command with existing key")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val command = ExerciseByKeyCommand(
      templateId,
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
      "SumToK",
      ValueRecord(None, ImmArray((None, ValueInt64(5)))),
    )
    val submitters = Set(alice)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]
    val result =
      res
        .flatMap { cmds =>
          suffixLenientEngine
            .interpretCommands(
              validating = false,
              submitters = submitters,
              readAs = readAs,
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = seeding,
            )
            .consume(
              lookupContract,
              lookupPackage,
              lookupKey,
            )
        }
    val Right((tx, txMeta)) = result

    "be translated" in {
      val Right((rtx, _)) = suffixLenientEngine
        .submit(
          submitters,
          readAs,
          Commands(ImmArray(command), let, "test"),
          participant,
          submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      isReplayedBy(tx, rtx) shouldBe Right(())
    }

    "reinterpret to the same result" in {
      val stx = suffix(tx)

      val Right((rtx, _)) =
        reinterpret(
          suffixStrictEngine,
          Set(alice),
          stx.roots,
          stx,
          txMeta,
          let,
          lookupPackage,
          defaultContracts,
          defaultKey,
        )

      isReplayedBy(stx, rtx) shouldBe Right(())
    }

    "be validated" in {
      val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
      val validated = suffixLenientEngine
        .validate(submitters, ntx, let, participant, let, submissionSeed)
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
      validated match {
        case Left(e) =>
          fail(e.message)
        case Right(()) => ()
      }
    }

    "mark all the exercise nodes as performed byKey" in {
      val expectedNodes = tx.nodes.collect { case (id, _: Node.NodeExercises) =>
        id
      }
      val actualNodes = byKeyNodes(tx)
      actualNodes shouldBe Symbol("nonEmpty")
      actualNodes shouldBe expectedNodes.toSet
    }
  }

  "exercise-by-key" should {
    val seed = hash("exercise-by-key")

    val now = Time.Timestamp.now()

    "crash if use a contract key with an empty set of maintainers" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")

      val cmds = ImmArray(
        speedy.Command.ExerciseByKey(
          templateId = templateId,
          contractKey = SParty(alice),
          choiceId = ChoiceName.assertFromString("Noop"),
          argument = SValue.SUnit,
        )
      )
      val submitters = Set(alice)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty sey of maintainers"
        )
      }
    }
  }

  "fecth-by-key" should {
    val seed = hash("fetch-by-key")

    val now = Time.Timestamp.now()

    "crash if use a contract key with an empty set of maintainers" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")

      val cmds = ImmArray(
        speedy.Command.FetchByKey(
          templateId = templateId,
          key = SParty(alice),
        )
      )

      val submitters = Set(alice)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty sey of maintainers"
        )
      }
    }

    "error if the engine fails to find the key" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")

      val cmds = ImmArray(
        speedy.Command.FetchByKey(
          templateId = templateId,
          key = SRecord(
            BasicTests_WithKey,
            ImmArray("p", "k"),
            ArrayList(SParty(alice), SInt64(43)),
          ),
        )
      )

      val submitters = Set(alice)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(Error.Interpretation(err, _)) =>
        err shouldBe
          Interpretation.DamlException(
            interpretation.Error.ContractKeyNotFound(
              GlobalKey.assertBuild(
                BasicTests_WithKey,
                ValueRecord(
                  Some(BasicTests_WithKey),
                  ImmArray(
                    (Some[Ref.Name]("p"), ValueParty(alice)),
                    (Some[Ref.Name]("k"), ValueInt64(43)),
                  ),
                ),
              )
            )
          )
      }
    }
    "error if Speedy fails to find the key" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:FailedFetchByKey")

      // This first does a negative lookupByKey which succeeds
      // and then a fetchByKey which fails in speedy without calling back to the engine.
      val cmds = ImmArray(
        speedy.Command.CreateAndExercise(
          templateId = templateId,
          SRecord(templateId, ImmArray.Empty, ArrayList(SParty(alice))),
          "FetchAfterLookup",
          SRecord(templateId, ImmArray.Empty, ArrayList(SInt64(43))),
        )
      )

      val submitters = Set(alice)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(Error.Interpretation(err, _)) =>
        err shouldBe
          Interpretation.DamlException(
            interpretation.Error.ContractKeyNotFound(
              GlobalKey.assertBuild(
                BasicTests_WithKey,
                ValueRecord(
                  Some(BasicTests_WithKey),
                  ImmArray(
                    (Some[Ref.Name]("p"), ValueParty(alice)),
                    (Some[Ref.Name]("k"), ValueInt64(43)),
                  ),
                ),
              )
            )
          )
      }
    }
  }

  "create-and-exercise command" should {
    val submissionSeed = hash("create-and-exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)
    val command =
      CreateAndExerciseCommand(
        templateId,
        ValueRecord(Some(templateId), ImmArray(Some[Name]("p") -> ValueParty(party))),
        "Hello",
        ValueRecord(Some(hello), ImmArray.Empty),
      )

    val submitters = Set(party)

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]
    val interpretResult =
      res
        .flatMap { cmds =>
          suffixLenientEngine
            .interpretCommands(
              validating = false,
              submitters = submitters,
              readAs = Set.empty,
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = InitialSeeding.TransactionSeed(txSeed),
            )
            .consume(
              lookupContract,
              lookupPackage,
              lookupKey,
            )
        }

    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      tx.roots should have length 2
      tx.nodes.keySet.toList should have length 2
      val ImmArray(create, exercise) = tx.roots.map(tx.nodes)
      create shouldBe a[Node.NodeCreate]
      exercise shouldBe a[Node.NodeExercises]
    }

    "reinterpret to the same result" in {
      val stx = suffix(tx)

      val Right((rtx, _)) =
        reinterpret(suffixStrictEngine, Set(party), stx.roots, stx, txMeta, let, lookupPackage)

      isReplayedBy(stx, rtx) shouldBe Right(())
    }

    "be validated" in {
      val ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
      val validated = suffixLenientEngine
        .validate(Set(submitter), ntx, let, participant, let, submissionSeed)
        .consume(
          lookupContract,
          lookupPackage,
          lookupKey,
        )
      validated match {
        case Left(e) =>
          fail(e.message)
        case Right(()) => ()
      }
    }

    "not mark any node as byKey" in {
      interpretResult.map { case (tx, _) => byKeyNodes(tx).size } shouldBe Right(0)
    }
  }

  "translate list value" should {
    "translate empty list" in {
      val list = ValueNil
      val res = preprocessor
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack.empty))
    }

    "translate singleton" in {
      val list = ValueList(FrontStack(ValueInt64(1)))
      val res = preprocessor
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack(SInt64(1))))
    }

    "translate average list" in {
      val list = ValueList(
        FrontStack(ValueInt64(1), ValueInt64(2), ValueInt64(3), ValueInt64(4), ValueInt64(5))
      )
      val res = preprocessor
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(
        SValue.SList(FrontStack(SInt64(1), SInt64(2), SInt64(3), SInt64(4), SInt64(5)))
      )
    }

    "does not translate command with nesting of more than the value limit" in {
      val nested = (1 to 149).foldRight(ValueRecord(None, ImmArray((None, ValueInt64(42))))) {
        case (_, v) => ValueRecord(None, ImmArray((None, v)))
      }
      preprocessor
        .translateValue(
          TTyConApp(TypeConName(basicTestsPkgId, "BasicTests:Nesting0"), ImmArray.Empty),
          nested,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
        .swap
        .toOption
        .get
        .message should include("Provided value exceeds maximum nesting level")
    }
  }

  "record value translation" should {
    "work with nested records" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:MyNestedRec")),
        ImmArray(
          (Some[Name]("bar"), ValueText("bar")),
          (
            Some[Name]("nested"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:MyRec")),
              ImmArray(
                (Some[Name]("foo"), ValueText("bar"))
              ),
            ),
          ),
        ),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(Identifier(basicTestsPkgId, "BasicTests:MyNestedRec"))
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:MyNestedRec"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe a[Right[_, _]]
    }

    "work with fields with type parameters" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray(
          (Some[Name]("p"), ValueParty(alice)),
          (Some[Name]("v"), ValueOptional(Some(ValueInt64(42)))),
        ),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(
          Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")
        )
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe a[Right[_, _]]
    }

    "work with fields with labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray(
          (Some[Name]("v"), ValueOptional(Some(ValueInt64(42)))),
          (Some[Name]("p"), ValueParty(alice)),
        ),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(
          Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")
        )
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe a[Right[_, _]]
    }

    "fail with fields with labels, with repetitions" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((Some(toName("p")), ValueParty(alice)), (Some(toName("p")), ValueParty(bob))),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(
          Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")
        )
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      inside(res) { case Left(Error.Preprocessing(error)) =>
        error shouldBe a[Error.Preprocessing.TypeMismatch]
      }
    }

    "work with fields without labels, in right order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueParty(alice)), (None, ValueOptional(Some(ValueInt64(42))))),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(
          Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")
        )
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe a[Right[_, _]]
    }

    "fail with fields without labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueOptional(Some(ValueInt64(42)))), (None, ValueParty(alice))),
      )

      val Right(DDataType(_, ImmArray(), _)) =
        basicTestsSignatures.lookupDataType(
          Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")
        )
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.Empty),
          rec,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      inside(res) { case Left(Error.Preprocessing(error)) =>
        error shouldBe a[Error.Preprocessing.TypeMismatch]
      }
    }

  }

  "exercise callable command" should {
    val submissionSeed = hash("exercise callable command")
    val originalCoid = toContractId("BasicTests:CallablePayout:1")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
    // we need to fix time as cid are depending on it
    val let = Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")
    val command = ExerciseCommand(
      templateId,
      originalCoid,
      "Transfer",
      ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))),
    )

    val submitters = Set(bob)
    val readAs = (Set.empty: Set[Party])

    val Right((tx, txMeta)) = suffixLenientEngine
      .submit(
        submitters,
        readAs,
        Commands(ImmArray(command), let, "test"),
        participant,
        submissionSeed,
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    val submissionTime = txMeta.submissionTime

    val txSeed =
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    val Right(cmds) = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    val Right((rtx, _)) = suffixLenientEngine
      .interpretCommands(
        validating = false,
        submitters = submitters,
        readAs = Set.empty,
        commands = cmds,
        ledgerTime = let,
        submissionTime = submissionTime,
        seeding = InitialSeeding.TransactionSeed(txSeed),
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      isReplayedBy(tx, rtx) shouldBe Right(())
    }

    val blindingInfo = Blinding.blind(tx)

    "reinterpret to the same result" in {
      val stx = suffix(tx)

      val Right((rtx, _)) =
        reinterpret(
          suffixStrictEngine,
          Set(bob),
          stx.transaction.roots,
          stx,
          txMeta,
          let,
          lookupPackage,
          defaultContracts,
        )

      isReplayedBy(rtx, stx) shouldBe Right(())
    }

    "blinded correctly" in {

      // Bob sees both the archive and the create
      val bobView = Blinding.divulgedTransaction(blindingInfo.disclosure, bob, tx.transaction)
      bobView.nodes.size shouldBe 2
      findNodeByIdx(bobView.nodes, 0).getOrElse(fail("node not found")) match {
        case Node.NodeExercises(
              coid,
              _,
              choice,
              consuming,
              actingParties,
              _,
              _,
              _,
              _,
              children,
              _,
              _,
              _,
              _,
            ) =>
          coid shouldBe originalCoid
          consuming shouldBe true
          actingParties shouldBe Set(bob)
          children.map(_.index) shouldBe ImmArray(1)
          choice shouldBe "Transfer"
        case _ => fail("exercise expected first for Bob")
      }

      findNodeByIdx(bobView.nodes, 1).getOrElse(fail("node not found")) match {
        case create: Node.NodeCreate =>
          create.templateId shouldBe templateId
          create.stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView =
        Blinding.divulgedTransaction(blindingInfo.disclosure, clara, tx.transaction)

      claraView.nodes.size shouldBe 1
      findNodeByIdx(claraView.nodes, 1).getOrElse(fail("node not found")) match {
        case create: Node.NodeCreate =>
          create.templateId shouldBe templateId
          create.stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }
    }
  }

  "dynamic fetch actors" should {
    // Basic test: an exercise (on a "Fetcher" contract) with a single consequence, a fetch of the "Fetched" contract
    // Test a couple of scenarios, with different combination of signatories/observers/actors on the parent action

    val submissionSeed = hash("dynamic fetch actors")
    val fetchedCid = toContractId("1")
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTArgs = ImmArray(
      (Some[Name]("sig1"), ValueParty(alice)),
      (Some[Name]("sig2"), ValueParty(bob)),
      (Some[Name]("obs"), ValueParty(clara)),
    )

    val fetcherStrTid = "BasicTests:Fetcher"
    val fetcherTid = Identifier(basicTestsPkgId, fetcherStrTid)

    val fetcher1Cid = toContractId("2")
    val fetcher1TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(alice)),
      (Some[Name]("obs"), ValueParty(bob)),
      (Some[Name]("fetcher"), ValueParty(clara)),
    )

    val fetcher2Cid = toContractId("3")
    val fetcher2TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(party)),
      (Some[Name]("obs"), ValueParty(alice)),
      (Some[Name]("fetcher"), ValueParty(clara)),
    )

    def makeContract(
        tid: Ref.QualifiedName,
        targs: ImmArray[(Option[Name], Value)],
    ) =
      assertAsVersionedContract(
        ContractInstance(
          TypeConName(basicTestsPkgId, tid),
          ValueRecord(Some(Identifier(basicTestsPkgId, tid)), targs),
          "",
        )
      )

    def lookupContract(id: ContractId): Option[VersionedContractInstance] = {
      id match {
        case `fetchedCid` => Some(makeContract(fetchedStrTid, fetchedTArgs))
        case `fetcher1Cid` => Some(makeContract(fetcherStrTid, fetcher1TArgs))
        case `fetcher2Cid` => Some(makeContract(fetcherStrTid, fetcher2TArgs))
        case _ => None
      }
    }

    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)

    def actFetchActors(n: Node.GenNode): Set[Party] = {
      n match {
        case Node.NodeFetch(_, _, actingParties, _, _, _, _, _) => actingParties
        case _ => Set()
      }
    }

    def txFetchActors(tx: GenTx): Set[Party] =
      tx.fold(Set[Party]()) { case (actors, (_, n)) =>
        actors union actFetchActors(n)
      }

    def runExample(cid: ContractId, exerciseActor: Party) = {
      val command = ExerciseCommand(
        fetcherTid,
        cid,
        "DoFetch",
        ValueRecord(None, ImmArray((Some[Name]("cid"), ValueContractId(fetchedCid)))),
      )

      val submitters = Set(exerciseActor)

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)

      res
        .flatMap { cmds =>
          suffixLenientEngine
            .interpretCommands(
              validating = false,
              submitters = submitters,
              readAs = Set.empty,
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = seeding,
            )
            .consume(
              lookupContract,
              lookupPackage,
              lookupKey,
            )
        }

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" in {

      val Right((tx, _)) = runExample(fetcher1Cid, clara)
      txFetchActors(tx.transaction) shouldBe Set(alice, clara)
    }

    "not propagate the parent's signatories nor actors when not stakeholders" in {

      val Right((tx, _)) = runExample(fetcher2Cid, clara)
      txFetchActors(tx.transaction) shouldBe Set(clara)
    }

    "be retained when reinterpreting single fetch nodes" in {
      val Right((tx, txMeta)) = runExample(fetcher1Cid, clara)
      val fetchNodes = tx.nodes.iterator.collect { case (nid, fetch: Node.NodeFetch) =>
        nid -> fetch
      }

      fetchNodes.foreach { case (_, n) =>
        val nid = NodeId(0) //we must use node-0 so the constructed tx is normalized
        val fetchTx = VersionedTransaction(n.version, Map(nid -> n), ImmArray(nid))
        val Right((reinterpreted, _)) =
          suffixLenientEngine
            .reinterpret(
              n.requiredAuthorizers,
              FetchCommand(n.templateId, n.coid),
              txMeta.nodeSeeds.toSeq.collectFirst { case (`nid`, seed) => seed },
              txMeta.submissionTime,
              let,
            )
            .consume(
              lookupContract,
              lookupPackage,
              lookupKey,
            )
        isReplayedBy(fetchTx, reinterpreted) shouldBe Right(())
      }
    }

    "not mark any node as byKey" in {
      runExample(fetcher2Cid, clara).map { case (tx, _) => byKeyNodes(tx).size } shouldBe Right(0)
    }
  }

  "reinterpreting fetch nodes" should {

    val fetchedCid = toContractId("1")
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTid = Identifier(basicTestsPkgId, fetchedStrTid)

    val fetchedContract =
      assertAsVersionedContract(
        ContractInstance(
          TypeConName(basicTestsPkgId, fetchedStrTid),
          ValueRecord(
            Some(Identifier(basicTestsPkgId, fetchedStrTid)),
            ImmArray(
              (Some[Name]("sig1"), ValueParty(alice)),
              (Some[Name]("sig2"), ValueParty(bob)),
              (Some[Name]("obs"), ValueParty(clara)),
            ),
          ),
          "",
        )
      )

    def lookupContract(id: ContractId): Option[VersionedContractInstance] = {
      id match {
        case `fetchedCid` => Some(fetchedContract)
        case _ => None
      }
    }

    "succeed with a fresh engine, correctly compiling packages" in {
      val engine = newEngine()

      val fetchNode = FetchCommand(
        templateId = fetchedTid,
        coid = fetchedCid,
      )

      val let = Time.Timestamp.now()

      val submitters = Set(alice)

      val reinterpreted =
        engine
          .reinterpret(submitters, fetchNode, None, let, let)
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )

      reinterpreted shouldBe a[Right[_, _]]
    }

  }

  "lookup by key" should {

    val seed = hash("interpreting lookup by key nodes")

    val lookedUpCid = toContractId("1")
    val lookerUpTemplate = "BasicTests:LookerUpByKey"
    val lookerUpTemplateId = Identifier(basicTestsPkgId, lookerUpTemplate)
    val lookerUpCid = toContractId("2")
    val lookerUpInst =
      assertAsVersionedContract(
        ContractInstance(
          TypeConName(basicTestsPkgId, lookerUpTemplate),
          ValueRecord(Some(lookerUpTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice)))),
          "",
        )
      )

    def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] = {
      (key.globalKey.templateId, key.globalKey.key) match {
        case (
              BasicTests_WithKey,
              ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
            ) =>
          Some(lookedUpCid)
        case _ =>
          None
      }
    }

    def lookupContractMap = Map(
      lookedUpCid -> withKeyContractInst,
      lookerUpCid -> lookerUpInst,
    )

    def firstLookupNode(
        tx: GenTx
    ): Option[(NodeId, Node.NodeLookupByKey)] =
      tx.nodes.collectFirst { case (nid, nl @ Node.NodeLookupByKey(_, _, _, _)) =>
        nid -> nl
      }

    val now = Time.Timestamp.now()

    "mark all lookupByKey nodes as byKey" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])
      val Right((tx, _)) = newEngine()
        .submit(submitters, readAs, Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )

      val expectedByKeyNodes = tx.transaction.nodes.collect { case (id, _: Node.NodeLookupByKey) =>
        id
      }
      val actualByKeyNodes = byKeyNodes(tx)
      actualByKeyNodes shouldBe Symbol("nonEmpty")
      actualByKeyNodes shouldBe expectedByKeyNodes.toSet
    }

    "be reinterpreted to the same node when lookup finds a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])

      val Right((tx, txMeta)) = suffixLenientEngine
        .submit(submitters, readAs, Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )
      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe Some(lookedUpCid)

      val Right((reinterpreted, _)) =
        newEngine()
          .reinterpret(
            submitters,
            LookupByKeyCommand(lookupNode.templateId, lookupNode.key.key),
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )

      firstLookupNode(reinterpreted.transaction).map(_._2) shouldEqual Some(lookupNode)
    }

    "be reinterpreted to the same node when lookup doesn't find a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(57)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])

      val Right((tx, txMeta)) = suffixLenientEngine
        .submit(submitters, readAs, Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )

      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe None

      val Right((reinterpreted, _)) =
        newEngine()
          .reinterpret(
            submitters,
            LookupByKeyCommand(lookupNode.templateId, lookupNode.key.key),
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )

      firstLookupNode(reinterpreted.transaction).map(_._2) shouldEqual Some(lookupNode)
    }

    "crash if use a contract key with an empty set of maintainers" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")

      val cmds = ImmArray(
        speedy.Command.LookupByKey(templateId, SParty(alice))
      )

      val submitters = Set(alice)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty sey of maintainers"
        )
      }
    }
  }

  "getTime set dependsOnTime flag" in {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:TimeGetter")
    def run(choiceName: ChoiceName) = {
      val submissionSeed = hash(s"getTime set dependsOnTime flag: ($choiceName)")
      val command =
        CreateAndExerciseCommand(
          templateId = templateId,
          createArgument = ValueRecord(None, ImmArray(None -> ValueParty(party))),
          choiceId = choiceName,
          choiceArgument = ValueRecord(None, ImmArray.Empty),
        )
      val submitters = Set(party)
      val readAs = (Set.empty: Set[Party])
      suffixLenientEngine
        .submit(
          submitters,
          readAs,
          Commands(ImmArray(command), Time.Timestamp.now(), "test"),
          participant,
          submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
    }

    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)
    run("GetTime").map(_._2.dependsOnTime) shouldBe Right(true)
    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)

  }

  "fetching contracts that have keys correctly fills in the transaction structure" when {
    val fetchedCid = toContractId("1")
    val now = Time.Timestamp.now()
    val submissionSeed = crypto.Hash.hashPrivateKey(
      "fetching contracts that have keys correctly fills in the transaction structure"
    )
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, now)

    "fetched via a fetch" in {

      val lookupContractMap = Map(fetchedCid -> withKeyContractInst)

      val cmd = speedy.Command.Fetch(BasicTests_WithKey, SValue.SContractId(fetchedCid))

      val submitters = Set(alice)

      val Right((tx, _)) = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = ImmArray(cmd),
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
        )
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )

      tx.transaction.nodes.values.headOption match {
        case Some(Node.NodeFetch(_, _, _, _, _, key, _, _)) =>
          key match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(Node.KeyWithMaintainers(_, maintainers)) =>
              assert(maintainers == Set(alice))
            case None => fail("the recomputed fetch didn't have a key")
          }
        case _ => fail("Recomputed a non-fetch or no nodes at all")
      }
    }

    "fetched via a fetchByKey" in {
      val fetcherTemplate = "BasicTests:FetcherByKey"
      val fetcherTemplateId = Identifier(basicTestsPkgId, fetcherTemplate)
      val fetcherCid = toContractId("2")
      val fetcherInst = assertAsVersionedContract(
        ContractInstance(
          TypeConName(basicTestsPkgId, fetcherTemplate),
          ValueRecord(Some(fetcherTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice)))),
          "",
        )
      )

      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] = {
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                BasicTests_WithKey,
                ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
              ) =>
            Some(fetchedCid)
          case _ =>
            None
        }
      }

      val lookupContractMap = Map(fetchedCid -> withKeyContractInst, fetcherCid -> fetcherInst)

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessCommands(
          ImmArray(
            ExerciseCommand(
              fetcherTemplateId,
              fetcherCid,
              "Fetch",
              ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
            )
          )
        )
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )

      val Right((tx, _)) = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
        )
        .consume(
          lookupContractMap.get,
          lookupPackage,
          lookupKey,
        )

      tx.transaction.nodes
        .collectFirst { case (id, nf: Node.NodeFetch) =>
          nf.key match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(Node.KeyWithMaintainers(_, maintainers)) =>
              assert(maintainers == Set(alice))
            case None => fail("the recomputed fetch didn't have a key")
          }
          byKeyNodes(tx) shouldBe Set(id)
        }
        .getOrElse(fail("didn't find the fetch node resulting from fetchByKey"))
    }
  }

  "wrongly typed contract id" should {
    val withKeyId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val simpleId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val fetcherId = Identifier(basicTestsPkgId, "BasicTests:Fetcher")
    val cid = toContractId("BasicTests:WithKey:1")
    val fetcherCid = toContractId("42")
    val fetcherInst = assertAsVersionedContract(
      ContractInstance(
        fetcherId,
        ValueRecord(
          None,
          ImmArray(
            (None, ValueParty(alice)),
            (None, ValueParty(alice)),
            (None, ValueParty(alice)),
          ),
        ),
        "",
      )
    )
    val contracts = defaultContracts + (fetcherCid -> fetcherInst)
    val lookupContract = contracts.get(_)
    val correctCommand =
      ExerciseCommand(withKeyId, cid, "SumToK", ValueRecord(None, ImmArray((None, ValueInt64(42)))))
    val incorrectCommand =
      ExerciseCommand(simpleId, cid, "Hello", ValueRecord(None, ImmArray.Empty))
    val incorrectFetch =
      ExerciseCommand(
        fetcherId,
        fetcherCid,
        "DoFetch",
        ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
      )

    val now = Time.Timestamp.now()
    val submissionSeed = hash("wrongly-typed cid")
    val submitters = Set(alice)
    val readAs = (Set.empty: Set[Party])
    def run(cmds: ImmArray[ApiCommand]) =
      suffixLenientEngine
        .submit(submitters, readAs, Commands(cmds, now, ""), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)

    "error on fetch" in {
      val result = run(ImmArray(incorrectFetch))
      inside(result) { case Left(e) =>
        e.message should include("wrongly typed contract id")
      }
    }
    "error on exercise" in {
      val result = run(ImmArray(incorrectCommand))
      inside(result) { case Left(e) =>
        e.message should include("wrongly typed contract id")
      }
    }
    "error on exercise even if used correctly before" in {
      val result = run(ImmArray(correctCommand, incorrectCommand))
      inside(result) { case Left(e) =>
        e.message should include("wrongly typed contract id")
      }
    }
  }

  "nested transactions" should {

    val forkableTemplate = "BasicTests:Forkable"
    val forkableTemplateId = Identifier(basicTestsPkgId, forkableTemplate)
    val forkableInst =
      ValueRecord(
        Some(forkableTemplateId),
        ImmArray(
          (Some[Name]("party"), ValueParty(party)),
          (Some[Name]("parent"), ValueOptional(None)),
        ),
      )

    val submissionSeed = hash("nested transaction test")
    val let = Time.Timestamp.now()

    def run(n: Int) = {
      val command = CreateAndExerciseCommand(
        templateId = forkableTemplateId,
        createArgument = forkableInst,
        choiceId = "Fork",
        choiceArgument = ValueRecord(None, ImmArray((None, ValueInt64(n.toLong)))),
      )
      val submitters = Set(party)
      val readAs = (Set.empty: Set[Party])
      suffixLenientEngine
        .submit(
          submitters,
          readAs,
          Commands(ImmArray(command), let, "test"),
          participant,
          submissionSeed,
        )
        .consume(_ => None, lookupPackage, _ => None)
    }

    "produce a quadratic number of nodes" in {
      run(0).map(_._1.transaction.nodes.size) shouldBe Right(2)
      run(1).map(_._1.transaction.nodes.size) shouldBe Right(6)
      run(2).map(_._1.transaction.nodes.size) shouldBe Right(14)
      run(3).map(_._1.transaction.nodes.size) shouldBe Right(30)
    }

    "be validable in whole" in {
      def validate(tx: SubmittedTransaction, metaData: Tx.Metadata) =
        for {
          submitter <- tx.guessSubmitter
          ntx = SubmittedTransaction(Normalization.normalizeTx(tx))
          res <- suffixLenientEngine
            .validate(
              Set(submitter),
              ntx,
              let,
              participant,
              metaData.submissionTime,
              submissionSeed,
            )
            .consume(
              _ => None,
              lookupPackage,
              _ => None,
            )
            .left
            .map(_.message)
        } yield res

      run(0).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
      run(3).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
    }

    "be partially reinterpretable" in {
      val Right((tx, txMeta)) = run(3)
      val stx = suffix(tx)

      val ImmArray(_, exeNode1) = tx.transaction.roots
      val Node.NodeExercises(_, _, _, _, _, _, _, _, _, children, _, _, _, _) =
        tx.transaction.nodes(exeNode1)
      val nids = children.toSeq.take(2).toImmArray

      reinterpret(
        suffixStrictEngine,
        Set(party),
        nids,
        stx,
        txMeta,
        let,
        lookupPackage,
      ) shouldBe a[Right[_, _]]

    }
  }

  "contract key" should {
    val now = Time.Timestamp.now()
    val submissionSeed = crypto.Hash.hashPrivateKey("contract key")
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, now)

    "be evaluated only when executing create" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyWhenExecutingCreate")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )
      val exerciseArg =
        ValueRecord(
          Some(Identifier(basicTestsPkgId, "BasicTests:DontExecuteCreate")),
          ImmArray.Empty,
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessCommands(
          ImmArray(
            CreateAndExerciseCommand(templateId, createArg, "DontExecuteCreate", exerciseArg)
          )
        )
        .consume(_ => None, lookupPackage, lookupKey)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
        )
        .consume(_ => None, lookupPackage, lookupKey)
      result shouldBe a[Right[_, _]]
    }

    "be evaluated after ensure clause" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyAfterEnsureClause")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessCommands(ImmArray(CreateCommand(templateId, createArg)))
        .consume(_ => None, lookupPackage, lookupKey)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
        )
        .consume(_ => None, lookupPackage, lookupKey)
      result shouldBe a[Left[_, _]]
      val Left(err) = result
      err.message should not include ("Boom")
      err.message should include("Template precondition violated")
    }

    "not be create if has an empty set of maintainer" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("sig"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessCommands(ImmArray(CreateCommand(templateId, createArg)))
        .consume(_ => None, lookupPackage, lookupKey)
      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty sey of maintainers"
        )
      }
    }

    // Note that we provide no stability for multi key semantics so
    // these tests serve only as an indication of the current behavior
    // but can be changed freely.
    "multi keys" should {
      import com.daml.lf.language.{LanguageVersion => LV}
      val nonUckEngine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LV.DevVersions,
          contractKeyUniqueness = ContractKeyUniquenessMode.Off,
          forbidV0ContractId = true,
          requireSuffixedGlobalContractId = true,
        )
      )
      val uckEngine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LV.DevVersions,
          contractKeyUniqueness = ContractKeyUniquenessMode.On,
          forbidV0ContractId = true,
          requireSuffixedGlobalContractId = true,
        )
      )
      val (multiKeysPkgId, _, allMultiKeysPkgs) = loadPackage("daml-lf/tests/MultiKeys.dar")
      val lookupPackage = allMultiKeysPkgs.get(_)
      val keyedId = Identifier(multiKeysPkgId, "MultiKeys:Keyed")
      val opsId = Identifier(multiKeysPkgId, "MultiKeys:KeyOperations")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("multikeys")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)

      val cid1 = toContractId("1")
      val cid2 = toContractId("2")
      val keyedInst = assertAsVersionedContract(
        ContractInstance(
          TypeConName(multiKeysPkgId, "MultiKeys:Keyed"),
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "",
        )
      )
      val contracts = Map(cid1 -> keyedInst, cid2 -> keyedInst)
      val lookupContract = contracts.get(_)
      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                `keyedId`,
                ValueParty(`party`),
              ) =>
            Some(cid1)
          case _ =>
            None
        }
      def run(engine: Engine, choice: String, argument: Value) = {
        val cmd = CreateAndExerciseCommand(
          opsId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          choice,
          argument,
        )
        val Right(cmds) = preprocessor
          .preprocessCommands(ImmArray(cmd))
          .consume(lookupContract, lookupPackage, lookupKey)
        engine
          .interpretCommands(
            validating = false,
            submitters = Set(party),
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = let,
            submissionTime = let,
            seeding = seeding,
          )
          .consume(lookupContract, lookupPackage, lookupKey)
      }
      val emptyRecord = ValueRecord(None, ImmArray.Empty)
      // The cid returned by a fetchByKey at the beginning
      val keyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid1))))
      // The cid not returned by a fetchByKey at the beginning
      val nonKeyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid2))))
      val twoCids =
        ValueRecord(None, ImmArray((None, ValueContractId(cid1)), (None, ValueContractId(cid2))))
      val createOverwritesLocal = ("CreateOverwritesLocal", emptyRecord)
      val createOverwritesUnknownGlobal = ("CreateOverwritesUnknownGlobal", emptyRecord)
      val createOverwritesKnownGlobal = ("CreateOverwritesKnownGlobal", emptyRecord)
      val fetchDoesNotOverwriteGlobal = ("FetchDoesNotOverwriteGlobal", nonKeyResultCid)
      val fetchDoesNotOverwriteLocal = ("FetchDoesNotOverwriteLocal", keyResultCid)
      val localArchiveOverwritesUnknownGlobal = ("LocalArchiveOverwritesUnknownGlobal", emptyRecord)
      val localArchiveOverwritesKnownGlobal = ("LocalArchiveOverwritesKnownGlobal", emptyRecord)
      val globalArchiveOverwritesUnknownGlobal = ("GlobalArchiveOverwritesUnknownGlobal", twoCids)
      val globalArchiveOverwritesKnownGlobal1 = ("GlobalArchiveOverwritesKnownGlobal1", twoCids)
      val globalArchiveOverwritesKnownGlobal2 = ("GlobalArchiveOverwritesKnownGlobal2", twoCids)
      val rollbackCreateNonRollbackFetchByKey = ("RollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey =
        ("RollbackFetchByKeyRollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyNonRollbackCreate = ("RollbackFetchByKeyNonRollbackCreate", emptyRecord)
      val rollbackFetchNonRollbackCreate = ("RollbackFetchNonRollbackCreate", keyResultCid)
      val rollbackGlobalArchiveNonRollbackCreate =
        ("RollbackGlobalArchiveNonRollbackCreate", keyResultCid)
      val rollbackCreateNonRollbackGlobalArchive =
        ("RollbackCreateNonRollbackGlobalArchive", keyResultCid)
      val rollbackGlobalArchiveUpdates =
        ("RollbackGlobalArchiveUpdates", twoCids)

      val allCases = Table(
        ("choice", "argument"),
        createOverwritesLocal,
        createOverwritesUnknownGlobal,
        createOverwritesKnownGlobal,
        fetchDoesNotOverwriteGlobal,
        fetchDoesNotOverwriteLocal,
        localArchiveOverwritesUnknownGlobal,
        localArchiveOverwritesKnownGlobal,
        globalArchiveOverwritesUnknownGlobal,
        globalArchiveOverwritesKnownGlobal1,
        globalArchiveOverwritesKnownGlobal2,
        rollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyNonRollbackCreate,
        rollbackFetchNonRollbackCreate,
        rollbackGlobalArchiveNonRollbackCreate,
        rollbackCreateNonRollbackGlobalArchive,
        rollbackGlobalArchiveUpdates,
      )

      val uckFailures = Set(
        "CreateOverwritesLocal",
        "CreateOverwritesKnownGlobal",
        "LocalArchiveOverwritesKnownGlobal",
        "RollbackCreateNonRollbackFetchByKey",
        "RollbackFetchByKeyRollbackCreateNonRollbackFetchByKey",
        "RollbackFetchByKeyNonRollbackCreate",
      )

      "non-uck mode" in {
        forEvery(allCases) { case (name, arg) =>
          run(nonUckEngine, name, arg) shouldBe a[Right[_, _]]
        }
      }
      "uck mode" in {
        forEvery(allCases) { case (name, arg) =>
          if (uckFailures.contains(name)) {
            run(uckEngine, name, arg) shouldBe a[Left[_, _]]
          } else {
            run(uckEngine, name, arg) shouldBe a[Right[_, _]]
          }
        }
      }
    }

    "exceptions" should {
      val (exceptionsPkgId, _, allExceptionsPkgs) = loadPackage("daml-lf/tests/Exceptions.dar")
      val lookupPackage = allExceptionsPkgs.get(_)
      val kId = Identifier(exceptionsPkgId, "Exceptions:K")
      val tId = Identifier(exceptionsPkgId, "Exceptions:T")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("rollback")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)
      val cid = toContractId("1")
      val contracts = Map(
        cid -> assertAsVersionedContract(
          ContractInstance(
            TypeConName(exceptionsPkgId, "Exceptions:K"),
            ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(0)))),
            "",
          )
        )
      )
      val lookupContract = contracts.get(_)
      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                `kId`,
                ValueRecord(_, ImmArray((_, ValueParty(`party`)), (_, ValueInt64(0)))),
              ) =>
            Some(cid)
          case _ =>
            None
        }
      def run(cmd: ApiCommand) = {
        val submitters = Set(party)
        val Right(cmds) = preprocessor
          .preprocessCommands(ImmArray(cmd))
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )
        suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = let,
            submissionTime = let,
            seeding = seeding,
          )
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )
      }
      "rolled-back archive of transient contract does not prevent consuming choice after rollback" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "RollbackArchiveTransient",
          ValueRecord(None, ImmArray((None, ValueInt64(0)))),
        )
        run(command) shouldBe a[Right[_, _]]
      }
      "archive of transient contract in try prevents consuming choice after try if not rolled back" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "ArchiveTransient",
          ValueRecord(None, ImmArray((None, ValueInt64(0)))),
        )
        run(command) shouldBe a[Left[_, _]]
      }
      "rolled-back archive of non-transient contract does not prevent consuming choice after rollback" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "RollbackArchiveNonTransient",
          ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
        )
        run(command) shouldBe a[Right[_, _]]
      }
      "archive of non-transient contract in try prevents consuming choice after try if not rolled back" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "ArchiveNonTransient",
          ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
        )
        run(command) shouldBe a[Left[_, _]]
      }
      "key updates in rollback node are rolled back" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "RollbackKey",
          ValueRecord(None, ImmArray((None, ValueInt64(0)))),
        )
        run(command) shouldBe a[Right[_, _]]
      }
      "key updates in try are not rolled back if no exception is thrown" in {
        val command = CreateAndExerciseCommand(
          tId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "Key",
          ValueRecord(None, ImmArray((None, ValueInt64(0)))),
        )
        run(command) shouldBe a[Right[_, _]]
      }
    }

    "action node seeds" should {
      val (exceptionsPkgId, _, allExceptionsPkgs) = loadPackage("daml-lf/tests/Exceptions.dar")
      val lookupPackage = allExceptionsPkgs.get(_)
      val kId = Identifier(exceptionsPkgId, "Exceptions:K")
      val seedId = Identifier(exceptionsPkgId, "Exceptions:NodeSeeds")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("rollback")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)
      val cid = toContractId("1")
      val contracts = Map(
        cid -> assertAsVersionedContract(
          ContractInstance(
            TypeConName(exceptionsPkgId, "Exceptions:K"),
            ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(0)))),
            "",
          )
        )
      )
      val lookupContract = contracts.get(_)
      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                `kId`,
                ValueRecord(_, ImmArray((_, ValueParty(`party`)), (_, ValueInt64(0)))),
              ) =>
            Some(cid)
          case _ =>
            None
        }
      def run(cmd: ApiCommand) = {
        val submitters = Set(party)
        val Right(cmds) = preprocessor
          .preprocessCommands(ImmArray(cmd))
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )
        suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = let,
            submissionTime = let,
            seeding = seeding,
          )
          .consume(
            lookupContract,
            lookupPackage,
            lookupKey,
          )
      }
      "Only create and exercise nodes end up in actionNodeSeeds" in {
        val command = CreateAndExerciseCommand(
          seedId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "CreateAllTypes",
          ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
        )
        inside(run(command)) { case Right((tx, meta)) =>
          tx.nodes.size shouldBe 9
          tx.nodes(NodeId(0)) shouldBe a[Node.NodeCreate]
          tx.nodes(NodeId(1)) shouldBe a[Node.NodeExercises]
          tx.nodes(NodeId(2)) shouldBe a[Node.NodeFetch]
          tx.nodes(NodeId(3)) shouldBe a[Node.NodeLookupByKey]
          tx.nodes(NodeId(4)) shouldBe a[Node.NodeCreate]
          tx.nodes(NodeId(5)) shouldBe a[Node.NodeRollback]
          tx.nodes(NodeId(6)) shouldBe a[Node.NodeFetch]
          tx.nodes(NodeId(7)) shouldBe a[Node.NodeLookupByKey]
          tx.nodes(NodeId(8)) shouldBe a[Node.NodeCreate]
          meta.nodeSeeds.map(_._1.index) shouldBe ImmArray(0, 1, 4, 8)
        }
      }
    }

    "global key lookups" should {
      val (exceptionsPkgId, _, allExceptionsPkgs) = loadPackage("daml-lf/tests/Exceptions.dar")
      val lookupPackage = allExceptionsPkgs.get(_)
      val kId = Identifier(exceptionsPkgId, "Exceptions:K")
      val tId = Identifier(exceptionsPkgId, "Exceptions:GlobalLookups")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("global-keys")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)
      val cid = toContractId("1")
      val contracts = Map(
        cid -> assertAsVersionedContract(
          ContractInstance(
            TypeConName(exceptionsPkgId, "Exceptions:K"),
            ValueRecord(None, ImmArray((None, ValueParty(party)), (None, ValueInt64(0)))),
            "",
          )
        )
      )
      val lookupContract = contracts.get(_)
      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                `kId`,
                ValueRecord(_, ImmArray((_, ValueParty(`party`)), (_, ValueInt64(0)))),
              ) =>
            Some(cid)
          case _ =>
            None
        }
      def run(cmd: ApiCommand): Int = {
        val submitters = Set(party)
        var keyLookups = 0
        def mockedKeyLookup(key: GlobalKeyWithMaintainers) = {
          keyLookups += 1
          lookupKey(key)
        }
        val Right(cmds) = preprocessor
          .preprocessCommands(ImmArray(cmd))
          .consume(
            lookupContract,
            lookupPackage,
            mockedKeyLookup,
          )
        val result = suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = let,
            submissionTime = let,
            seeding = seeding,
          )
          .consume(
            lookupContract,
            lookupPackage,
            mockedKeyLookup,
          )
        inside(result) { case Right(_) =>
          keyLookups
        }
      }
      val cidArg = ValueRecord(None, ImmArray((None, ValueContractId(cid))))
      val emptyArg = ValueRecord(None, ImmArray.empty)
      "Lookup a global key at most once" in {
        val cases = Table(
          ("choice", "argument", "lookups"),
          ("LookupTwice", emptyArg, 1),
          ("LookupAfterCreate", emptyArg, 0),
          ("LookupAfterCreateArchive", emptyArg, 0),
          ("LookupAfterFetch", cidArg, 1),
          ("LookupAfterArchive", cidArg, 1),
          ("LookupAfterRollbackCreate", emptyArg, 0),
          ("LookupAfterRollbackLookup", emptyArg, 1),
          ("LookupAfterArchiveAfterRollbackLookup", cidArg, 1),
        )
        forAll(cases) { case (choice, argument, lookups) =>
          val command = CreateAndExerciseCommand(
            tId,
            ValueRecord(None, ImmArray((None, ValueParty(party)))),
            choice,
            argument,
          )
          run(command) shouldBe lookups
        }
      }
    }
  }

  "Engine.preloadPackage" should {

    import com.daml.lf.language.{LanguageVersion => LV}

    def engine(min: LV, max: LV) =
      new Engine(
        EngineConfig(
          allowedLanguageVersions = VersionRange(min, max),
          forbidV0ContractId = true,
          requireSuffixedGlobalContractId = true,
        )
      )

    val pkgId = Ref.PackageId.assertFromString("-pkg-")

    def pkg(version: LV) =
      language.Ast.Package(
        Iterable.empty,
        Iterable.empty,
        version,
        None,
      )

    "reject disallow packages" in {
      val negativeTestCases = Table(
        ("pkg version", "minVersion", "maxversion"),
        (LV.v1_6, LV.v1_6, LV.v1_8),
        (LV.v1_7, LV.v1_6, LV.v1_8),
        (LV.v1_8, LV.v1_6, LV.v1_8),
        (LV.v1_dev, LV.v1_6, LV.v1_dev),
      )
      val positiveTestCases = Table(
        ("pkg version", "minVersion", "maxversion"),
        (LV.v1_6, LV.v1_7, LV.v1_dev),
        (LV.v1_7, LV.v1_8, LV.v1_8),
        (LV.v1_8, LV.v1_6, LV.v1_7),
        (LV.v1_dev, LV.v1_6, LV.v1_8),
      )

      forEvery(negativeTestCases)((v, min, max) =>
        engine(min, max).preloadPackage(pkgId, pkg(v)) shouldBe a[ResultDone[_]]
      )

      forEvery(positiveTestCases)((v, min, max) =>
        engine(min, max).preloadPackage(pkgId, pkg(v)) shouldBe a[ResultError]
      )

    }

  }

}

object EngineTest {

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")
  private def byKeyNodes(tx: VersionedTransaction) =
    tx.nodes.collect { case (nodeId, node: GenActionNode) if node.byKey => nodeId }.toSet

  private val party = Party.assertFromString("Party")
  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")
  private val clara = Party.assertFromString("Clara")

  private def newEngine(requireCidSuffixes: Boolean = false) =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = language.LanguageVersion.DevVersions,
        forbidV0ContractId = true,
        requireSuffixedGlobalContractId = requireCidSuffixes,
      )
    )

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  private def findNodeByIdx[Cid](nodes: Map[NodeId, Node.GenNode], idx: Int) =
    nodes.collectFirst { case (nodeId, node) if nodeId.index == idx => node }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def resultEq: Equality[Either[Error, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  private def isReplayedBy(
      recorded: VersionedTransaction,
      replayed: VersionedTransaction,
  ): Either[ReplayMismatch, Unit] = {
    // we normalize the LEFT arg before calling isReplayedBy to mimic the effect of serialization
    Validation.isReplayedBy(Normalization.normalizeTx(recorded), replayed)
  }

  private def suffix(tx: Tx.Transaction) =
    data.assertRight(tx.suffixCid(_ => dummySuffix))

  private[this] case class ReinterpretState(
      contracts: Map[ContractId, VersionedContractInstance],
      keys: Map[GlobalKey, ContractId],
      nodes: HashMap[NodeId, GenNode] = HashMap.empty,
      roots: BackStack[NodeId] = BackStack.empty,
      dependsOnTime: Boolean = false,
      nodeSeeds: BackStack[(NodeId, crypto.Hash)] = BackStack.empty,
  ) {
    def commit(tr: GenTx, meta: Tx.Metadata) = {
      val (newContracts, newKeys) = tr.fold((contracts, keys)) {
        case ((contracts, keys), (_, exe: Node.NodeExercises)) =>
          (contracts - exe.targetCoid, keys)
        case ((contracts, keys), (_, create: Node.NodeCreate)) =>
          (
            contracts.updated(
              create.coid,
              create.versionedCoinst,
            ),
            create.key.fold(keys)(k =>
              keys.updated(GlobalKey.assertBuild(create.templateId, k.key), create.coid)
            ),
          )
        case (acc, _) => acc
      }
      ReinterpretState(
        newContracts,
        newKeys,
        nodes ++ tr.nodes,
        roots :++ tr.roots,
        dependsOnTime || meta.dependsOnTime,
        nodeSeeds :++ meta.nodeSeeds,
      )
    }
  }

  // Mimics Canton reinterpreation
  // requires a suffixed transaction.
  private def reinterpret(
      engine: Engine,
      submitters: Set[Party],
      nodes: ImmArray[NodeId],
      tx: Tx.Transaction,
      txMeta: Tx.Metadata,
      ledgerEffectiveTime: Time.Timestamp,
      lookupPackages: PackageId => Option[Package],
      contracts: Map[ContractId, VersionedContractInstance] = Map.empty,
      keys: Map[GlobalKey, ContractId] = Map.empty,
  ): Either[Error, (Tx.Transaction, Tx.Metadata)] = {

    val nodeSeedMap = txMeta.nodeSeeds.toSeq.toMap

    val finalState =
      nodes.foldLeft[Either[Error, ReinterpretState]](Right(ReinterpretState(contracts, keys))) {
        case (acc, nodeId) =>
          for {
            state <- acc
            cmd = tx.transaction.nodes(nodeId) match {
              case create: Node.NodeCreate =>
                CreateCommand(create.templateId, create.arg)
              case fetch: Node.NodeFetch if fetch.byKey =>
                val key = fetch.key.getOrElse(sys.error("unexpected empty contract key")).key
                FetchByKeyCommand(fetch.templateId, key)
              case fetch: Node.NodeFetch =>
                FetchCommand(fetch.templateId, fetch.coid)
              case lookup: Node.NodeLookupByKey =>
                LookupByKeyCommand(lookup.templateId, lookup.key.key)
              case exe: Node.NodeExercises if exe.byKey =>
                val key = exe.key.getOrElse(sys.error("unexpected empty contract key")).key
                ExerciseByKeyCommand(exe.templateId, key, exe.choiceId, exe.chosenValue)
              case exe: Node.NodeExercises =>
                ExerciseCommand(exe.templateId, exe.targetCoid, exe.choiceId, exe.chosenValue)
              case _: Node.NodeRollback =>
                sys.error("unexpected rollback node")
            }
            currentStep <- engine
              .reinterpret(
                submitters,
                cmd,
                nodeSeedMap.get(nodeId),
                txMeta.submissionTime,
                ledgerEffectiveTime,
              )
              .consume(
                state.contracts.get,
                lookupPackages,
                k => state.keys.get(k.globalKey),
              )
            (tr0, meta0) = currentStep
            tr1 = suffix(tr0)
            n = state.nodes.size
            nodeRenaming = (nid: NodeId) => NodeId(nid.index + n)
            tr = tr1.transaction.mapNodeId(nodeRenaming)
            meta = meta0.copy(nodeSeeds = meta0.nodeSeeds.map { case (nid, seed) =>
              nodeRenaming(nid) -> seed
            })
          } yield state.commit(tr, meta)
      }

    finalState.map(state =>
      (
        TxVersions.asVersionedTransaction(
          GenTx(
            state.nodes,
            state.roots.toImmArray,
          )
        ),
        Tx.Metadata(
          submissionSeed = None,
          submissionTime = txMeta.submissionTime,
          usedPackages = Set.empty,
          dependsOnTime = state.dependsOnTime,
          nodeSeeds = state.nodeSeeds.toImmArray,
        ),
      )
    )
  }

}
