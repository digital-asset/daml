// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.io.File
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  Normalization,
  ReplayMismatch,
  SubmittedTransaction,
  Validation,
  VersionedTransaction,
  Transaction => Tx,
  TransactionVersion => TxVersions,
}
import com.daml.lf.value.Value
import Value._
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf
import com.daml.lf.speedy.{ArrayList, DisclosedContract, InitialSeeding, SValue, svalue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.command._
import com.daml.lf.crypto.Hash
import com.daml.lf.engine.Error.Interpretation
import com.daml.lf.engine.Error.Interpretation.DamlException
import com.daml.lf.language.{
  LanguageMajorVersion,
  LanguageVersion,
  PackageInterface,
  StablePackages,
}
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.logging.LoggingContext
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.tagToContainer
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import org.scalactic.Equality
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertion, EitherValues}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.annotation.nowarn
import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.math.Ordered.orderingToOrdered

class EngineTestV2 extends EngineTest(LanguageMajorVersion.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class EngineTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with SecurityTestSuite {

  val helpers = new EngineTestHelpers(majorLanguageVersion)
  import helpers._

  "minimal create command" should {
    val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val let = Time.Timestamp.now()
    val command =
      ApiCommand.Create(
        id.toRef,
        ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party)))),
      )
    val submissionSeed = hash("minimal create command")
    val submitters = Set(party)
    val readAs = (Set.empty: Set[Party])
    val res = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]
    val interpretResult = suffixLenientEngine
      .submit(
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(ImmArray(command), let, "test"),
        disclosures = ImmArray.empty,
        participantId = participant,
        submissionSeed = submissionSeed,
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
      ApiCommand.Create(
        id(templateId).toRef,
        ValueRecord(Some(id(templateId)), templateArgs.to(ImmArray)),
      )
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
        .preprocessApiCommands(Map.empty, ImmArray(cmd))
        .consume(lookupContract, lookupPackage, lookupKey)
      withClue("Preprocessing result: ")(res shouldBe a[Right[_, _]])

      suffixLenientEngine
        .submit(
          submitters = actAs,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(cmd), let, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
        )
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
          .consume(lookupContract, lookupPackage, lookupKey)
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
      ApiCommand.Exercise(
        templateId.toRef,
        cid,
        "Hello",
        ValueRecord(Some(hello), ImmArray.Empty),
      )
    val submitters = Set(party)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
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
            .consume(lookupContract, lookupPackage, lookupKey)
        }
    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      val Right((rtx, _)) = suffixLenientEngine
        .submit(
          submitters = Set(party),
          readAs = readAs,
          cmds = ApiCommands(ImmArray(command), let, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
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
        .consume(lookupContract, lookupPackage, lookupKey)
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
    val command = ApiCommand.ExerciseByKey(
      templateId.toRef,
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(43)))),
      "SumToK",
      ValueRecord(None, ImmArray((None, ValueInt64(5)))),
    )
    val submitters = Set(alice)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe a[Right[_, _]]

    "fail at submission" in {
      val submitResult = suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(command), let, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
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
    val command = ApiCommand.ExerciseByKey(
      templateId.toRef,
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
      "SumToK",
      ValueRecord(None, ImmArray((None, ValueInt64(5)))),
    )
    val submitters = Set(alice)
    val readAs = (Set.empty: Set[Party])

    val res = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
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
            .consume(lookupContract, lookupPackage, lookupKey)
        }
    val Right((tx, txMeta)) = result

    "be translated" in {
      val Right((rtx, _)) = suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(command), let, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
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
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.message)
        case Right(()) => ()
      }
    }

    "mark all the exercise nodes as performed byKey" in {
      val expectedNodes = tx.nodes.collect { case (id, _: Node.Exercise) =>
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
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty set of maintainers"
        )
      }
    }
  }

  "Daml exercise-by-key" should {
    val seed = hash("exercise-by-key")
    val now = Time.Timestamp.now()

    "create a Exercise node with flag byKey without Fetch REMY" in {

      val tmplId = Identifier(basicTestsPkgId, "BasicTests:ExerciseByKey")
      val cmds = ImmArray(
        speedy.Command.CreateAndExercise(
          templateId = tmplId,
          createArgument =
            SRecord(tmplId, ImmArray(Ref.Name.assertFromString("p")), ArrayList(SParty(alice))),
          choiceId = ChoiceName.assertFromString("Exercise"),
          choiceArgument = SUnit,
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
        .consume(lookupContract, lookupPackage, lookupKey)

      inside(result) { case Right((tx, _)) =>
        inside(tx.roots.map(tx.nodes)) { case ImmArray(create: Node.Create, exe: Node.Exercise) =>
          create.templateId shouldBe tmplId
          exe.templateId shouldBe tmplId
          inside(exe.children.map(tx.nodes)) { case ImmArray(exeByKey: Node.Exercise) =>
            exeByKey.templateId shouldBe Identifier(basicTestsPkgId, "BasicTests:WithKey")
            exeByKey.children shouldBe ImmArray.empty
            exeByKey.byKey shouldBe true
          }
        }
      }
    }
  }

  "fetch-by-key" should {
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
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty set of maintainers"
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
            ImmArray("_1", "_2"),
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
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

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
          SRecord(templateId, ImmArray("p"), ArrayList(SParty(alice))),
          "FetchAfterLookup",
          SRecord(templateId, ImmArray("n"), ArrayList(SInt64(43))),
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
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

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

    "unused disclosed contracts not saved to ledger" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val usedContractSKey = SValue.SRecord(
        templateId,
        ImmArray("_1", "_2").map(Ref.Name.assertFromString),
        values = ArrayList(SValue.SParty(alice), SValue.SInt64(42)),
      )
      val usedContractKey = usedContractSKey.toNormalizedValue(TxVersions.minVersion)
      val unusedContractKey = Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueInt64(69),
        ),
      )
      val usedDisclosedContract = DisclosedContract(
        templateId,
        toContractId("BasicTests:WithKey:1"),
        SValue.SRecord(
          templateId,
          ImmArray(Ref.Name.assertFromString("p"), Ref.Name.assertFromString("k")),
          ArrayList(SValue.SParty(alice), SValue.SInt64(42)),
        ),
        Some(crypto.Hash.assertHashContractKey(templateId, usedContractKey)),
      )
      val unusedDisclosedContract = DisclosedContract(
        templateId,
        toContractId("BasicTests:WithKey:2"),
        SValue.SRecord(
          templateId,
          ImmArray(Ref.Name.assertFromString("p"), Ref.Name.assertFromString("k")),
          ArrayList(SValue.SParty(alice), SValue.SInt64(69)),
        ),
        Some(crypto.Hash.assertHashContractKey(templateId, unusedContractKey)),
      )
      val fetchByKeyCommand = speedy.Command.FetchByKey(
        templateId = templateId,
        key = usedContractSKey,
      )

      val transactionVersion = TxVersions.assignNodeVersion(basicTestsPkg.languageVersion)
      val expectedProcessedDisclosedContract = Node.Create(
        coid = usedDisclosedContract.contractId,
        packageName = getPackageName(basicTestsPkg),
        templateId = usedDisclosedContract.templateId,
        arg = usedDisclosedContract.argument.toNormalizedValue(transactionVersion),
        signatories = Set(alice),
        stakeholders = Set(alice),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(usedDisclosedContract.templateId, usedContractKey, Set(alice))
        ),
        version = transactionVersion,
      )

      ExplicitDisclosureTesting.unusedDisclosedContractsNotSavedToLedger(
        fetchByKeyCommand,
        unusedDisclosedContract,
        usedDisclosedContract,
        expectedProcessedDisclosedContract,
      )
    }
  }

  "create-and-exercise command" should {
    val submissionSeed = hash("create-and-exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)
    val command =
      ApiCommand.CreateAndExercise(
        templateId.toRef,
        ValueRecord(Some(templateId), ImmArray(Some[Name]("p") -> ValueParty(party))),
        "Hello",
        ValueRecord(Some(hello), ImmArray.Empty),
      )

    val submitters = Set(party)

    val res = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
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
            .consume(lookupContract, lookupPackage, lookupKey)
        }

    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      tx.roots should have length 2
      tx.nodes.keySet.toList should have length 2
      val ImmArray(create, exercise) = tx.roots.map(tx.nodes)
      create shouldBe a[Node.Create]
      exercise shouldBe a[Node.Exercise]
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
        .consume(lookupContract, lookupPackage, lookupKey)
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
    val command = ApiCommand.Exercise(
      templateId.toRef,
      originalCoid,
      "Transfer",
      ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))),
    )

    val submitters = Set(bob)
    val readAs = (Set.empty: Set[Party])

    val Right((tx, txMeta)) = suffixLenientEngine
      .submit(
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(ImmArray(command), let, "test"),
        disclosures = ImmArray.empty,
        participantId = participant,
        submissionSeed = submissionSeed,
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    val submissionTime = txMeta.submissionTime

    val txSeed =
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    val Right(cmds) = preprocessor
      .preprocessApiCommands(Map.empty, ImmArray(command))
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
        case exe: Node.Exercise =>
          exe.targetCoid shouldBe originalCoid
          exe.consuming shouldBe true
          exe.actingParties shouldBe Set(bob)
          exe.children.map(_.index) shouldBe ImmArray(1)
          exe.choiceId shouldBe "Transfer"
        case _ => fail("exercise expected first for Bob")
      }

      findNodeByIdx(bobView.nodes, 1).getOrElse(fail("node not found")) match {
        case create: Node.Create =>
          create.templateId shouldBe templateId
          create.stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView =
        Blinding.divulgedTransaction(blindingInfo.disclosure, clara, tx.transaction)

      claraView.nodes.size shouldBe 1
      findNodeByIdx(claraView.nodes, 1).getOrElse(fail("node not found")) match {
        case create: Node.Create =>
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

    val fetcher3Cid = toContractId("4")
    val fetcher3TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(clara)),
      (Some[Name]("obs"), ValueParty(alice)),
      (Some[Name]("fetcher"), ValueParty(party)),
    )

    def makeContract(
        tid: Ref.QualifiedName,
        targs: ImmArray[(Option[Name], Value)],
    ) =
      assertAsVersionedContract(
        ContractInstance(
          basicTestsPkg.name,
          TypeConName(basicTestsPkgId, tid),
          ValueRecord(Some(Identifier(basicTestsPkgId, tid)), targs),
        )
      )

    val lookupContract: PartialFunction[ContractId, VersionedContractInstance] = {
      case `fetchedCid` => makeContract(fetchedStrTid, fetchedTArgs)
      case `fetcher1Cid` => makeContract(fetcherStrTid, fetcher1TArgs)
      case `fetcher2Cid` => makeContract(fetcherStrTid, fetcher2TArgs)
      case `fetcher3Cid` => makeContract(fetcherStrTid, fetcher3TArgs)
    }

    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)

    def actFetchActors(n: Node): Set[Party] = {
      n match {
        case fetch: Node.Fetch => fetch.actingParties
        case _ => Set()
      }
    }

    def txFetchActors(tx: Tx): Set[Party] =
      tx.fold(Set[Party]()) { case (actors, (_, n)) =>
        actors union actFetchActors(n)
      }

    def runExample(cid: ContractId, exerciseActor: Party, readAs: Party) = {
      val command = ApiCommand.Exercise(
        fetcherTid.toRef,
        cid,
        "DoFetch",
        ValueRecord(None, ImmArray((Some[Name]("cid"), ValueContractId(fetchedCid)))),
      )

      val submitters = Set(exerciseActor)

      val res = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)

      res
        .flatMap { cmds =>
          suffixLenientEngine
            .interpretCommands(
              validating = false,
              submitters = submitters,
              readAs = Set(readAs),
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = seeding,
            )
            .consume(lookupContract, lookupPackage, lookupKey)
        }

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" taggedAs SecurityTest(
      Authorization,
      "ledger",
      Attack(
        "ledger api user",
        "try to authorize an action through exercise observers", // i.e. bob
        "only record signatories and actors as fetch actors",
      ),
    ) in {

      // fetch stakeholders: alice, bob, clara

      // alice: parent signatory
      // bob: parent observer
      // clara: parent actor

      val Right((tx, _)) = runExample(fetcher1Cid, clara, alice)
      txFetchActors(tx.transaction) shouldBe Set(alice, clara)
    }

    "not propagate the parent's signatories nor actors when not stakeholders" taggedAs SecurityTest(
      Authorization,
      "ledger",
      Attack(
        "ledger api user",
        "try to fetch a contract without authorization from a stakeholder of the fetched contract",
        "only record stakeholders of the fetched contract as fetch actors", // i.e., clara
      ),
    ) in {

      // fetch stakeholders: alice, bob, clara

      // party: parent signatory
      // alice: parent observer
      // clara: parent actor
      val Right((tx1, _)) = runExample(fetcher2Cid, clara, alice)
      txFetchActors(tx1.transaction) shouldBe Set(clara)

      // clara: parent signatory
      // alice: parent observer
      // party: parent actor
      val Right((tx2, _)) = runExample(fetcher3Cid, party, alice)
      txFetchActors(tx2.transaction) shouldBe Set(clara)
    }

    "be retained when reinterpreting single fetch nodes" in {
      val Right((tx, txMeta)) = runExample(fetcher1Cid, clara, alice)
      val fetchNodes = tx.nodes.iterator.collect { case (nid, fetch: Node.Fetch) =>
        nid -> fetch
      }

      fetchNodes.foreach { case (_, n) =>
        val nid = NodeId(0) // we must use node-0 so the constructed tx is normalized
        val fetchTx = VersionedTransaction(n.version, Map(nid -> n), ImmArray(nid))
        val Right((reinterpreted, _)) =
          suffixLenientEngine
            .reinterpret(
              n.requiredAuthorizers,
              ReplayCommand.Fetch(n.templateId, n.coid),
              txMeta.nodeSeeds.toSeq.collectFirst { case (`nid`, seed) => seed },
              txMeta.submissionTime,
              let,
            )
            .consume(lookupContract, lookupPackage, lookupKey)
        isReplayedBy(fetchTx, reinterpreted) shouldBe Right(())
      }
    }

    "not mark any node as byKey" in {
      runExample(fetcher2Cid, clara, alice).map { case (tx, _) =>
        byKeyNodes(tx).size
      } shouldBe Right(0)
    }
  }

  "reinterpreting fetch nodes" should {

    val fetchedCid = toContractId("1")
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTid = Identifier(basicTestsPkgId, fetchedStrTid)

    val fetchedContract =
      assertAsVersionedContract(
        ContractInstance(
          basicTestsPkg.name,
          TypeConName(basicTestsPkgId, fetchedStrTid),
          ValueRecord(
            Some(Identifier(basicTestsPkgId, fetchedStrTid)),
            ImmArray(
              (Some[Name]("sig1"), ValueParty(alice)),
              (Some[Name]("sig2"), ValueParty(bob)),
              (Some[Name]("obs"), ValueParty(clara)),
            ),
          ),
        )
      )

    val lookupContract: PartialFunction[ContractId, VersionedContractInstance] = {
      case `fetchedCid` => fetchedContract
    }

    "succeed with a fresh engine, correctly compiling packages" in {
      val engine = newEngine()

      val fetchNode = ReplayCommand.Fetch(
        templateId = fetchedTid,
        coid = fetchedCid,
      )

      val let = Time.Timestamp.now()

      val submitters = Set(alice)

      val reinterpreted =
        engine
          .reinterpret(submitters, fetchNode, None, let, let)
          .consume(lookupContract, lookupPackage, lookupKey)

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
          basicTestsPkg.name,
          TypeConName(basicTestsPkgId, lookerUpTemplate),
          ValueRecord(Some(lookerUpTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice)))),
        )
      )

    val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
      case GlobalKeyWithMaintainers(
            GlobalKey(
              BasicTests_WithKey,
              ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
            ),
            _,
          ) =>
        lookedUpCid
    }

    def lookupContract = Map(
      lookedUpCid -> withKeyContractInst,
      lookerUpCid -> lookerUpInst,
    )

    def firstLookupNode(tx: Tx): Option[(NodeId, Node.LookupByKey)] =
      tx.nodes.collectFirst { case (nid, nl: Node.LookupByKey) =>
        nid -> nl
      }

    val now = Time.Timestamp.now()

    "mark all lookupByKey nodes as byKey" in {
      val exerciseCmd = ApiCommand.Exercise(
        lookerUpTemplateId.toRef,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])
      val Right((tx, _)) = newEngine()
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(exerciseCmd), now, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = seed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      val expectedByKeyNodes = tx.transaction.nodes.collect { case (id, _: Node.LookupByKey) =>
        id
      }
      val actualByKeyNodes = byKeyNodes(tx)
      actualByKeyNodes shouldBe Symbol("nonEmpty")
      actualByKeyNodes shouldBe expectedByKeyNodes.toSet
    }

    "be reinterpreted to the same node when lookup finds a contract" in {
      val exerciseCmd = ApiCommand.Exercise(
        lookerUpTemplateId.toRef,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])

      val Right((tx, txMeta)) = suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(exerciseCmd), now, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = seed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe Some(lookedUpCid)

      val Right((reinterpreted, _)) =
        newEngine()
          .reinterpret(
            submitters,
            ReplayCommand.LookupByKey(lookupNode.templateId, lookupNode.key.value),
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted.transaction).map(_._2) shouldEqual Some(lookupNode)
    }

    "be reinterpreted to the same node when lookup doesn't find a contract" in {
      val exerciseCmd = ApiCommand.Exercise(
        lookerUpTemplateId.toRef,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(57)))),
      )
      val submitters = Set(alice)
      val readAs = (Set.empty: Set[Party])

      val Right((tx, txMeta)) = suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(exerciseCmd), now, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = seed,
        )
        .consume(lookupContract, lookupPackage, lookupKey)

      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe None

      val Right((reinterpreted, _)) =
        newEngine()
          .reinterpret(
            submitters,
            ReplayCommand.LookupByKey(lookupNode.templateId, lookupNode.key.value),
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(lookupContract, lookupPackage, lookupKey)

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
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty set of maintainers"
        )
      }
    }

    "unused disclosed contracts not saved to ledger" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val usedContractSKey = SValue.SRecord(
        templateId,
        ImmArray("_1", "_2").map(Ref.Name.assertFromString),
        values = ArrayList(SValue.SParty(alice), SValue.SInt64(42)),
      )
      val usedContractKey = Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueInt64(42),
        ),
      )
      val unusedContractKey = Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueInt64(69),
        ),
      )
      val usedDisclosedContract = DisclosedContract(
        templateId,
        toContractId("BasicTests:WithKey:1"),
        SValue.SRecord(
          templateId,
          ImmArray(Ref.Name.assertFromString("p"), Ref.Name.assertFromString("k")),
          ArrayList(SValue.SParty(alice), SValue.SInt64(42)),
        ),
        Some(crypto.Hash.assertHashContractKey(templateId, usedContractKey)),
      )
      val unusedDisclosedContract = DisclosedContract(
        templateId,
        toContractId("BasicTests:WithKey:2"),
        SValue.SRecord(
          templateId,
          ImmArray(Ref.Name.assertFromString("p"), Ref.Name.assertFromString("k")),
          ArrayList(SValue.SParty(alice), SValue.SInt64(69)),
        ),
        Some(crypto.Hash.assertHashContractKey(templateId, unusedContractKey)),
      )
      val lookupByKeyCommand = speedy.Command.LookupByKey(
        templateId = templateId,
        contractKey = usedContractSKey,
      )

      val transactionVersion = TxVersions.assignNodeVersion(basicTestsPkg.languageVersion)
      val expectedDisclosedEvent = Node.Create(
        coid = usedDisclosedContract.contractId,
        packageName = getPackageName(basicTestsPkg),
        templateId = usedDisclosedContract.templateId,
        arg = usedDisclosedContract.argument.toNormalizedValue(transactionVersion),
        signatories = Set(alice),
        stakeholders = Set(alice),
        keyOpt = Some(
          GlobalKeyWithMaintainers
            .assertBuild(templateId, usedContractKey, Set(alice))
        ),
        version = transactionVersion,
      )

      ExplicitDisclosureTesting.unusedDisclosedContractsNotSavedToLedger(
        lookupByKeyCommand,
        unusedDisclosedContract,
        usedDisclosedContract,
        expectedDisclosedEvent,
      )
    }
  }

  "fetch template" should {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val usedDisclosedContract = DisclosedContract(
      templateId,
      toContractId("BasicTests:Simple:1"),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("p")),
        ArrayList(SValue.SParty(alice)),
      ),
      None,
    )
    val unusedDisclosedContract = DisclosedContract(
      templateId,
      toContractId("BasicTests:Simple:2"),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("p")),
        ArrayList(SValue.SParty(alice)),
      ),
      None,
    )

    "unused disclosed contracts not saved to ledger" in {
      val fetchTemplateCommand = speedy.Command.FetchTemplate(
        templateId = templateId,
        coid = SContractId(usedDisclosedContract.contractId),
      )

      val transactionVersion = TxVersions.assignNodeVersion(basicTestsPkg.languageVersion)
      val expectedDisclosedEvent = Node.Create(
        coid = usedDisclosedContract.contractId,
        packageName = getPackageName(basicTestsPkg),
        templateId = usedDisclosedContract.templateId,
        arg = usedDisclosedContract.argument.toNormalizedValue(transactionVersion),
        signatories = Set(alice),
        stakeholders = Set(alice),
        keyOpt = None,
        version = transactionVersion,
      )

      ExplicitDisclosureTesting.unusedDisclosedContractsNotSavedToLedger(
        fetchTemplateCommand,
        unusedDisclosedContract,
        usedDisclosedContract,
        expectedDisclosedEvent,
      )
    }
  }

  "getTime set dependsOnTime flag" in {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:TimeGetter")

    def run(choiceName: ChoiceName) = {
      val submissionSeed = hash(s"getTime set dependsOnTime flag: ($choiceName)")
      val command =
        ApiCommand.CreateAndExercise(
          templateRef = templateId.toRef,
          createArgument = ValueRecord(None, ImmArray(None -> ValueParty(party))),
          choiceId = choiceName,
          choiceArgument = ValueRecord(None, ImmArray.Empty),
        )
      val submitters = Set(party)
      val readAs = (Set.empty: Set[Party])
      suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(command), Time.Timestamp.now(), "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
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

      val cmd = speedy.Command.FetchTemplate(BasicTests_WithKey, SValue.SContractId(fetchedCid))

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
        .consume(lookupContractMap, lookupPackage, lookupKey)

      tx.transaction.nodes.values.headOption match {
        case Some(fetch: Node.Fetch) =>
          fetch.keyOpt match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(GlobalKeyWithMaintainers(_, maintainers)) =>
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
          basicTestsPkg.name,
          TypeConName(basicTestsPkgId, fetcherTemplate),
          ValueRecord(Some(fetcherTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice)))),
        )
      )

      val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
        case GlobalKeyWithMaintainers(
              GlobalKey(
                BasicTests_WithKey,
                ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
              ),
              _,
            ) =>
          fetchedCid
      }

      val lookupContractMap = Map(fetchedCid -> withKeyContractInst, fetcherCid -> fetcherInst)

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessApiCommands(
          Map.empty,
          ImmArray(
            ApiCommand.Exercise(
              fetcherTemplateId.toRef,
              fetcherCid,
              "Fetch",
              ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))),
            )
          ),
        )
        .consume(lookupContractMap, lookupPackage, lookupKey)

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
        .consume(lookupContractMap, lookupPackage, lookupKey)

      tx.transaction.nodes
        .collectFirst { case (id, nf: Node.Fetch) =>
          nf.keyOpt match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(GlobalKeyWithMaintainers(_, maintainers)) =>
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
        basicTestsPkg.name,
        fetcherId,
        ValueRecord(
          None,
          ImmArray(
            (None, ValueParty(alice)),
            (None, ValueParty(alice)),
            (None, ValueParty(alice)),
          ),
        ),
      )
    )
    val contracts = defaultContracts + (fetcherCid -> fetcherInst)
    val correctCommand =
      ApiCommand.Exercise(
        withKeyId.toRef,
        cid,
        "SumToK",
        ValueRecord(None, ImmArray((None, ValueInt64(42)))),
      )
    val incorrectCommand =
      ApiCommand.Exercise(
        simpleId.toRef,
        cid,
        "Hello",
        ValueRecord(None, ImmArray.Empty),
      )
    val incorrectFetch =
      ApiCommand.Exercise(
        fetcherId.toRef,
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
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(cmds, now, ""),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
        )
        .consume(contracts, lookupPackage, lookupKey)

    // TODO: https://github.com/digital-asset/daml/issues/17082
    // - When `enableContractUpgrading = true`, these tests change behaviour & dont fail with "wrongly typed contract"

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
      val command = ApiCommand.CreateAndExercise(
        templateRef = forkableTemplateId.toRef,
        createArgument = forkableInst,
        choiceId = "Fork",
        choiceArgument = ValueRecord(None, ImmArray((None, ValueInt64(n.toLong)))),
      )
      val submitters = Set(party)
      val readAs = (Set.empty: Set[Party])
      suffixLenientEngine
        .submit(
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(command), let, "test"),
          disclosures = ImmArray.empty,
          participantId = participant,
          submissionSeed = submissionSeed,
        )
        .consume(PartialFunction.empty, lookupPackage, PartialFunction.empty)
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
            .consume(PartialFunction.empty, lookupPackage, PartialFunction.empty)
            .left
            .map(_.message)
        } yield res

      @nowarn("cat=lint-infer-any")
      def assertRun(n: Int): Assertion =
        run(n).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
      assertRun(0)
      assertRun(3)
    }

    "be partially reinterpretable" in {
      val Right((tx, txMeta)) = run(3)
      val stx = suffix(tx)

      val ImmArray(_, exeNode1) = tx.transaction.roots
      val exe = tx.transaction.nodes(exeNode1).asInstanceOf[Node.Exercise]
      val nids = exe.children.toSeq.take(2).toImmArray

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

  "exceptions" should {
    val (exceptionsPkgId, exceptionsPkg, allExceptionsPkgs) =
      loadPackage(s"daml-lf/tests/Exceptions-v${majorLanguageVersion.pretty}.dar")
    val kId = Identifier(exceptionsPkgId, "Exceptions:K")
    val tId = Identifier(exceptionsPkgId, "Exceptions:T")
    val let = Time.Timestamp.now()
    val submissionSeed = hash("rollback")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid = toContractId("1")
    val contracts = Map(
      cid -> assertAsVersionedContract(
        ContractInstance(
          exceptionsPkg.name,
          TypeConName(exceptionsPkgId, "Exceptions:K"),
          ValueRecord(
            None,
            ImmArray(
              (None, ValueParty(party)),
              (None, ValueInt64(666)),
              (None, ValueText("text666")),
            ),
          ),
        )
      )
    )
    val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
      case GlobalKeyWithMaintainers(
            GlobalKey(
              `kId`,
              ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(0)))),
            ),
            _,
          ) =>
        cid
    }

    def run(cmd: ApiCommand) = {
      val submitters = Set(party)
      val Right(cmds) = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(cmd))
        .consume(contracts, allExceptionsPkgs, lookupKey)
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
        .consume(contracts, allExceptionsPkgs, lookupKey)
    }

    "rolled-back archive of transient contract does not prevent consuming choice after rollback" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "RollbackArchiveTransient",
        ValueRecord(None, ImmArray((None, ValueInt64(0)))),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    "archive of transient contract in try prevents consuming choice after try if not rolled back" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ArchiveTransient",
        ValueRecord(None, ImmArray((None, ValueInt64(0)))),
      )
      run(command) shouldBe a[Left[_, _]]
    }
    "rolled-back archive of non-transient contract does not prevent consuming choice after rollback" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "RollbackArchiveNonTransient",
        ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    "archive of non-transient contract in try prevents consuming choice after try if not rolled back" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ArchiveNonTransient",
        ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
      )
      run(command) shouldBe a[Left[_, _]]
    }
    "key updates in rollback node are rolled back" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "RollbackKey",
        ValueRecord(None, ImmArray((None, ValueInt64(0)))),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    "key updates in try are not rolled back if no exception is thrown" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "Key",
        ValueRecord(None, ImmArray((None, ValueInt64(0)))),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    // TEST_EVIDENCE: Integrity: Rollback creates cannot be exercise
    "creates in rollback are rolled back" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ExerciseAfterRollbackCreate",
        ValueRecord(None, ImmArray.empty),
      )
      inside(run(command)) {
        case Left(Interpretation(DamlException(interpretation.Error.ContractNotFound(_)), _)) =>
      }
    }
    "ThrowInHandler" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ThrowInHandler",
        ValueRecord(None, ImmArray.empty),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    "ThrowPureInHandler" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ThrowPureInHandler",
        ValueRecord(None, ImmArray.empty),
      )
      run(command) shouldBe a[Right[_, _]]
    }
    "ThrowPureInHandlerPattern" in {
      val command = ApiCommand.CreateAndExercise(
        tId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "ThrowPureInHandlerPattern",
        ValueRecord(None, ImmArray.empty),
      )
      run(command) shouldBe a[Right[_, _]]
    }
  }

  "action node seeds" should {
    val (exceptionsPkgId, exceptionsPkg, allExceptionsPkgs) =
      loadPackage(s"daml-lf/tests/Exceptions-v${majorLanguageVersion.pretty}.dar")
    val kId = Identifier(exceptionsPkgId, "Exceptions:K")
    val seedId = Identifier(exceptionsPkgId, "Exceptions:NodeSeeds")
    val let = Time.Timestamp.now()
    val submissionSeed = hash("rollback")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid = toContractId("1")
    val contracts = Map(
      cid -> assertAsVersionedContract(
        ContractInstance(
          exceptionsPkg.name,
          TypeConName(exceptionsPkgId, "Exceptions:K"),
          ValueRecord(
            None,
            ImmArray(
              (None, ValueParty(party)),
              (None, ValueInt64(777)),
              (None, ValueText("text777")),
            ),
          ),
        )
      )
    )
    val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
      case GlobalKeyWithMaintainers(
            GlobalKey(
              `kId`,
              ValueRecord(_, ImmArray((_, ValueParty(`party`)), (_, ValueInt64(0)))),
            ),
            _,
          ) =>
        cid
    }

    def run(cmd: ApiCommand) = {
      val submitters = Set(party)
      val Right(cmds) = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(cmd))
        .consume(contracts, allExceptionsPkgs, lookupKey)
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
        .consume(contracts, allExceptionsPkgs, lookupKey)
    }

    "Only create and exercise nodes end up in actionNodeSeeds" in {
      val command = ApiCommand.CreateAndExercise(
        seedId.toRef,
        ValueRecord(None, ImmArray((None, ValueParty(party)))),
        "CreateAllTypes",
        ValueRecord(None, ImmArray((None, ValueContractId(cid)))),
      )
      inside(run(command)) { case Right((tx, meta)) =>
        tx.nodes.size shouldBe 9
        tx.nodes(NodeId(0)) shouldBe a[Node.Create]
        tx.nodes(NodeId(1)) shouldBe a[Node.Exercise]
        tx.nodes(NodeId(2)) shouldBe a[Node.Fetch]
        tx.nodes(NodeId(3)) shouldBe a[Node.LookupByKey]
        tx.nodes(NodeId(4)) shouldBe a[Node.Create]
        tx.nodes(NodeId(5)) shouldBe a[Node.Rollback]
        tx.nodes(NodeId(6)) shouldBe a[Node.Fetch]
        tx.nodes(NodeId(7)) shouldBe a[Node.LookupByKey]
        tx.nodes(NodeId(8)) shouldBe a[Node.Create]
        meta.nodeSeeds.map(_._1.index) shouldBe ImmArray(0, 1, 4, 8)
      }
    }
  }

  "global key lookups" should {
    val (exceptionsPkgId, exceptionsPkg, allExceptionsPkgs) =
      loadPackage(s"daml-lf/tests/Exceptions-v${majorLanguageVersion.pretty}.dar")
    val kId = Identifier(exceptionsPkgId, "Exceptions:K")
    val tId = Identifier(exceptionsPkgId, "Exceptions:GlobalLookups")
    val let = Time.Timestamp.now()
    val submissionSeed = hash("global-keys")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid = toContractId("1")
    val contracts = Map(
      cid -> assertAsVersionedContract(
        ContractInstance(
          exceptionsPkg.name,
          TypeConName(exceptionsPkgId, "Exceptions:K"),
          ValueRecord(
            None,
            ImmArray(
              (None, ValueParty(party)),
              (None, ValueInt64(0)), // matches 0 in the daml code
              (None, ValueText("text0")),
            ),
          ),
        )
      )
    )
    val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
      case GlobalKeyWithMaintainers(
            GlobalKey(
              `kId`,
              ValueRecord(_, ImmArray((_, ValueParty(`party`)), (_, ValueInt64(0)))),
            ),
            _,
          ) =>
        cid
    }

    def run(cmd: ApiCommand): Int = {
      val submitters = Set(party)
      var keyLookups = 0

      val mockedKeyLookup = Function.unlift { (key: GlobalKeyWithMaintainers) =>
        keyLookups += 1
        lookupKey.lift(key)
      }

      val Right(cmds) = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(cmd))
        .consume(contracts, allExceptionsPkgs, mockedKeyLookup)
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
          contracts,
          allExceptionsPkgs,
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
        ("LookupAfterFetch", cidArg, 0),
        ("LookupAfterArchive", cidArg, 0),
        ("LookupAfterRollbackCreate", emptyArg, 0),
        ("LookupAfterRollbackLookup", emptyArg, 1),
        ("LookupAfterArchiveAfterRollbackLookup", cidArg, 1),
      )
      forEvery(cases) { case (choice, argument, lookups) =>
        val command = ApiCommand.CreateAndExercise(
          tId.toRef,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          choice,
          argument,
        )
        run(command) shouldBe lookups
      }
    }
  }

  "Engine.preloadPackage" should {
    import com.daml.lf.language.{LanguageVersion => LV}

    def engine(min: LV, max: LV) =
      new Engine(
        EngineConfig(
          allowedLanguageVersions = VersionRange(min, max),
          requireSuffixedGlobalContractId = true,
        )
      )

    val devVersion = majorLanguageVersion.dev
    val (_, _, allPackagesDev) = new EngineTestHelpers(majorLanguageVersion).loadPackage(
      s"daml-lf/engine/BasicTests-v${majorLanguageVersion.pretty}dev.dar"
    )
    val compatibleLanguageVersions = LanguageVersion.All
    val stablePackages = StablePackages(majorLanguageVersion).allPackages

    s"accept stable packages from ${devVersion} even if version is smaller than min version" in {
      for {
        lv <- compatibleLanguageVersions.filter(_ <= devVersion)
        eng = engine(min = lv, max = devVersion)
        pkg <- stablePackages
        pkgId = pkg.packageId
        pkg <- allPackagesDev.get(pkgId).toList
      } yield eng.preloadPackage(pkgId, pkg) shouldBe a[ResultDone[_]]
    }

    s"reject stable packages from ${devVersion} if version is greater than max version" in {
      for {
        lv <- compatibleLanguageVersions
        eng = engine(min = compatibleLanguageVersions.min, max = lv)
        pkg <- stablePackages
        pkgId = pkg.packageId
        pkg <- allPackagesDev.get(pkgId).toList
      } yield inside(eng.preloadPackage(pkgId, pkg)) {
        case ResultDone(_) => pkg.languageVersion shouldBe <=(lv)
        case ResultError(_) => pkg.languageVersion shouldBe >(lv)
      }
    }
  }
}

class EngineTestAllVersions extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  "Engine.preloadPackage" should {

    import com.daml.lf.language.{LanguageVersion => LV}

    def engine(min: LV, max: LV) =
      new Engine(
        EngineConfig(
          allowedLanguageVersions = VersionRange(min, max),
          requireSuffixedGlobalContractId = true,
        )
      )

    val pkgId = Ref.PackageId.assertFromString("-pkg-")

    def pkg(version: LV) =
      language.Ast.Package(
        Map.empty,
        Set.empty,
        version,
        PackageMetadata(
          PackageName.assertFromString("foo"),
          PackageVersion.assertFromString("0.0.0"),
          None,
        ),
      )

    "reject disallowed packages" in {
      val negativeTestCases = Table(
        ("pkg version", "minVersion", "maxversion"),
        (LV.v2_1, LV.v2_1, LV.v2_dev),
        (LV.v2_dev, LV.v2_1, LV.v2_dev),
      )

      forEvery(negativeTestCases)((v, min, max) =>
        engine(min, max).preloadPackage(pkgId, pkg(v)) shouldBe a[ResultDone[_]]
      )
    }
  }
}

class EngineTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  import Matchers._

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  implicit val resultEq: Equality[Either[Error, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  implicit def toName(s: String): Name =
    Name.assertFromString(s)

  val (basicTestsPkgId, basicTestsPkg, allPackages) = loadPackage(
    s"daml-lf/engine/BasicTests-v${majorLanguageVersion.pretty}.dar"
  )

  val basicTestsSignatures: PackageInterface =
    language.PackageInterface(Map(basicTestsPkgId -> basicTestsPkg))

  val basicUseSharedKeys: Boolean = true

  val party: Ref.IdString.Party = Party.assertFromString("Party")
  val alice: Ref.IdString.Party = Party.assertFromString("Alice")
  val bob: Ref.IdString.Party = Party.assertFromString("Bob")
  val clara: Ref.IdString.Party = Party.assertFromString("Clara")

  val dummySuffix: Bytes = Bytes.assertFromString("00")

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey: lf.data.Ref.ValueRef = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: VersionedContractInstance =
    assertAsVersionedContract(
      ContractInstance(
        basicTestsPkg.name,
        TypeConName(basicTestsPkgId, withKeyTemplate),
        ValueRecord(
          Some(BasicTests_WithKey),
          ImmArray(
            (Some[Ref.Name]("p"), ValueParty(alice)),
            (Some[Ref.Name]("k"), ValueInt64(42)),
          ),
        ),
      )
    )

  val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("BasicTests:Simple:1") ->
        assertAsVersionedContract(
          ContractInstance(
            basicTestsPkg.name,
            TypeConName(basicTestsPkgId, "BasicTests:Simple"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
              ImmArray((Some[Name]("p"), ValueParty(party))),
            ),
          )
        ),
      toContractId("BasicTests:CallablePayout:1") ->
        assertAsVersionedContract(
          ContractInstance(
            basicTestsPkg.name,
            TypeConName(basicTestsPkgId, "BasicTests:CallablePayout"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some[Ref.Name]("giver"), ValueParty(alice)),
                (Some[Ref.Name]("receiver"), ValueParty(bob)),
              ),
            ),
          )
        ),
      toContractId("BasicTests:WithKey:1") ->
        withKeyContractInst,
    )

  val defaultKey = Map(
    GlobalKeyWithMaintainers(
      GlobalKey.assertBuild(
        TypeConName(basicTestsPkgId, withKeyTemplate),
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
      ),
      Set(alice),
    )
      ->
        toContractId("BasicTests:WithKey:1")
  )

  val lookupContract = defaultContracts

  val suffixLenientEngine: Engine = newEngine()
  val suffixStrictEngine: Engine = newEngine(requireCidSuffixes = true)
  val preprocessor =
    new preprocessing.Preprocessor(
      ConcurrentCompiledPackages(suffixLenientEngine.config.getCompilerConfig)
    )

  def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  val lookupPackage = allPackages

  val lookupKey: PartialFunction[GlobalKeyWithMaintainers, ContractId] = {
    case GlobalKeyWithMaintainers(
          GlobalKey(
            BasicTests_WithKey,
            ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ),
          _,
        ) =>
      toContractId("BasicTests:WithKey:1")
  }

  def hash(s: String): Hash = crypto.Hash.hashPrivateKey(s)
  def participant: Ref.IdString.ParticipantId = Ref.ParticipantId.assertFromString("participant")
  def byKeyNodes(tx: VersionedTransaction): Set[NodeId] =
    tx.nodes.collect { case (nodeId, node: Node.Action) if node.byKey => nodeId }.toSet

  def getPackageName(basicTestsPkg: Package): Option[PackageName] =
    Some(basicTestsPkg.metadata.name)

  def newEngine(requireCidSuffixes: Boolean = false) =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = language.LanguageVersion.AllVersions(majorLanguageVersion),
        requireSuffixedGlobalContractId = requireCidSuffixes,
      )
    )

  def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

  def findNodeByIdx[Cid](nodes: Map[NodeId, Node], idx: Int): Option[Node] =
    nodes.collectFirst { case (nodeId, node) if nodeId.index == idx => node }

  def isReplayedBy(
      recorded: VersionedTransaction,
      replayed: VersionedTransaction,
  ): Either[ReplayMismatch, Unit] = {
    // we normalize the LEFT arg before calling isReplayedBy to mimic the effect of serialization
    Validation.isReplayedBy(Normalization.normalizeTx(recorded), replayed)
  }

  def suffix(tx: VersionedTransaction): VersionedTransaction =
    data.assertRight(tx.suffixCid(_ => dummySuffix))

  // Mimics Canton reinterpretation
  // requires a suffixed transaction.
  def reinterpret(
      engine: Engine,
      submitters: Set[Party],
      nodes: ImmArray[NodeId],
      tx: VersionedTransaction,
      txMeta: Tx.Metadata,
      ledgerEffectiveTime: Time.Timestamp,
      lookupPackages: PartialFunction[PackageId, Package],
      contracts: Map[ContractId, VersionedContractInstance] = Map.empty,
      keys: Map[GlobalKeyWithMaintainers, ContractId] = Map.empty,
  ): Either[Error, (VersionedTransaction, Tx.Metadata)] = {

    val nodeSeedMap = txMeta.nodeSeeds.toSeq.toMap

    val finalState =
      nodes.foldLeft[Either[Error, ReinterpretState]](Right(ReinterpretState(contracts, keys))) {
        case (acc, nodeId) =>
          for {
            state <- acc
            cmd = tx.transaction.nodes(nodeId) match {
              case create: Node.Create =>
                ReplayCommand.Create(create.templateId, create.arg)
              case fetch: Node.Fetch if fetch.byKey =>
                val key = fetch.keyOpt.getOrElse(sys.error("unexpected empty contract key")).value
                ReplayCommand.FetchByKey(fetch.templateId, key)
              case fetch: Node.Fetch =>
                ReplayCommand.Fetch(fetch.templateId, fetch.coid)
              case lookup: Node.LookupByKey =>
                ReplayCommand.LookupByKey(lookup.templateId, lookup.key.value)
              case exe: Node.Exercise if exe.byKey =>
                val key = exe.keyOpt.getOrElse(sys.error("unexpected empty contract key")).value
                ReplayCommand.ExerciseByKey(
                  exe.templateId,
                  key,
                  exe.choiceId,
                  exe.chosenValue,
                )
              case exe: Node.Exercise =>
                ReplayCommand.Exercise(
                  exe.templateId,
                  exe.interfaceId,
                  exe.targetCoid,
                  exe.choiceId,
                  exe.chosenValue,
                )
              case _: Node.Rollback =>
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
                state.contracts,
                lookupPackages,
                state.keys,
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
          Tx(state.nodes, state.roots.toImmArray)
        ),
        Tx.Metadata(
          submissionSeed = None,
          submissionTime = txMeta.submissionTime,
          usedPackages = Set.empty,
          dependsOnTime = state.dependsOnTime,
          nodeSeeds = state.nodeSeeds.toImmArray,
          globalKeyMapping = Map.empty,
          disclosedEvents = ImmArray.empty,
        ),
      )
    )
  }

  object ExplicitDisclosureTesting {
    def unusedDisclosedContractsNotSavedToLedger(
        cmd: speedy.Command,
        unusedDisclosedContract: DisclosedContract,
        usedDisclosedContract: DisclosedContract,
        expectedDisclosedEvent: Node.Create,
    ): Assertion = {
      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          readAs = Set.empty,
          commands = ImmArray(cmd),
          ledgerTime = Time.Timestamp.now(),
          submissionTime = Time.Timestamp.now(),
          seeding = InitialSeeding.TransactionSeed(hash(s"$cmd")),
          disclosures = ImmArray(unusedDisclosedContract, usedDisclosedContract),
        )
        .consume(PartialFunction.empty, lookupPackage, lookupKey)

      inside(result) { case Right((transaction, metadata)) =>
        transaction should haveDisclosedInputContracts(usedDisclosedContract)
        metadata should haveDisclosedEvents(expectedDisclosedEvent)
      }
    }

    @SuppressWarnings(
      Array(
        "org.wartremover.warts.JavaSerializable",
        "org.wartremover.warts.Product",
        "org.wartremover.warts.Serializable",
      )
    )
    def haveDisclosedEvents(
        expectedProcessedDisclosedContracts: Node.Create*
    ): Matcher[Tx.Metadata] =
      Matcher { metadata =>
        val expectedResult = ImmArray(expectedProcessedDisclosedContracts: _*)
        val actualResult = metadata.disclosedEvents

        val debugMessage = Seq(
          s"expected but missing contract IDs: ${expectedResult.filter(!actualResult.toSeq.contains(_)).map(_.coid)}",
          s"unexpected but found contract IDs: ${actualResult.filter(!expectedResult.toSeq.contains(_)).map(_.coid)}",
        ).mkString("\n  ", "\n  ", "")

        MatchResult(
          expectedResult == actualResult,
          s"Failed with unexpected disclosed contracts: $expectedResult != $actualResult $debugMessage",
          s"Failed with unexpected disclosed contracts: $expectedResult == $actualResult",
        )
      }

    def haveDisclosedInputContracts(
        disclosedContracts: DisclosedContract*
    ): Matcher[VersionedTransaction] =
      Matcher { transaction =>
        val expectedResult = disclosedContracts.map(_.contractId).toSet
        val actualResult = transaction.inputContracts
        val debugMessage = Seq(
          s"expected but missing contract IDs: ${expectedResult.filter(!actualResult.contains(_))}",
          s"unexpected but found contract IDs: ${actualResult.filter(!expectedResult.contains(_))}",
        ).mkString("\n  ", "\n  ", "")

        MatchResult(
          expectedResult == actualResult,
          s"Failed with unexpected disclosed contracts: $expectedResult != $actualResult $debugMessage",
          s"Failed with unexpected disclosed contracts: $expectedResult == $actualResult",
        )
      }
  }

  case class ReinterpretState(
      contracts: Map[ContractId, VersionedContractInstance],
      keys: Map[GlobalKeyWithMaintainers, ContractId],
      nodes: HashMap[NodeId, Node] = HashMap.empty,
      roots: BackStack[NodeId] = BackStack.empty,
      dependsOnTime: Boolean = false,
      nodeSeeds: BackStack[(NodeId, crypto.Hash)] = BackStack.empty,
  ) {
    def commit(tr: Tx, meta: Tx.Metadata): ReinterpretState = {
      val (newContracts, newKeys) = tr.fold((contracts, keys)) {
        case ((contracts, keys), (_, exe: Node.Exercise)) =>
          (contracts - exe.targetCoid, keys)
        case ((contracts, keys), (_, create: Node.Create)) =>
          (
            contracts.updated(
              create.coid,
              create.versionedCoinst,
            ),
            create.keyOpt.fold(keys)(k => keys.updated(k, create.coid)),
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
}
