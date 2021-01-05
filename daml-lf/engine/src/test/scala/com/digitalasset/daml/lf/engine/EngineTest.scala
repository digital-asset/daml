// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.util
import java.io.File

import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  SubmittedTransaction,
  VersionedTransaction,
  GenTransaction => GenTx,
  Transaction => Tx,
  TransactionVersion => TxVersions
}
import com.daml.lf.value.Value
import Value._
import com.daml.lf.speedy.{InitialSeeding, SValue, svalue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.command._
import com.daml.lf.transaction.Node.GenNode
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedValue
import org.scalactic.Equality
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._
import scalaz.std.either._
import scalaz.syntax.apply._

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product"
  ))
class EngineTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import EngineTest._

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

  private[this] def byKeyNodes[Nid, Cid](tx: VersionedTransaction[Nid, Cid]) =
    tx.nodes.collect { case (nodeId, node) if node.byKey => nodeId }.toSet

  private val party = Party.assertFromString("Party")
  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")
  private val clara = Party.assertFromString("Clara")

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages =
      UniversalArchiveReader().readFile(new File(rlocation(resource))).get
    val packagesMap = Map(packages.all.map {
      case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
    }: _*)
    val (mainPkgId, mainPkgArchive) = packages.main
    val mainPkg = Decode.readArchivePayloadAndVersion(mainPkgId, mainPkgArchive)._1._2
    (mainPkgId, mainPkg, packagesMap)
  }

  private val (basicTestsPkgId, basicTestsPkg, allPackages) = loadPackage(
    "daml-lf/tests/BasicTests.dar")

  val basicTestsSignatures = toSignature(basicTestsPkg)

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: ContractInst[Value.VersionedValue[ContractId]] =
    ContractInst(
      TypeConName(basicTestsPkgId, withKeyTemplate),
      assertAsVersionedValue(
        ValueRecord(
          Some(BasicTests_WithKey),
          ImmArray(
            (Some("p"), ValueParty(alice)),
            (Some("k"), ValueInt64(42))
          ))),
      ""
    )

  val defaultContracts: Map[ContractId, ContractInst[Value.VersionedValue[ContractId]]] =
    Map(
      toContractId("#BasicTests:Simple:1") ->
        ContractInst(
          TypeConName(basicTestsPkgId, "BasicTests:Simple"),
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
              ImmArray((Some[Name]("p"), ValueParty(party))))),
          ""
        ),
      toContractId("#BasicTests:CallablePayout:1") ->
        ContractInst(
          TypeConName(basicTestsPkgId, "BasicTests:CallablePayout"),
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some[Ref.Name]("giver"), ValueParty(alice)),
                (Some[Ref.Name]("receiver"), ValueParty(bob))
              )
            )),
          ""
        ),
      toContractId("#BasicTests:WithKey:1") ->
        withKeyContractInst
    )

  val lookupContract = defaultContracts.get(_)

  def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
    (key.globalKey.templateId, key.globalKey.key) match {
      case (
          BasicTests_WithKey,
          ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ) =>
        Some(toContractId("#BasicTests:WithKey:1"))
      case _ =>
        None
    }

  // TODO make these two per-test, so that we make sure not to pollute the package cache and other possibly mutable stuff
  val engine = Engine.DevEngine()
  val preprocessor =
    new preprocessing.Preprocessor(ConcurrentCompiledPackages(engine.config.getCompilerConfig))

  "valid data variant identifier" should {
    "found and return the argument types" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Tree")
      val Right((params, DataVariant(variants))) =
        SignatureLookup.lookupVariant(basicTestsSignatures, id.qualifiedName)
      params should have length 1
      variants.find(_._1 == "Leaf") shouldBe Some(("Leaf", TVar(params(0)._1)))
    }
  }

  "valid data record identifier" should {
    "found and return the argument types" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:MyRec")
      val Right((_, DataRecord(fields))) =
        SignatureLookup.lookupRecord(basicTestsSignatures, id.qualifiedName)
      fields shouldBe ImmArray(("foo", TBuiltin(BTText)))
    }
  }

  "valid template Identifier" should {
    "return the right argument type" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val Right((_, DataRecord(fields))) =
        SignatureLookup.lookupRecord(basicTestsSignatures, id.qualifiedName)
      fields shouldBe ImmArray(("p", TBuiltin(BTParty)))
    }
  }

  "command translation" should {
    "translate create commands argument including labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val command =
        CreateCommand(id, ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create commands argument without labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val command =
        CreateCommand(id, ValueRecord(Some(id), ImmArray((None, ValueParty(party)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create commands argument wrong label" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val command =
        CreateCommand(
          id,
          ValueRecord(Some(id), ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(party)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate exercise commands argument including labels" in {
      val originalCoid = toContractId("#BasicTests:CallablePayout:1")
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise commands argument without labels" in {
      val originalCoid = toContractId("#BasicTests:CallablePayout:1")
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        ValueRecord(None, ImmArray((None, ValueParty(clara)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise-by-key commands with argument with labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(5))))
      )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise-by-key commands with argument without labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((None, ValueInt64(5))))
      )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate exercise-by-key commands with argument with wrong labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((Some[Name]("WRONG"), ValueInt64(5))))
      )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res.left.value.msg should startWith("Missing record label n for record")
    }

    "not translate exercise-by-key commands if the template specifies no key" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "Transfer",
        ValueRecord(None, ImmArray((None, ValueParty(clara))))
      )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res.left.value.msg should startWith(
        "Impossible to exercise by key, no key is defined for template")
    }

    "not translate exercise-by-key commands if the given key does not match the type specified in the template" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueInt64(42)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((None, ValueInt64(5))))
      )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res.left.value.msg should startWith("mismatching type")
    }

    "translate create-and-exercise commands argument including labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((Some("giver"), ValueParty(clara)), (Some("receiver"), ValueParty(clara)))
          ),
          "Transfer",
          ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara))))
        )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create-and-exercise commands argument without labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((None, ValueParty(clara)), (None, ValueParty(clara)))),
          "Transfer",
          ValueRecord(None, ImmArray((None, ValueParty(clara))))
        )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create-and-exercise commands argument wrong label in create arguments" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((None, ValueParty(clara)), (Some("this_is_not_the_one"), ValueParty(clara)))
          ),
          "Transfer",
          ValueRecord(None, ImmArray((None, ValueParty(clara))))
        )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "not translate create-and-exercise commands argument wrong label in choice arguments" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((None, ValueParty(clara)), (None, ValueParty(clara)))),
          "Transfer",
          ValueRecord(None, ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(clara))))
        )

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate Optional values" in {
      val (optionalPkgId, _, allOptionalPackages) =
        loadPackage("daml-lf/tests/Optional.dar")

      val translator =
        new preprocessing.Preprocessor(ConcurrentCompiledPackages(engine.config.getCompilerConfig))

      val id = Identifier(optionalPkgId, "Optional:Rec")
      val someValue =
        ValueRecord(
          Some(id),
          ImmArray(Some[Name]("recField") -> ValueOptional(Some(ValueText("foo")))))
      val noneValue =
        ValueRecord(Some(id), ImmArray(Some[Name]("recField") -> ValueOptional(None)))
      val typ = TTyConApp(id, ImmArray.empty)

      translator
        .translateValue(typ, someValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldEqual
        Right(SRecord(id, ImmArray("recField"), ArrayList(SOptional(Some(SText("foo"))))))

      translator
        .translateValue(typ, noneValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldEqual
        Right(SRecord(id, ImmArray("recField"), ArrayList(SOptional(None))))

    }

    "returns correct error when resuming" in {
      val translator =
        new preprocessing.Preprocessor(ConcurrentCompiledPackages(engine.config.getCompilerConfig))
      val id = Identifier(basicTestsPkgId, "BasicTests:MyRec")
      val wrongRecord =
        ValueRecord(Some(id), ImmArray(Some[Name]("wrongLbl") -> ValueText("foo")))
      translator
        .translateValue(
          TTyConApp(id, ImmArray.empty),
          wrongRecord
        )
        .consume(lookupContract, lookupPackage, lookupKey) shouldBe 'left
    }
  }

  "minimal create command" should {
    val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val let = Time.Timestamp.now()
    val command =
      CreateCommand(id, ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party)))))
    val submissionSeed = hash("minimal create command")
    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult = engine
      .submit(Set(party), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      interpretResult shouldBe 'right
    }

    "reinterpret to the same result" in {
      val Right((tx, txMeta)) = interpretResult

      val Right((rtx, _)) =
        reinterpret(
          engine,
          Set(party),
          tx.roots,
          tx,
          txMeta,
          let,
          lookupPackage
        )
      Tx.isReplayedBy(tx, rtx) shouldBe Right(())
    }

    "be validated" in {
      val Right((tx, meta)) = interpretResult
      val Right(submitter) = tx.guessSubmitter
      val validated = engine
        .validate(Set(submitter), tx, let, participant, meta.submissionTime, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
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
      (multiPartyTemplate, Set("p1" -> alice, "p2" -> bob), Set(alice, bob, clara))
    )

    def id(templateId: String) = Identifier(basicTestsPkgId, templateId)
    def command(templateId: String, signatories: Set[(String, Party)]) = {
      val templateArgs: Set[(Some[Name], ValueParty)] = signatories.map {
        case (label, party) =>
          Some[Name](label) -> ValueParty(party)
      }
      CreateCommand(id(templateId), ValueRecord(Some(id(templateId)), ImmArray(templateArgs)))
    }

    val let = Time.Timestamp.now()
    val submissionSeed = hash("multi-party create command")

    def interpretResult(
        templateId: String,
        signatories: Set[(String, Party)],
        actAs: Set[Party],
    ) = {
      val cmd = command(templateId, signatories)
      val res = preprocessor
        .preprocessCommands(ImmArray(cmd))
        .consume(lookupContract, lookupPackage, lookupKey)
      withClue("Preprocessing result: ")(res shouldBe 'right)

      engine
        .submit(actAs, Commands(ImmArray(cmd), let, "test"), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
    }

    "be translated" in {
      forAll(cases) {
        case (templateId, signatories, submitters) =>
          interpretResult(templateId, signatories, submitters) shouldBe 'right
      }
    }

    "reinterpret to the same result" in {
      forAll(cases) {
        case (templateId, signatories, submitters) =>
          val Right((tx, txMeta)) = interpretResult(templateId, signatories, submitters)

          val Right((rtx, _)) =
            reinterpret(
              engine,
              signatories.map(_._2),
              tx.roots,
              tx,
              txMeta,
              let,
              lookupPackage
            )
          Tx.isReplayedBy(tx, rtx) shouldBe Right(())
      }
    }

    "be validated" in {
      forAll(cases) {
        case (templateId, signatories, submitters) =>
          val Right((tx, meta)) = interpretResult(templateId, signatories, submitters)
          val validated = engine
            .validate(submitters, tx, let, participant, meta.submissionTime, submissionSeed)
            .consume(lookupContract, lookupPackage, lookupKey)
          validated match {
            case Left(e) =>
              fail(e.msg)
            case Right(()) => succeed
          }
      }
    }

    "allow replay with a superset of submitters" in {
      forAll(cases) {
        case (templateId, signatories, submitters) =>
          val Right((tx, _)) = interpretResult(templateId, signatories, submitters)

          val replaySubmitters = submitters + party
          val replayResult = engine.replay(
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
      forAll(cases) {
        case (templateId, signatories, submitters) =>
          val Right((tx, _)) = interpretResult(templateId, signatories, submitters)

          val replaySubmitters = submitters.drop(1)
          val replayResult = engine.replay(
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
    val cid = toContractId("#BasicTests:Simple:1")
    val command =
      ExerciseCommand(templateId, cid, "Hello", ValueRecord(Some(hello), ImmArray.empty))

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res
        .flatMap {
          case (cmds, globalCids) =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(party),
                commands = cmds,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
                globalCids = globalCids,
              )
              .consume(lookupContract, lookupPackage, lookupKey)
        }
    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      val Right((rtx, _)) = engine
        .submit(Set(party), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      Tx.isReplayedBy(tx, rtx) shouldBe Right(())
    }

    "reinterpret to the same result" in {
      val Right((rtx, _)) =
        reinterpret(
          engine,
          Set(party),
          tx.roots,
          tx,
          txMeta,
          let,
          lookupPackage,
          defaultContracts
        )
      Tx.isReplayedBy(tx, rtx) shouldBe Right(())
    }

    "be validated" in {
      val validated = engine
        .validate(Set(submitter), tx, let, participant, let, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }

    "events are collected" in {
      val blindingInfo = Blinding.blind(tx)
      val events = Event.collectEvents(tx.transaction, blindingInfo.disclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains party)
      partyEvents.size shouldBe 1
      partyEvents(0) match {
        case _: ExerciseEvent[NodeId, ContractId] => succeed
        case _ => fail("expected exercise")
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
      ValueRecord(None, ImmArray((None, ValueInt64(5))))
    )

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right

    "fail at submission" in {
      val submitResult = engine
        .submit(Set(alice), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      submitResult.left.value.msg should startWith("dependency error: couldn't find key")
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
      ValueRecord(None, ImmArray((None, ValueInt64(5))))
    )

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val result =
      res
        .flatMap {
          case (cmds, globalCids) =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(alice),
                commands = cmds,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
                globalCids = globalCids,
              )
              .consume(lookupContract, lookupPackage, lookupKey)
        }
    val Right((tx, txMeta)) = result
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      val submitResult = engine
        .submit(Set(alice), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
        .map(_._1)
      (result.map(_._1) |@| submitResult)((tx, rtx) => Tx.isReplayedBy(tx, rtx)) shouldBe Right(
        Right(()))
    }

    "reinterpret to the same result" in {

      val reinterpretResult =
        reinterpret(engine, Set(alice), tx.roots, tx, txMeta, let, lookupPackage, defaultContracts)
          .map(_._1)
      (result.map(_._1) |@| reinterpretResult)((tx, rtx) => Tx.isReplayedBy(tx, rtx)) shouldBe Right(
        Right(()))
    }

    "be validated" in {
      val validated = engine
        .validate(Set(submitter), tx, let, participant, let, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }

    "events are collected" in {
      val blindingInfo = Blinding.blind(tx)
      val events = Event.collectEvents(tx.transaction, blindingInfo.disclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains alice)
      partyEvents.size shouldBe 1
      partyEvents.head match {
        case ExerciseEvent(_, _, _, _, _, _, _, _, _, _) => succeed
        case _ => fail("expected exercise")
      }
    }

    "mark all the exercise nodes as performed byKey" in {
      val expectedNodes = tx.nodes.collect {
        case (id, _: Node.NodeExercises[_, _]) => id
      }
      val actualNodes = byKeyNodes(tx)
      actualNodes shouldBe 'nonEmpty
      actualNodes shouldBe expectedNodes.toSet
    }
  }

  "exercise-by-key" should {
    val seed = hash("exercise-by-key")

    val now = Time.Timestamp.now

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

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
          globalCids = Set.empty,
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) {
        case Left(err) =>
          err.msg should include(
            "Update failed due to a contract key with an empty sey of maintainers")
      }
    }
  }

  "fecth-by-key" should {
    val seed = hash("fetch-by-key")

    val now = Time.Timestamp.now

    "crash if use a contract key with an empty set of maintainers" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")

      val cmds = ImmArray(
        speedy.Command.FetchByKey(
          templateId = templateId,
          key = SParty(alice),
        )
      )

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
          globalCids = Set.empty,
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) {
        case Left(err) =>
          err.msg should include(
            "Update failed due to a contract key with an empty sey of maintainers")
      }
    }

    "crash if the contract key does not exists" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")

      val cmds = ImmArray(
        speedy.Command.FetchByKey(
          templateId = templateId,
          key = SRecord(
            BasicTests_WithKey,
            ImmArray("p", "k"),
            ArrayList(SParty(alice), SInt64(43))
          )
        )
      )

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
          globalCids = Set.empty,
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) {
        case Left(err) =>
          err.msg should include(
            s"couldn't find key GlobalKey($basicTestsPkgId:BasicTests:WithKey, ValueRecord(Some($basicTestsPkgId:BasicTests:WithKey),ImmArray((Some(p),ValueParty(Alice)),(Some(k),ValueInt64(43)))))")
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
        ValueRecord(Some(hello), ImmArray.empty)
      )

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res
        .flatMap {
          case (cmds, globalCids) =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(party),
                commands = cmds,
                ledgerTime = let,
                submissionTime = let,
                seeding = InitialSeeding.TransactionSeed(txSeed),
                globalCids = globalCids,
              )
              .consume(lookupContract, lookupPackage, lookupKey)
        }

    val Right((tx, txMeta)) = interpretResult
    val Right(submitter) = tx.guessSubmitter

    "be translated" in {
      tx.roots should have length 2
      tx.nodes.keySet.toList should have length 2
      val ImmArray(create, exercise) = tx.roots.map(tx.nodes)
      create shouldBe a[Node.NodeCreate[_]]
      exercise shouldBe a[Node.NodeExercises[_, _]]
    }

    "reinterpret to the same result" in {

      val reinterpretResult =
        reinterpret(engine, Set(party), tx.roots, tx, txMeta, let, lookupPackage)
          .map(_._1)
      (interpretResult.map(_._1) |@| reinterpretResult)((tx, rtx) => Tx.isReplayedBy(tx, rtx)) shouldBe Right(
        Right(()))
    }

    "be validated" in {
      val validated = engine
        .validate(Set(submitter), tx, let, participant, let, submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
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

      res shouldEqual Right(SList(FrontStack(ImmArray(SInt64(1)))))
    }

    "translate average list" in {
      val list = ValueList(
        FrontStack(ValueInt64(1), ValueInt64(2), ValueInt64(3), ValueInt64(4), ValueInt64(5)))
      val res = preprocessor
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(
        SValue.SList(FrontStack(ImmArray(SInt64(1), SInt64(2), SInt64(3), SInt64(4), SInt64(5)))))
    }

    "does not translate command with nesting of more than the value limit" in {
      val nested = (1 to 149).foldRight(ValueRecord(None, ImmArray((None, ValueInt64(42))))) {
        case (_, v) => ValueRecord(None, ImmArray((None, v)))
      }
      preprocessor
        .translateValue(
          TTyConApp(TypeConName(basicTestsPkgId, "BasicTests:Nesting0"), ImmArray.empty),
          nested)
        .consume(lookupContract, lookupPackage, lookupKey)
        .left
        .get
        .msg should include("Provided value exceeds maximum nesting level")
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
              )))
        )
      )

      val Right(DDataType(_, ImmArray(), _)) = SignatureLookup
        .lookupDataType(basicTestsSignatures, "BasicTests:MyNestedRec")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:MyNestedRec"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "work with fields with type parameters" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray(
          (Some[Name]("p"), ValueParty(alice)),
          (Some[Name]("v"), ValueOptional(Some(ValueInt64(42)))))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        SignatureLookup.lookupDataType(basicTestsSignatures, "BasicTests:TypeWithParameters")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "work with fields with labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray(
          (Some[Name]("v"), ValueOptional(Some(ValueInt64(42)))),
          (Some[Name]("p"), ValueParty(alice)))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        SignatureLookup.lookupDataType(basicTestsSignatures, "BasicTests:TypeWithParameters")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "fail with fields with labels, with repetitions" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((Some(toName("p")), ValueParty(alice)), (Some(toName("p")), ValueParty(bob)))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        SignatureLookup.lookupDataType(basicTestsSignatures, "BasicTests:TypeWithParameters")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'left
    }

    "work with fields without labels, in right order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueParty(alice)), (None, ValueOptional(Some(ValueInt64(42)))))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        SignatureLookup.lookupDataType(basicTestsSignatures, "BasicTests:TypeWithParameters")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "fail with fields without labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueOptional(Some(ValueInt64(42)))), (None, ValueParty(alice)))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        SignatureLookup.lookupDataType(basicTestsSignatures, "BasicTests:TypeWithParameters")
      val res = preprocessor
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'left
    }

  }

  "exercise callable command" should {
    val submissionSeed = hash("exercise callable command")
    val originalCoid = toContractId("#BasicTests:CallablePayout:1")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
    // we need to fix time as cid are depending on it
    val let = Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")
    val command = ExerciseCommand(
      templateId,
      originalCoid,
      "Transfer",
      ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))))

    val Right((tx, txMeta)) = engine
      .submit(Set(bob), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
      .consume(lookupContract, lookupPackage, lookupKey)

    val submissionTime = txMeta.submissionTime

    val txSeed =
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    val Right((cmds, globalCids)) = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    val Right((rtx, _)) = engine
      .interpretCommands(
        validating = false,
        submitters = Set(bob),
        commands = cmds,
        ledgerTime = let,
        submissionTime = submissionTime,
        seeding = InitialSeeding.TransactionSeed(txSeed),
        globalCids = globalCids,
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      Tx.isReplayedBy(tx, rtx) shouldBe Right(())
    }

    val blindingInfo = Blinding.blind(tx)

    "reinterpret to the same result" in {

      val Right((rtx, _)) =
        reinterpret(
          engine,
          Set(bob),
          tx.transaction.roots,
          tx,
          txMeta,
          let,
          lookupPackage,
          defaultContracts,
        )
      Tx.isReplayedBy(rtx, tx) shouldBe Right(())
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
            _,
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
        case Node.NodeCreate(_, coins, _, _, stakeholders, _, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView =
        Blinding.divulgedTransaction(blindingInfo.disclosure, clara, tx.transaction)

      claraView.nodes.size shouldBe 1
      findNodeByIdx(claraView.nodes, 1).getOrElse(fail("node not found")) match {
        case Node.NodeCreate(_, coins, _, _, stakeholders, _, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }
    }

    "events generated correctly" in {
      // we reinterpret with a static submission time to check precisely hashing of cid
      val submissionTime = let.addMicros(-1000)
      val transactionSeed =
        crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)

      val Right((tx, _)) =
        engine
          .interpretCommands(
            validating = false,
            submitters = Set(bob),
            commands = cmds,
            ledgerTime = let,
            submissionTime = submissionTime,
            seeding = InitialSeeding.TransactionSeed(transactionSeed),
            globalCids,
          )
          .consume(lookupContract, lookupPackage, lookupKey)
      val Seq(_, noid1) = tx.transaction.nodes.keys.toSeq.sortBy(_.index)
      val blindingInfo = Blinding.blind(tx)
      val events = Event.collectEvents(tx.transaction, blindingInfo.disclosure)
      val partyEvents = events.filter(_.witnesses contains bob)
      partyEvents.roots.length shouldBe 1
      val bobExercise = partyEvents.events(partyEvents.roots(0))
      val cid =
        toContractId("00b39433a649bebecd3b01d651be38a75923efdb92f34592b5600aee3fec8a8cc3")
      bobExercise shouldBe
        ExerciseEvent(
          contractId = originalCoid,
          templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout"),
          choice = "Transfer",
          choiceArgument = ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:Transfer")),
            ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))),
          actingParties = Set(bob),
          isConsuming = true,
          children = ImmArray(noid1),
          stakeholders = Set(bob, alice),
          witnesses = Set(bob, alice),
          exerciseResult = Some(ValueContractId(cid))
        )

      val bobVisibleCreate = partyEvents.events(noid1)
      bobVisibleCreate shouldBe
        CreateEvent(
          cid,
          Identifier(basicTestsPkgId, "BasicTests:CallablePayout"),
          None,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray(
              (Some[Name]("giver"), ValueParty(alice)),
              (Some[Name]("receiver"), ValueParty(clara)))
          ),
          "",
          signatories = Set(alice),
          observers = Set(clara), // Clara is implicitly an observer because she controls a choice
          witnesses = Set(bob, clara, alice),
        )
      bobVisibleCreate.asInstanceOf[CreateEvent[_]].stakeholders == Set("Alice", "Clara")
    }
  }

  "dynamic fetch actors" should {
    // Basic test: an exercise (on a "Fetcher" contract) with a single consequence, a fetch of the "Fetched" contract
    // Test a couple of scenarios, with different combination of signatories/observers/actors on the parent action

    val submissionSeed = hash("dynamic fetch actors")
    val fetchedCid = toContractId("#1")
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTArgs = ImmArray(
      (Some[Name]("sig1"), ValueParty(alice)),
      (Some[Name]("sig2"), ValueParty(bob)),
      (Some[Name]("obs"), ValueParty(clara))
    )

    val fetcherStrTid = "BasicTests:Fetcher"
    val fetcherTid = Identifier(basicTestsPkgId, fetcherStrTid)

    val fetcher1Cid = toContractId("#2")
    val fetcher1TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(alice)),
      (Some[Name]("obs"), ValueParty(bob)),
      (Some[Name]("fetcher"), ValueParty(clara)),
    )

    val fetcher2Cid = toContractId("#3")
    val fetcher2TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(party)),
      (Some[Name]("obs"), ValueParty(alice)),
      (Some[Name]("fetcher"), ValueParty(clara)),
    )

    def makeContract[Cid <: ContractId](
        tid: Ref.QualifiedName,
        targs: ImmArray[(Option[Name], Value[Cid])]) =
      ContractInst(
        TypeConName(basicTestsPkgId, tid),
        assertAsVersionedValue(ValueRecord(Some(Identifier(basicTestsPkgId, tid)), targs)),
        ""
      )

    def lookupContract(id: ContractId): Option[ContractInst[Value.VersionedValue[ContractId]]] = {
      id match {
        case `fetchedCid` => Some(makeContract(fetchedStrTid, fetchedTArgs))
        case `fetcher1Cid` => Some(makeContract(fetcherStrTid, fetcher1TArgs))
        case `fetcher2Cid` => Some(makeContract(fetcherStrTid, fetcher2TArgs))
        case _ => None
      }
    }

    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)

    def actFetchActors[Nid, Cid](n: Node.GenNode[Nid, Cid]): Set[Party] = {
      n match {
        case Node.NodeFetch(_, _, _, actingParties, _, _, _, _, _) => actingParties
        case _ => Set()
      }
    }

    def txFetchActors[Nid, Cid](tx: GenTx[Nid, Cid]): Set[Party] =
      tx.fold(Set[Party]()) {
        case (actors, (_, n)) => actors union actFetchActors(n)
      }

    def runExample(cid: ContractId, exerciseActor: Party) = {
      val command = ExerciseCommand(
        fetcherTid,
        cid,
        "DoFetch",
        ValueRecord(None, ImmArray((Some[Name]("cid"), ValueContractId(fetchedCid)))))

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)

      res
        .flatMap {
          case (cmds, globalCids) =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(exerciseActor),
                commands = cmds,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
                globalCids = globalCids,
              )
              .consume(lookupContract, lookupPackage, lookupKey)
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
      val fetchNodes = tx.nodes.iterator.collect {
        case entry @ (_, Node.NodeFetch(_, _, _, _, _, _, _, _, _)) => entry
      }

      fetchNodes.foreach {
        case (nid, n) =>
          val fetchTx = VersionedTransaction(n.version, Map(nid -> n), ImmArray(nid))
          val Right((reinterpreted, _)) =
            engine
              .reinterpret(n.requiredAuthorizers, n, txMeta.nodeSeeds.toSeq.collectFirst {
                case (`nid`, seed) => seed
              }, txMeta.submissionTime, let)
              .consume(lookupContract, lookupPackage, lookupKey)
          Tx.isReplayedBy(fetchTx, reinterpreted) shouldBe Right(())
      }
    }

    "not mark any node as byKey" in {
      runExample(fetcher2Cid, clara).map { case (tx, _) => byKeyNodes(tx).size } shouldBe Right(0)
    }
  }

  "reinterpreting fetch nodes" should {

    val fetchedCid = toContractId("#1")
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTid = Identifier(basicTestsPkgId, fetchedStrTid)

    val fetchedContract = ContractInst(
      TypeConName(basicTestsPkgId, fetchedStrTid),
      assertAsVersionedValue(
        ValueRecord(
          Some(Identifier(basicTestsPkgId, fetchedStrTid)),
          ImmArray(
            (Some[Name]("sig1"), ValueParty(alice)),
            (Some[Name]("sig2"), ValueParty(bob)),
            (Some[Name]("obs"), ValueParty(clara))
          )
        )),
      ""
    )

    def lookupContract(id: ContractId): Option[ContractInst[Value.VersionedValue[ContractId]]] = {
      id match {
        case `fetchedCid` => Some(fetchedContract)
        case _ => None
      }
    }

    "succeed with a fresh engine, correctly compiling packages" in {
      val engine = Engine.DevEngine()

      val fetchNode = Node.NodeFetch(
        coid = fetchedCid,
        templateId = fetchedTid,
        optLocation = None,
        actingParties = Set.empty,
        signatories = Set.empty,
        stakeholders = Set.empty,
        key = None,
        byKey = false,
        version = TxVersions.minVersion,
      )

      val let = Time.Timestamp.now()

      val reinterpreted =
        engine
          .reinterpret(Set(alice), fetchNode, None, let, let)
          .consume(lookupContract, lookupPackage, lookupKey)

      reinterpreted shouldBe 'right
    }

  }

  "lookup by key" should {

    val seed = hash("interpreting lookup by key nodes")

    val lookedUpCid = toContractId("#1")
    val lookerUpTemplate = "BasicTests:LookerUpByKey"
    val lookerUpTemplateId = Identifier(basicTestsPkgId, lookerUpTemplate)
    val lookerUpCid = toContractId("#2")
    val lookerUpInst = ContractInst(
      TypeConName(basicTestsPkgId, lookerUpTemplate),
      assertAsVersionedValue(
        ValueRecord(Some(lookerUpTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice))))),
      ""
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

    def firstLookupNode[Nid, Cid](
        tx: GenTx[Nid, Cid],
    ): Option[(Nid, Node.NodeLookupByKey[Cid])] =
      tx.nodes.collectFirst {
        case (nid, nl @ Node.NodeLookupByKey(_, _, _, _, _)) => nid -> nl
      }

    val now = Time.Timestamp.now()

    "mark all lookupByKey nodes as byKey" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
      val Right((tx, _)) = engine
        .submit(Set(alice), Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val expectedByKeyNodes = tx.transaction.nodes.collect {
        case (id, _: Node.NodeLookupByKey[_]) => id
      }
      val actualByKeyNodes = byKeyNodes(tx)
      actualByKeyNodes shouldBe 'nonEmpty
      actualByKeyNodes shouldBe expectedByKeyNodes.toSet
    }

    "be reinterpreted to the same node when lookup finds a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
      val Right((tx, txMeta)) = engine
        .submit(Set(alice), Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)
      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe Some(lookedUpCid)

      val Right((reinterpreted, _)) =
        Engine
          .DevEngine()
          .reinterpret(
            Set(alice),
            lookupNode,
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted.transaction).map(_._2) shouldEqual Some(lookupNode)
    }

    "be reinterpreted to the same node when lookup doesn't find a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(57)))))
      val Right((tx, txMeta)) = engine
        .submit(Set(alice), Commands(ImmArray(exerciseCmd), now, "test"), participant, seed)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx.transaction)
      lookupNode.result shouldBe None

      val Right((reinterpreted, _)) =
        Engine
          .DevEngine()
          .reinterpret(Set(alice), lookupNode, nodeSeedMap.get(nid), txMeta.submissionTime, now)
          .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted.transaction).map(_._2) shouldEqual Some(lookupNode)
    }

    "crash if use a contract key with an empty set of maintainers" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")

      val cmds = ImmArray(
        speedy.Command.LookupByKey(templateId, SParty(alice))
      )

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(seed),
          globalCids = Set.empty,
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) {
        case Left(err) =>
          err.msg should include(
            "Update failed due to a contract key with an empty sey of maintainers")
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
          choiceArgument = ValueRecord(None, ImmArray.empty),
        )
      engine
        .submit(
          Set(party),
          Commands(ImmArray(command), Time.Timestamp.now(), "test"),
          participant,
          submissionSeed)
        .consume(lookupContract, lookupPackage, lookupKey)
    }

    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)
    run("GetTime").map(_._2.dependsOnTime) shouldBe Right(true)
    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)

  }

  "fetching contracts that have keys correctly fills in the transaction structure" when {
    val fetchedCid = toContractId("#1")
    val now = Time.Timestamp.now()
    val submissionSeed = crypto.Hash.hashPrivateKey(
      "fetching contracts that have keys correctly fills in the transaction structure")
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, now)

    "fetched via a fetch" in {

      val lookupContractMap = Map(fetchedCid -> withKeyContractInst)

      val cmd = speedy.Command.Fetch(BasicTests_WithKey, SValue.SContractId(fetchedCid))

      val Right((tx, _)) = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = ImmArray(cmd),
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          globalCids = Set(fetchedCid),
        )
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      tx.transaction.nodes.values.headOption match {
        case Some(Node.NodeFetch(_, _, _, _, _, _, key, _, _)) =>
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
      val fetcherCid = toContractId("#2")
      val fetcherInst = ContractInst(
        TypeConName(basicTestsPkgId, fetcherTemplate),
        assertAsVersionedValue(
          ValueRecord(Some(fetcherTemplateId), ImmArray((Some[Name]("p"), ValueParty(alice))))),
        ""
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

      val Right((cmds, globalCids)) = preprocessor
        .preprocessCommands(
          ImmArray(
            ExerciseCommand(
              fetcherTemplateId,
              fetcherCid,
              "Fetch",
              ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42))))
            )
          ))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val Right((tx, _)) = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          globalCids = globalCids,
        )
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      tx.transaction.nodes
        .collectFirst {
          case (id, nf: Node.NodeFetch[_]) =>
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

  "nested transactions" should {

    val forkableTemplate = "BasicTests:Forkable"
    val forkableTemplateId = Identifier(basicTestsPkgId, forkableTemplate)
    val forkableInst =
      ValueRecord(
        Some(forkableTemplateId),
        ImmArray(
          (Some[Name]("party"), ValueParty(party)),
          (Some[Name]("parent"), ValueOptional(None)))
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
      engine
        .submit(Set(party), Commands(ImmArray(command), let, "test"), participant, submissionSeed)
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
          submitter <- tx.guessSubmitter.left.map(ValidationError)
          res <- engine
            .validate(Set(submitter), tx, let, participant, metaData.submissionTime, submissionSeed)
            .consume(_ => None, lookupPackage, _ => None)
        } yield res

      run(0).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
      run(3).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
    }

    "be partially reinterpretable" in {
      val Right((tx, txMeta)) = run(3)
      val ImmArray(_, exeNode1) = tx.transaction.roots
      val Node.NodeExercises(_, _, _, _, _, _, _, _, _, _, children, _, _, _, _) =
        tx.transaction.nodes(exeNode1)
      val nids = children.toSeq.take(2).toImmArray

      reinterpret(
        engine,
        Set(party),
        nids,
        tx,
        txMeta,
        let,
        lookupPackage,
      ) shouldBe 'right

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
          ImmArray.empty,
        )

      val Right((cmds, globalCids)) = preprocessor
        .preprocessCommands(ImmArray(
          CreateAndExerciseCommand(templateId, createArg, "DontExecuteCreate", exerciseArg)))
        .consume(_ => None, lookupPackage, lookupKey)

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          globalCids = globalCids,
        )
        .consume(_ => None, lookupPackage, lookupKey)
      result shouldBe 'right
    }

    "be evaluated after ensure clause" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyAfterEnsureClause")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice)))
        )

      val Right((cmds, globalCids)) = preprocessor
        .preprocessCommands(ImmArray(CreateCommand(templateId, createArg)))
        .consume(_ => None, lookupPackage, lookupKey)

      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          globalCids = globalCids,
        )
        .consume(_ => None, lookupPackage, lookupKey)
      result shouldBe 'left
      val Left(err) = result
      err.msg should not include ("Boom")
      err.msg should include("precondition violation")
    }

    "not be create if has an empty set of maintainer" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("sig"), ValueParty(alice)))
        )

      val Right((cmds, globalCids)) = preprocessor
        .preprocessCommands(ImmArray(CreateCommand(templateId, createArg)))
        .consume(_ => None, lookupPackage, lookupKey)
      val result = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = cmds,
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          globalCids = globalCids,
        )
        .consume(_ => None, lookupPackage, lookupKey)

      inside(result) {
        case Left(err) =>
          err.msg should include(
            "Update failed due to a contract key with an empty sey of maintainers")
      }
    }
  }

  "Engine.preloadPackage" should {

    import com.daml.lf.language.{LanguageVersion => LV}

    def engine(min: LV, max: LV) =
      new Engine(
        EngineConfig.Dev.copy(
          allowedLanguageVersions = VersionRange(min, max)
        )
      )

    val pkgId = Ref.PackageId.assertFromString("-pkg-")

    def pkg(version: LV) =
      language.Ast.Package(
        Traversable.empty,
        Traversable.empty,
        version,
        None
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
        engine(min, max).preloadPackage(pkgId, pkg(v)) shouldBe a[ResultDone[_]])

      forEvery(positiveTestCases)((v, min, max) =>
        engine(min, max).preloadPackage(pkgId, pkg(v)) shouldBe a[ResultError])

    }

  }

}

object EngineTest {

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private def toContractId(s: String): ContractId =
    ContractId.assertFromString(s)

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  private def findNodeByIdx[Cid](nodes: Map[NodeId, Node.GenNode[NodeId, Cid]], idx: Int) =
    nodes.collectFirst { case (nodeId, node) if nodeId.index == idx => node }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def resultEq: Equality[Either[Error, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

  private def reinterpret(
      engine: Engine,
      submitters: Set[Party],
      nodes: ImmArray[NodeId],
      tx: Tx.Transaction,
      txMeta: Tx.Metadata,
      ledgerEffectiveTime: Time.Timestamp,
      lookupPackages: PackageId => Option[Package],
      contracts: Map[ContractId, ContractInst[Value.VersionedValue[ContractId]]] = Map.empty,
  ): Either[Error, (Tx.Transaction, Tx.Metadata)] = {
    type Acc =
      (
          HashMap[NodeId, GenNode[NodeId, ContractId]],
          BackStack[NodeId],
          Boolean,
          BackStack[(NodeId, crypto.Hash)],
          Map[ContractId, ContractInst[Value.VersionedValue[ContractId]]],
          Map[GlobalKey, ContractId],
      )
    val nodeSeedMap = txMeta.nodeSeeds.toSeq.toMap

    val iterate =
      nodes.foldLeft[Either[Error, Acc]](
        Right((HashMap.empty, BackStack.empty, false, BackStack.empty, contracts, Map.empty))) {
        case (acc, nodeId) =>
          for {
            previousStep <- acc
            (nodes, roots, dependsOnTime, nodeSeeds, contracts0, keys0) = previousStep
            currentStep <- engine
              .reinterpret(
                submitters,
                tx.transaction.nodes(nodeId),
                nodeSeedMap.get(nodeId),
                txMeta.submissionTime,
                ledgerEffectiveTime)
              .consume(contracts0.get, lookupPackages, k => keys0.get(k.globalKey))
            (tr1, meta1) = currentStep
            (contracts1, keys1) = tr1.transaction.fold((contracts0, keys0)) {
              case (
                  (contracts, keys),
                  (
                    _,
                    Node.NodeExercises(
                      targetCoid: ContractId,
                      _,
                      _,
                      _,
                      consuming @ true,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                    ))) =>
                (contracts - targetCoid, keys)
              case ((contracts, keys), (_, create: Node.NodeCreate[ContractId])) =>
                (
                  contracts.updated(
                    create.coid,
                    create.versionedCoinst,
                  ),
                  create.key.fold(keys)(
                    k =>
                      keys.updated(
                        GlobalKey(
                          create.templateId,
                          k.key.assertNoCid(cid => s"unexpected relative contract ID $cid")),
                        create.coid))
                )
              case (acc, _) => acc
            }
            n = nodes.size
            nodeRenaming = (nid: NodeId) => NodeId(nid.index + n)
            tr = tr1.transaction.mapNodeId(nodeRenaming)
          } yield
            (
              nodes ++ tr.nodes,
              roots :++ tr.roots,
              dependsOnTime || meta1.dependsOnTime,
              nodeSeeds :++ meta1.nodeSeeds.map { case (nid, seed) => nodeRenaming(nid) -> seed },
              contracts1,
              keys1,
            )
      }

    iterate.map {
      case (nodes, roots, dependsOnTime, nodeSeeds, _, _) =>
        (
          TxVersions.asVersionedTransaction(
            roots.toImmArray,
            nodes,
          ),
          Tx.Metadata(
            submissionSeed = None,
            submissionTime = txMeta.submissionTime,
            usedPackages = Set.empty,
            dependsOnTime = dependsOnTime,
            nodeSeeds = nodeSeeds.toImmArray,
          )
        )
    }
  }

}
