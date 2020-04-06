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
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{GenTransaction => GenTx, Transaction => Tx}
import com.daml.lf.value.Value
import Value._
import com.daml.lf.speedy.{SValue, svalue}
import com.daml.lf.speedy.SValue._
import com.daml.lf.command._
import com.daml.lf.value.ValueVersions.assertAsVersionedValue
import org.scalactic.Equality
import org.scalatest.{EitherValues, Matchers, WordSpec}
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
class EngineTest extends WordSpec with Matchers with EitherValues with BazelRunfiles {

  import EngineTest._

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

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

  def lookupContract(@deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(
      ContractInst(
        TypeConName(basicTestsPkgId, "BasicTests:Simple"),
        assertAsVersionedValue(
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
            ImmArray((Some[Name]("p"), ValueParty(party))))),
        ""
      ))
  }

  def lookupContractForPayout(
      @deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(
      ContractInst(
        TypeConName(basicTestsPkgId, "BasicTests:CallablePayout"),
        assertAsVersionedValue(
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray(
              (Some("giver"), ValueParty(alice)),
              (Some("receiver"), ValueParty(bob))
            ))),
        ""
      ))
  }

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: ContractInst[Tx.Value[AbsoluteContractId]] =
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

  def lookupContractWithKey(
      @deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(withKeyContractInst)
  }

  def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  def lookupKey(key: GlobalKey): Option[AbsoluteContractId] =
    (key.templateId, key.key) match {
      case (
          BasicTests_WithKey,
          ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ) =>
        Some(toContractId("#1"))
      case _ =>
        None
    }

  // TODO make these two per-test, so that we make sure not to pollute the package cache and other possibly mutable stuff
  val engine = Engine()
  val commandTranslator = new CommandPreprocessor(ConcurrentCompiledPackages())

  "valid data variant identifier" should {
    "found and return the argument types" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Tree")
      val Right((params, DataVariant(variants))) =
        PackageLookup.lookupVariant(basicTestsPkg, id.qualifiedName)
      params should have length 1
      variants.find(_._1 == "Leaf") shouldBe Some(("Leaf", TVar(params(0)._1)))
    }
  }

  "valid data record identifier" should {
    "found and return the argument types" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:MyRec")
      val Right((_, DataRecord(fields, _))) =
        PackageLookup.lookupRecord(basicTestsPkg, id.qualifiedName)
      fields shouldBe ImmArray(("foo", TBuiltin(BTText)))
    }
  }

  "valid template Identifier" should {
    "return the right argument type" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val Right((_, DataRecord(fields, tpl))) =
        PackageLookup.lookupRecord(basicTestsPkg, id.qualifiedName)
      fields shouldBe ImmArray(("p", TBuiltin(BTParty)))
      tpl.isEmpty shouldBe false
    }
  }

  "command translation" should {
    "translate create commands argument including labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(id, ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party)))))

      val res = commandTranslator
        .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create commands argument without labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(id, ValueRecord(Some(id), ImmArray((None, ValueParty(party)))))

      val res = commandTranslator
        .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create commands argument wrong label" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(
          id,
          ValueRecord(Some(id), ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(party)))))

      val res = commandTranslator
        .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate exercise commands argument including labels" in {
      val originalCoid = toContractId("#1")
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))))

      val res = commandTranslator
        .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise commands argument without labels" in {
      val originalCoid = toContractId("#1")
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        ValueRecord(None, ImmArray((None, ValueParty(clara)))))

      val res = commandTranslator
        .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise-by-key commands with argument with labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val let = Time.Timestamp.now()
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(5))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise-by-key commands with argument without labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val let = Time.Timestamp.now()
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((None, ValueInt64(5))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate exercise-by-key commands with argument with wrong labels" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val let = Time.Timestamp.now()
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((Some[Name]("WRONG"), ValueInt64(5))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res.left.value.msg should startWith("Missing record label n for record")
    }

    "not translate exercise-by-key commands if the template specifies no key" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
        "Transfer",
        ValueRecord(None, ImmArray((None, ValueParty(clara))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res.left.value.msg should startWith(
        "Impossible to exercise by key, no key is defined for template")
    }

    "not translate exercise-by-key commands if the given key does not match the type specified in the template" in {
      val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
      val let = Time.Timestamp.now()
      val command = ExerciseByKeyCommand(
        templateId,
        ValueRecord(None, ImmArray((None, ValueInt64(42)), (None, ValueInt64(42)))),
        "SumToK",
        ValueRecord(None, ImmArray((None, ValueInt64(5))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res.left.value.msg should startWith("mismatching type")
    }

    "translate create-and-exercise commands argument including labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
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

      val res = commandTranslator
        .preprocessCommands(Commands(clara, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create-and-exercise commands argument without labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((None, ValueParty(clara)), (None, ValueParty(clara)))),
          "Transfer",
          ValueRecord(None, ImmArray((None, ValueParty(clara))))
        )

      val res = commandTranslator
        .preprocessCommands(Commands(clara, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create-and-exercise commands argument wrong label in create arguments" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
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

      val res = commandTranslator
        .preprocessCommands(Commands(clara, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "not translate create-and-exercise commands argument wrong label in choice arguments" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray((None, ValueParty(clara)), (None, ValueParty(clara)))),
          "Transfer",
          ValueRecord(None, ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(clara))))
        )

      val res = commandTranslator
        .preprocessCommands(Commands(clara, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate Optional values" in {
      val (optionalPkgId, optionalPkg @ _, allOptionalPackages) =
        loadPackage("daml-lf/tests/Optional.dar")

      val translator = new CommandPreprocessor(ConcurrentCompiledPackages.apply())

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
        Right(SRecord(id, Name.Array("recField"), ArrayList(SOptional(Some(SText("foo"))))))

      translator
        .translateValue(typ, noneValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldEqual
        Right(SRecord(id, Name.Array("recField"), ArrayList(SOptional(None))))

    }

    "returns correct error when resuming" in {
      val translator = new CommandPreprocessor(ConcurrentCompiledPackages.apply())
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
    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult = engine
      .submit(Commands(party, ImmArray(command), let, "test"), participant, Some(submissionSeed))
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      interpretResult shouldBe 'right
    }

    "reinterpret to the same result" in {
      val Right((tx, metaData)) = interpretResult
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val Right((rtx, _)) =
        engine
          .reinterpret(
            Some(submissionSeed -> metaData.submissionTime),
            participant,
            Set(party),
            txRoots,
            let)
          .consume(lookupContract, lookupPackage, lookupKey)
      (tx isReplayedBy rtx) shouldBe true
    }

    "be validated" in {
      val Right((tx, meta)) = interpretResult
      val validated = engine
        .validate(tx, let, participant, Some(submissionSeed -> meta.submissionTime))
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
  }

  "exercise command" should {
    val submissionSeed = hash("exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val transactionSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)
    val command =
      ExerciseCommand(
        templateId,
        toContractId("#1"),
        "Hello",
        ValueRecord(Some(hello), ImmArray.empty))

    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                checkSubmitterInMaintainers = true,
                submitters = Set(party),
                commands = r,
                ledgerTime = let,
                transactionSeedAndSubmissionTime = Some(transactionSeed -> let)
              )
              .consume(lookupContract, lookupPackage, lookupKey))
    val Right((tx, _)) = interpretResult

    "be translated" in {
      val Right((rtx, _)) = engine
        .submit(Commands(party, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
      (tx isReplayedBy rtx) shouldBe true
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val Right((rtx, _)) = engine
        .reinterpret(Some(submissionSeed -> let), participant, Set(party), txRoots, let)
        .consume(lookupContract, lookupPackage, lookupKey)
      (tx isReplayedBy rtx) shouldBe true
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, Some(submissionSeed -> let))
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }

    "events are collected" in {
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set(party))
      val events = Event.collectEvents(tx, blindingInfo.disclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains party)
      partyEvents.size shouldBe 1
      partyEvents(0) match {
        case _: ExerciseEvent[Tx.NodeId, ContractId, Tx.Value[ContractId]] => succeed
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

    val res = commandTranslator
      .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
      .consume(lookupContractWithKey, lookupPackage, lookupKey)
    res shouldBe 'right

    "fail at submission" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContractWithKey, lookupPackage, lookupKey)
      submitResult.left.value.msg should startWith("dependency error: couldn't find key")
    }
  }

  "exercise-by-key command with existing key" should {
    val submissionSeed = hash("exercise-by-key command with existing key")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val transactionSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)
    val command = ExerciseByKeyCommand(
      templateId,
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
      "SumToK",
      ValueRecord(None, ImmArray((None, ValueInt64(5))))
    )

    val res = commandTranslator
      .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
      .consume(lookupContractWithKey, lookupPackage, lookupKey)
    res shouldBe 'right
    val result =
      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                checkSubmitterInMaintainers = true,
                submitters = Set(alice),
                commands = r,
                ledgerTime = let,
                transactionSeedAndSubmissionTime = Some(transactionSeed -> let)
              )
              .consume(lookupContractWithKey, lookupPackage, lookupKey))
        .map(_._1)
    val tx = result.right.value

    "be translated" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContractWithKey, lookupPackage, lookupKey)
        .map(_._1)
      (result |@| submitResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Some(submissionSeed -> let), participant, Set(alice), txRoots, let)
          .consume(lookupContractWithKey, lookupPackage, lookupKey)
          .map(_._1)
      (result |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, Some(submissionSeed -> let))
        .consume(lookupContractWithKey, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }

    "events are collected" in {
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set(alice))
      val events = Event.collectEvents(tx, blindingInfo.disclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains alice)
      partyEvents.size shouldBe 1
      partyEvents.head match {
        case ExerciseEvent(_, _, _, _, _, _, _, _, _, _) => succeed
        case _ => fail("expected exercise")
      }
    }
  }

  "create-and-exercise command" should {
    val submissionSeed = hash("create-and-exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val transactionSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)
    val command =
      CreateAndExerciseCommand(
        templateId,
        ValueRecord(Some(templateId), ImmArray(Some[Name]("p") -> ValueParty(party))),
        "Hello",
        ValueRecord(Some(hello), ImmArray.empty)
      )

    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                checkSubmitterInMaintainers = true,
                submitters = Set(party),
                commands = r,
                ledgerTime = let,
                transactionSeedAndSubmissionTime = Some(transactionSeed -> let)
              )
              .consume(lookupContract, lookupPackage, lookupKey))
        .map(_._1)
    val Right(tx) = interpretResult

    "be translated" in {
      tx.roots should have length 2
      tx.nodes.keySet.toList should have length 2
      val ImmArray(create, exercise) = tx.roots.map(tx.nodes)
      create shouldBe a[NodeCreate[_, _]]
      exercise shouldBe a[NodeExercises[_, _, _]]
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Some(submissionSeed -> let), participant, Set(party), txRoots, let)
          .consume(lookupContract, lookupPackage, lookupKey)
          .map(_._1)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, Some(submissionSeed -> let))
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }
  }

  "translate list value" should {
    "translate empty list" in {
      val list = ValueNil
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack.empty))
    }

    "translate singleton" in {
      val list = ValueList(FrontStack(ValueInt64(1)))
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack(ImmArray(SInt64(1)))))
    }

    "translate average list" in {
      val list = ValueList(
        FrontStack(ValueInt64(1), ValueInt64(2), ValueInt64(3), ValueInt64(4), ValueInt64(5)))
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), list)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(
        SValue.SList(FrontStack(ImmArray(SInt64(1), SInt64(2), SInt64(3), SInt64(4), SInt64(5)))))
    }

    "does not translate command with nesting of more than the value limit" in {
      val nested = (1 to 149).foldRight(ValueRecord(None, ImmArray((None, ValueInt64(42))))) {
        case (_, v) => ValueRecord(None, ImmArray((None, v)))
      }
      commandTranslator
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

      val Right(DDataType(_, ImmArray(), _)) = PackageLookup
        .lookupDataType(basicTestsPkg, "BasicTests:MyNestedRec")
      val res = commandTranslator
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
        .translateValue(
          TTyConApp(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters"), ImmArray.empty),
          rec)
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'left
    }

  }

  "exercise callable command" should {
    val submissionSeed = hash("exercise callable command")
    val originalCoid = toContractId("#1")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
    // we need to fix time as cid are depending on it
    val let = Time.Timestamp.assertFromString("1969-07-20T20:17:00Z")
    val command = ExerciseCommand(
      templateId,
      originalCoid,
      "Transfer",
      ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))))

    val Right((tx, meta)) = engine
      .submit(Commands(bob, ImmArray(command), let, "test"), participant, Some(submissionSeed))
      .consume(lookupContractForPayout, lookupPackage, lookupKey)

    val submissionTime = meta.submissionTime

    val transactionSeed =
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    val Right(cmds) = commandTranslator
      .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
      .consume(lookupContractForPayout, lookupPackage, lookupKey)
    val Right((rtx, _)) = engine
      .interpretCommands(
        validating = false,
        checkSubmitterInMaintainers = true,
        submitters = Set(bob),
        commands = cmds,
        ledgerTime = let,
        transactionSeedAndSubmissionTime = Some(transactionSeed -> submissionTime)
      )
      .consume(lookupContractForPayout, lookupPackage, lookupKey)

    "be translated" in {
      (rtx isReplayedBy tx) shouldBe true
    }

    val Right(blindingInfo) =
      Blinding.checkAuthorizationAndBlind(tx, Set(bob))

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val Right((rtx, _)) =
        engine
          .reinterpret(Some(submissionSeed -> submissionTime), participant, Set(bob), txRoots, let)
          .consume(lookupContractForPayout, lookupPackage, lookupKey)
      (rtx isReplayedBy tx) shouldBe true
    }

    "blinded correctly" in {

      // Bob sees both the archive and the create
      val bobView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, bob, tx)
      bobView.nodes.size shouldBe 2
      findNodeByIdx(bobView.nodes, 0).getOrElse(fail("node not found")) match {
        case NodeExercises(
            nodeSeed @ _,
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
            _) =>
          coid shouldBe originalCoid
          consuming shouldBe true
          actingParties shouldBe Set(bob)
          children.map(_.index) shouldBe ImmArray(1)
          choice shouldBe "Transfer"
        case _ => fail("exercise expected first for Bob")
      }

      findNodeByIdx(bobView.nodes, 1).getOrElse(fail("node not found")) match {
        case NodeCreate(nodeSeed @ _, _, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, clara, tx)

      claraView.nodes.size shouldBe 1
      findNodeByIdx(claraView.nodes, 1).getOrElse(fail("node not found")) match {
        case NodeCreate(nodeSeed @ _, _, coins, _, _, stakeholders, _) =>
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
            checkSubmitterInMaintainers = true,
            submitters = Set(bob),
            commands = cmds,
            ledgerTime = let,
            transactionSeedAndSubmissionTime = Some(transactionSeed -> submissionTime)
          )
          .consume(lookupContractForPayout, lookupPackage, lookupKey)
      val Seq(_, noid1) = tx.nodes.keys.toSeq.sortBy(_.index)
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set(bob))
      val events = Event.collectEvents(tx, blindingInfo.disclosure)
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
          choiceArgument = assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Transfer")),
              ImmArray((Some[Name]("newReceiver"), ValueParty(clara))))),
          actingParties = Set(bob),
          isConsuming = true,
          children = ImmArray(noid1),
          stakeholders = Set(bob, alice),
          witnesses = Set(bob, alice),
          exerciseResult = Some(assertAsVersionedValue(ValueContractId(cid)))
        )

      val bobVisibleCreate = partyEvents.events(noid1)
      bobVisibleCreate shouldBe
        CreateEvent(
          cid,
          Identifier(basicTestsPkgId, "BasicTests:CallablePayout"),
          None,
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some[Name]("giver"), ValueParty(alice)),
                (Some[Name]("receiver"), ValueParty(clara)))
            )),
          "",
          signatories = Set(alice),
          observers = Set(clara), // Clara is implicitly an observer because she controls a choice
          witnesses = Set(bob, clara, alice),
        )
      bobVisibleCreate.asInstanceOf[CreateEvent[_, _]].stakeholders == Set("Alice", "Clara")
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
      (Some[Name]("fetcher"), ValueParty(party)),
    )

    def makeContract[Cid <: ContractId](
        tid: Ref.QualifiedName,
        targs: ImmArray[(Option[Name], Value[Cid])]) =
      ContractInst(
        TypeConName(basicTestsPkgId, tid),
        assertAsVersionedValue(ValueRecord(Some(Identifier(basicTestsPkgId, tid)), targs)),
        ""
      )

    def lookupContract(
        id: AbsoluteContractId): Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
      id match {
        case `fetchedCid` => Some(makeContract(fetchedStrTid, fetchedTArgs))
        case `fetcher1Cid` => Some(makeContract(fetcherStrTid, fetcher1TArgs))
        case `fetcher2Cid` => Some(makeContract(fetcherStrTid, fetcher2TArgs))
        case _ => None
      }
    }

    val let = Time.Timestamp.now()
    val transactionSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, let)

    def actFetchActors[Nid, Cid, Val](n: GenNode[Nid, Cid, Val]): Set[Party] = {
      n match {
        case NodeFetch(_, _, _, actingParties, _, _, _) => actingParties.getOrElse(Set.empty)
        case _ => Set()
      }
    }

    def txFetchActors[Nid, Cid, Val](tx: GenTx[Nid, Cid, Val]): Set[Party] =
      tx.fold(Set[Party]()) {
        case (actors, (_, n)) => actors union actFetchActors(n)
      }

    def runExample(cid: AbsoluteContractId, exerciseActor: Party) = {
      val command = ExerciseCommand(
        fetcherTid,
        cid,
        "DoFetch",
        ValueRecord(None, ImmArray((Some[Name]("cid"), ValueContractId(fetchedCid)))))

      val res = commandTranslator
        .preprocessCommands(Commands(exerciseActor, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)

      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                checkSubmitterInMaintainers = true,
                submitters = Set(exerciseActor),
                commands = r,
                ledgerTime = let,
                transactionSeedAndSubmissionTime = Some(transactionSeed -> let)
              )
              .consume(lookupContract, lookupPackage, lookupKey))
        .map(_._1)

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" in {

      val Right(tx) = runExample(fetcher1Cid, clara)
      txFetchActors(tx) shouldBe Set(alice, clara)
    }

    "not propagate the parent's signatories nor actors when not stakeholders" in {

      val Right(tx) = runExample(fetcher2Cid, party)
      txFetchActors(tx) shouldBe Set()
    }

    "be retained when reinterpreting single fetch nodes" in {
      val Right(tx) = runExample(fetcher1Cid, clara)
      val fetchNodes =
        tx.fold(Seq[(NodeId, GenNode.WithTxValue[NodeId, ContractId])]()) {
          case (ns, (nid, n @ NodeFetch(_, _, _, _, _, _, _))) => ns :+ ((nid, n))
          case (ns, _) => ns
        }
      fetchNodes.foreach {
        case (nid, n) =>
          val fetchTx = GenTx(HashMap(nid -> n), ImmArray(nid))
          val Right((reinterpreted, _)) = engine
            .reinterpret(None, participant, n.requiredAuthorizers, Seq(n), let)
            .consume(lookupContract, lookupPackage, lookupKey)
          (fetchTx isReplayedBy reinterpreted) shouldBe true
      }
    }
  }

  "reinterpreting fetch nodes" should {

    val submissionSeed = hash("reinterpreting fetch nodes")

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

    def lookupContract(
        id: AbsoluteContractId): Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
      id match {
        case `fetchedCid` => Some(fetchedContract)
        case _ => None
      }
    }

    "succeed with a fresh engine, correctly compiling packages" in {
      val engine = Engine()

      val fetchNode = NodeFetch(
        coid = fetchedCid,
        templateId = fetchedTid,
        optLocation = None,
        actingParties = None,
        signatories = Set.empty,
        stakeholders = Set.empty,
        key = None,
      )

      val let = Time.Timestamp.now()

      val reinterpreted = engine
        .reinterpret(Some(submissionSeed -> let), participant, Set.empty, Seq(fetchNode), let)
        .consume(lookupContract, lookupPackage, lookupKey)

      reinterpreted shouldBe 'right
    }

  }

  "reinterpreting lookup by key" should {

    val submissionSeed = hash("interpreting lookup by key nodes")

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

    def lookupKey(key: GlobalKey): Option[AbsoluteContractId] = {
      (key.templateId, key.key) match {
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

    def firstLookupNode[Nid, Cid, Val](
        tx: GenTx[Nid, Cid, Val]): Option[NodeLookupByKey[Cid, Val]] =
      tx.nodes.values.collectFirst {
        case nl @ NodeLookupByKey(_, _, _, _) => nl
      }

    val now = Time.Timestamp.now()

    "reinterpret to the same node when lookup finds a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
      val Right((tx, txMeta @ _)) = engine
        .submit(
          Commands(alice, ImmArray(exerciseCmd), now, "test"),
          participant,
          Some(submissionSeed))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val Some(lookupNode) = firstLookupNode(tx)
      lookupNode.result shouldBe Some(lookedUpCid)

      val freshEngine = Engine()
      val Right((reinterpreted, dependsOnTime @ _)) = freshEngine
        .reinterpret(Some(submissionSeed -> now), participant, Set.empty, Seq(lookupNode), now)
        .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted) shouldEqual Some(lookupNode)
    }

    "reinterpret to the same node when lookup doesn't find a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(57)))))
      val Right((tx, txMeta @ _)) = engine
        .submit(
          Commands(alice, ImmArray(exerciseCmd), now, "test"),
          participant,
          Some(submissionSeed))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val Some(lookupNode) = firstLookupNode(tx)
      lookupNode.result shouldBe None

      val freshEngine = Engine()
      val Right((reinterpreted, dependsOnTime @ _)) = freshEngine
        .reinterpret(Some(submissionSeed -> now), participant, Set.empty, Seq(lookupNode), now)
        .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted) shouldEqual Some(lookupNode)
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
          Commands(party, ImmArray(command), Time.Timestamp.now(), "test"),
          participant,
          Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
    }

    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)
    run("GetTime").map(_._2.dependsOnTime) shouldBe Right(true)
    run("FactorialOfThree").map(_._2.dependsOnTime) shouldBe Right(false)

  }

  "fetching contracts that have keys correctly fills in the transaction structure" when {
    val fetchedCid = toContractId("#1")
    val now = Time.Timestamp.now()

    "fetched via a fetch" in {

      val lookupContractMap = Map(fetchedCid -> withKeyContractInst)

      val Right(cmds) = commandTranslator
        .preprocessFetch(BasicTests_WithKey, fetchedCid)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val Right((tx, dependsOnTime @ _)) = engine
        .interpretCommands(false, false, Set(alice), ImmArray(cmds), now, None)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      tx.nodes.values.headOption match {
        case Some(NodeFetch(_, _, _, _, _, _, key)) =>
          key match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(KeyWithMaintainers(keyValue @ _, maintainers)) =>
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

      def lookupKey(key: GlobalKey): Option[AbsoluteContractId] = {
        (key.templateId, key.key) match {
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
      val now = Time.Timestamp.now()

      val Right(cmds) = commandTranslator
        .preprocessExercise(
          fetcherTemplateId,
          fetcherCid,
          "Fetch",
          ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val Right((tx, dependsOnTime @ _)) = engine
        .interpretCommands(false, false, Set(alice), ImmArray(cmds), now, None)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      tx.nodes.values.collectFirst {
        case nf: NodeFetch[_, _] => nf
      } match {
        case Some(nf) =>
          nf.key match {
            // just test that the maintainers match here, getting the key out is a bit hairier
            case Some(KeyWithMaintainers(keyValue @ _, maintainers)) =>
              assert(maintainers == Set(alice))
            case None => fail("the recomputed fetch didn't have a key")
          }
        case None => fail("didn't find the fetch node resulting from fetchByKey")
      }
    }
  }

}

object EngineTest {

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private def toContractId(s: String): AbsoluteContractId =
    AbsoluteContractId.assertFromString(s)

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  private def findNodeByIdx[Cid, Val](nodes: Map[NodeId, GenNode[NodeId, Cid, Val]], idx: Int) =
    nodes.collectFirst { case (nodeId, node) if nodeId.index == idx => node }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit def resultEq: Equality[Either[Error, SValue]] = {
    case (Right(v1: SValue), Right(v2: SValue)) => svalue.Equality.areEqual(v1, v2)
    case (Left(e1), Left(e2)) => e1 == e2
    case _ => false
  }

}
