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
import com.daml.lf.speedy.{InitialSeeding, SValue, svalue}
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

  val defaultContracts =
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

  def lookupKey(key: GlobalKey): Option[AbsoluteContractId] =
    (key.templateId, key.key) match {
      case (
          BasicTests_WithKey,
          ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ) =>
        Some(toContractId("#BasicTests:WithKey:1"))
      case _ =>
        None
    }

  // TODO make these two per-test, so that we make sure not to pollute the package cache and other possibly mutable stuff
  val engine = Engine()
  val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())

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
      val (optionalPkgId, optionalPkg @ _, allOptionalPackages) =
        loadPackage("daml-lf/tests/Optional.dar")

      val translator = new preprocessing.Preprocessor(ConcurrentCompiledPackages.apply())

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
      val translator = new preprocessing.Preprocessor(ConcurrentCompiledPackages.apply())
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
      .submit(Commands(party, ImmArray(command), let, "test"), participant, Some(submissionSeed))
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
      (tx isReplayedBy rtx) shouldBe true
    }

    "be validated" in {
      val Right((tx, meta)) = interpretResult
      val validated = engine
        .validate(tx, let, participant, meta.submissionTime, Some(submissionSeed))
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
      interpretResult.map(_._2.byKeyNodes) shouldBe Right(ImmArray.empty)
    }

  }

  "exercise command" should {
    val submissionSeed = hash("exercise command")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(Some(submissionSeed), participant, let)
    val cid = toContractId("#BasicTests:Simple:1")
    val command =
      ExerciseCommand(templateId, cid, "Hello", ValueRecord(Some(hello), ImmArray.empty))

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(party),
                commands = r,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
              )
              .consume(lookupContract, lookupPackage, lookupKey))
    val Right((tx, txMeta)) = interpretResult

    "be translated" in {
      val Right((rtx, _)) = engine
        .submit(Commands(party, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
      (tx isReplayedBy rtx) shouldBe true
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
      (tx isReplayedBy rtx) shouldBe true
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, let, Some(submissionSeed))
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

    val res = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right

    "fail at submission" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
      submitResult.left.value.msg should startWith("dependency error: couldn't find key")
    }
  }

  "exercise-by-key command with existing key" should {
    val submissionSeed = hash("exercise-by-key command with existing key")
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val seeding = Engine.initialSeeding(Some(submissionSeed), participant, let)
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
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(alice),
                commands = r,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
              )
              .consume(lookupContract, lookupPackage, lookupKey))
    val Right((tx, txMeta)) = result

    "be translated" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
        .map(_._1)
      (result.map(_._1) |@| submitResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "reinterpret to the same result" in {

      val reinterpretResult =
        reinterpret(engine, Set(alice), tx.roots, tx, txMeta, let, lookupPackage, defaultContracts)
          .map(_._1)
      (result.map(_._1) |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, let, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
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

    "mark all the exercise nodes as performed byKey" in {
      val exerciseNodes = tx.nodes.collect {
        case (id, _: NodeExercises[_, _, _]) => id
      }
      txMeta.byKeyNodes shouldBe 'nonEmpty
      txMeta.byKeyNodes.toSeq.toSet shouldBe exerciseNodes.toSet
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
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(party),
                commands = r,
                ledgerTime = let,
                submissionTime = let,
                seeding = InitialSeeding.TransactionSeed(txSeed),
              )
              .consume(lookupContract, lookupPackage, lookupKey))

    val Right((tx, txMeta)) = interpretResult

    "be translated" in {
      tx.roots should have length 2
      tx.nodes.keySet.toList should have length 2
      val ImmArray(create, exercise) = tx.roots.map(tx.nodes)
      create shouldBe a[NodeCreate[_, _]]
      exercise shouldBe a[NodeExercises[_, _, _]]
    }

    "reinterpret to the same result" in {

      val reinterpretResult =
        reinterpret(engine, Set(party), tx.roots, tx, txMeta, let, lookupPackage)
          .map(_._1)
      (interpretResult.map(_._1) |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let, participant, let, Some(submissionSeed))
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) => ()
      }
    }

    "not mark any node as byKey" in {
      interpretResult.map(_._2.byKeyNodes) shouldBe Right(ImmArray.empty)
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

      val Right(DDataType(_, ImmArray(), _)) = PackageLookup
        .lookupDataType(basicTestsPkg, "BasicTests:MyNestedRec")
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
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
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
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
      .submit(Commands(bob, ImmArray(command), let, "test"), participant, Some(submissionSeed))
      .consume(lookupContract, lookupPackage, lookupKey)

    val submissionTime = txMeta.submissionTime

    val txSeed =
      crypto.Hash.deriveTransactionSeed(submissionSeed, participant, submissionTime)
    val Right(cmds) = preprocessor
      .preprocessCommands(ImmArray(command))
      .consume(lookupContract, lookupPackage, lookupKey)
    val Right((rtx, _)) = engine
      .interpretCommands(
        validating = false,
        submitters = Set(bob),
        commands = cmds,
        ledgerTime = let,
        submissionTime = submissionTime,
        seeding = InitialSeeding.TransactionSeed(txSeed)
      )
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      (rtx isReplayedBy tx) shouldBe true
    }

    val Right(blindingInfo) =
      Blinding.checkAuthorizationAndBlind(tx, Set(bob))

    "reinterpret to the same result" in {

      val Right((rtx, _)) =
        reinterpret(engine, Set(bob), tx.roots, tx, txMeta, let, lookupPackage, defaultContracts)
      (rtx isReplayedBy tx) shouldBe true
    }

    "blinded correctly" in {

      // Bob sees both the archive and the create
      val bobView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, bob, tx)
      bobView.nodes.size shouldBe 2
      findNodeByIdx(bobView.nodes, 0).getOrElse(fail("node not found")) match {
        case NodeExercises(
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
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, clara, tx)

      claraView.nodes.size shouldBe 1
      findNodeByIdx(claraView.nodes, 1).getOrElse(fail("node not found")) match {
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
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
          )
          .consume(lookupContract, lookupPackage, lookupKey)
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
    val seeding = Engine.initialSeeding(Some(submissionSeed), participant, let)

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

      val res = preprocessor
        .preprocessCommands(ImmArray(command))
        .consume(lookupContract, lookupPackage, lookupKey)

      res
        .flatMap(
          r =>
            engine
              .interpretCommands(
                validating = false,
                submitters = Set(exerciseActor),
                commands = r,
                ledgerTime = let,
                submissionTime = let,
                seeding = seeding,
              )
              .consume(lookupContract, lookupPackage, lookupKey))

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" in {

      val Right((tx, _)) = runExample(fetcher1Cid, clara)
      txFetchActors(tx) shouldBe Set(alice, clara)
    }

    "not propagate the parent's signatories nor actors when not stakeholders" in {

      val Right((tx, _)) = runExample(fetcher2Cid, party)
      txFetchActors(tx) shouldBe Set()
    }

    "be retained when reinterpreting single fetch nodes" in {
      val Right((tx, txMeta)) = runExample(fetcher1Cid, clara)
      val fetchNodes =
        tx.fold(Seq[(NodeId, GenNode.WithTxValue[NodeId, ContractId])]()) {
          case (ns, (nid, n @ NodeFetch(_, _, _, _, _, _, _))) => ns :+ ((nid, n))
          case (ns, _) => ns
        }

      fetchNodes.foreach {
        case (nid, n) =>
          val fetchTx = GenTx(HashMap(nid -> n), ImmArray(nid))
          val Right((reinterpreted, _)) =
            engine
              .reinterpret(n.requiredAuthorizers, n, txMeta.nodeSeeds.toSeq.collectFirst {
                case (`nid`, seed) => seed
              }, txMeta.submissionTime, let)
              .consume(lookupContract, lookupPackage, lookupKey)
          (fetchTx isReplayedBy reinterpreted) shouldBe true
      }
    }

    "not mark any node as byKey" in {
      runExample(fetcher2Cid, party).map(_._2.byKeyNodes) shouldBe Right(ImmArray.empty)
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

      val reinterpreted =
        engine
          .reinterpret(Set.empty, fetchNode, None, let, let)
          .consume(lookupContract, lookupPackage, lookupKey)

      reinterpreted shouldBe 'right
    }

  }

  "lookup by key" should {

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
        tx: GenTx[Nid, Cid, Val],
    ): Option[(Nid, NodeLookupByKey[Cid, Val])] =
      tx.nodes.collectFirst {
        case (nid, nl @ NodeLookupByKey(_, _, _, _)) => nid -> nl
      }

    val now = Time.Timestamp.now()

    "mark all lookupByKey nodes as byKey" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
      val Right((tx, txMeta)) = engine
        .submit(
          Commands(alice, ImmArray(exerciseCmd), now, "test"),
          participant,
          Some(submissionSeed))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      val lookupNodes = tx.nodes.collect {
        case (id, _: NodeLookupByKey[_, _]) => id
      }

      txMeta.byKeyNodes shouldBe 'nonEmpty
      txMeta.byKeyNodes.toSeq.toSet shouldBe lookupNodes.toSet
    }

    "be reinterpreted to the same node when lookup finds a contract" in {
      val exerciseCmd = ExerciseCommand(
        lookerUpTemplateId,
        lookerUpCid,
        "Lookup",
        ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(42)))))
      val Right((tx, txMeta)) = engine
        .submit(
          Commands(alice, ImmArray(exerciseCmd), now, "test"),
          participant,
          Some(submissionSeed))
        .consume(lookupContractMap.get, lookupPackage, lookupKey)
      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx)
      lookupNode.result shouldBe Some(lookedUpCid)

      val Right((reinterpreted, _)) =
        Engine()
          .reinterpret(
            Set.empty,
            lookupNode,
            nodeSeedMap.get(nid),
            txMeta.submissionTime,
            now,
          )
          .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted).map(_._2) shouldEqual Some(lookupNode)
    }

    "be reinterpreted to the same node when lookup doesn't find a contract" in {
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

      val nodeSeedMap = HashMap(txMeta.nodeSeeds.toSeq: _*)

      val Some((nid, lookupNode)) = firstLookupNode(tx)
      lookupNode.result shouldBe None

      val Right((reinterpreted, _)) =
        Engine()
          .reinterpret(Set.empty, lookupNode, nodeSeedMap.get(nid), txMeta.submissionTime, now)
          .consume(lookupContract, lookupPackage, lookupKey)

      firstLookupNode(reinterpreted).map(_._2) shouldEqual Some(lookupNode)
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

      val cmd = speedy.Command.Fetch(BasicTests_WithKey, SValue.SContractId(fetchedCid))

      val Right((tx, _)) = engine
        .interpretCommands(
          validating = false,
          submitters = Set(alice),
          commands = ImmArray(cmd),
          ledgerTime = now,
          submissionTime = now,
          seeding = InitialSeeding.NoSeed,
        )
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

      val Right(cmds) = preprocessor
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

      val Right((tx, txMeta)) = engine
        .interpretCommands(false, Set(alice), cmds, now, now, InitialSeeding.NoSeed)
        .consume(lookupContractMap.get, lookupPackage, lookupKey)

      tx.nodes
        .collectFirst {
          case (id, nf: NodeFetch[_, _]) =>
            nf.key match {
              // just test that the maintainers match here, getting the key out is a bit hairier
              case Some(KeyWithMaintainers(keyValue @ _, maintainers)) =>
                assert(maintainers == Set(alice))
              case None => fail("the recomputed fetch didn't have a key")
            }
            txMeta.byKeyNodes shouldBe ImmArray(id)
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
          (Some[Name]("party"), ValueParty(alice)),
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
        .submit(Commands(party, ImmArray(command), let, "test"), participant, Some(submissionSeed))
        .consume(_ => None, lookupPackage, _ => None)
    }

    "produce a quadratic number of nodes" in {
      run(0).map(_._1.nodes.size) shouldBe Right(2)
      run(1).map(_._1.nodes.size) shouldBe Right(6)
      run(2).map(_._1.nodes.size) shouldBe Right(14)
      run(3).map(_._1.nodes.size) shouldBe Right(30)
    }

    "be validable in whole" in {
      def validate(tx: Tx.Transaction, metaData: Tx.Metadata) =
        engine
          .validate(tx, let, participant, metaData.submissionTime, Some(submissionSeed))
          .consume(_ => None, lookupPackage, _ => None)

      run(0).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
      run(3).flatMap { case (tx, metaData) => validate(tx, metaData) } shouldBe Right(())
    }

    "be partially reinterpretable" in {
      val Right((tx, txMeta)) = run(3)
      val ImmArray(_, exeNode1) = tx.roots
      val NodeExercises(_, _, _, _, _, _, _, _, _, _, children, _, _) =
        tx.nodes(exeNode1)
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

  private def reinterpret(
      engine: Engine,
      submitters: Set[Party],
      nodes: ImmArray[Tx.NodeId],
      tx: Tx.Transaction,
      txMeta: Tx.Metadata,
      ledgerEffectiveTime: Time.Timestamp,
      lookupPackages: PackageId => Option[Package],
      contracts: Map[AbsoluteContractId, Tx.ContractInst[AbsoluteContractId]] = Map.empty,
  ): Either[Error, (Tx.Transaction, Tx.Metadata)] = {
    type Acc =
      (
          HashMap[NodeId, Tx.Node],
          BackStack[NodeId],
          Boolean,
          BackStack[(NodeId, crypto.Hash)],
          Map[AbsoluteContractId, Tx.ContractInst[AbsoluteContractId]],
          Map[GlobalKey, AbsoluteContractId],
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
                tx.nodes(nodeId),
                nodeSeedMap.get(nodeId),
                txMeta.submissionTime,
                ledgerEffectiveTime)
              .consume(contracts0.get, lookupPackages, keys0.get)
            (tr1, meta1) = currentStep
            (contracts1, keys1) = tr1.fold((contracts0, keys0)) {
              case (
                  (contracts, keys),
                  (
                    _,
                    NodeExercises(
                      targetCoid: AbsoluteContractId,
                      _,
                      _,
                      _,
                      true,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _,
                      _))) =>
                (contracts - targetCoid, keys)
              case (
                  (contracts, keys),
                  (_, NodeCreate(cid: AbsoluteContractId, coinst, _, _, _, key))) =>
                (
                  contracts.updated(
                    cid,
                    coinst.assertNoRelCid(cid => s"unexpected relative contract ID $cid")),
                  key.fold(keys)(
                    k =>
                      keys.updated(
                        GlobalKey(
                          coinst.template,
                          k.key.value.assertNoCid(cid => s"unexpected relative contract ID $cid")),
                        cid))
                )
              case (acc, _) => acc
            }
            n = nodes.size
            nodeRenaming = (nid: NodeId) => NodeId(nid.index + n)
            tr = tr1.mapNodeId(nodeRenaming)
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
          GenTx(nodes, roots.toImmArray),
          Tx.Metadata(
            submissionSeed = None,
            submissionTime = txMeta.submissionTime,
            usedPackages = Set.empty,
            dependsOnTime = dependsOnTime,
            nodeSeeds = nodeSeeds.toImmArray,
            byKeyNodes = ImmArray.empty,
          ))
    }
  }

}
