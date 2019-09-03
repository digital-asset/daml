// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import java.util
import java.io.File

import com.digitalasset.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.digitalasset.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction => GenTx, Transaction => Tx}
import com.digitalasset.daml.lf.value.Value
import Value._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.value.ValueVersions.assertAsVersionedValue
import org.scalatest.{EitherValues, Matchers, WordSpec}
import scalaz.std.either._
import scalaz.syntax.apply._

import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product"
  ))
class EngineTest extends WordSpec with Matchers with EitherValues with BazelRunfiles {

  import EngineTest._

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

  def lookupContractWithKey(
      @deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(
      ContractInst(
        TypeConName(basicTestsPkgId, "BasicTests:WithKey"),
        assertAsVersionedValue(
          ValueRecord(
            Some(BasicTests_WithKey),
            ImmArray(
              (Some("p"), ValueParty(alice)),
              (Some("k"), ValueInt64(42))
            ))),
        ""
      ))
  }

  def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  val BasicTests_WithKey = Identifier(basicTestsPkgId, "BasicTests:WithKey")

  def lookupKey(key: GlobalKey): Option[AbsoluteContractId] =
    key match {
      case GlobalKey(
          BasicTests_WithKey,
          Value.VersionedValue(_, ValueRecord(_, ImmArray((_, p), (_, ValueInt64(42))))))
          if p == ValueParty(alice) =>
        Some(AbsoluteContractId("1"))
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
        CreateCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party))))))

      val res = commandTranslator
        .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create commands argument without labels" in {
      val id = Identifier(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(
          id,
          assertAsVersionedValue(ValueRecord(Some(id), ImmArray((None, ValueParty(party))))))

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
          assertAsVersionedValue(
            ValueRecord(
              Some(id),
              ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(party))))))

      val res = commandTranslator
        .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate exercise commands argument including labels" in {
      val originalCoid = toContractId("1")
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara))))))

      val res = commandTranslator
        .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise commands argument without labels" in {
      val originalCoid = "1"
      val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty(clara))))))

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
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42))))),
        "SumToK",
        assertAsVersionedValue(ValueRecord(None, ImmArray((Some[Name]("n"), ValueInt64(5)))))
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
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42))))),
        "SumToK",
        assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueInt64(5)))))
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
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42))))),
        "SumToK",
        assertAsVersionedValue(ValueRecord(None, ImmArray((Some[Name]("WRONG"), ValueInt64(5)))))
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
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42))))),
        "Transfer",
        assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty(clara)))))
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
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((None, ValueInt64(42)), (None, ValueInt64(42))))),
        "SumToK",
        assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueInt64(5)))))
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
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((Some("giver"), ValueParty(clara)), (Some("receiver"), ValueParty(clara))))),
          "Transfer",
          assertAsVersionedValue(
            ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara)))))
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
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((None, ValueParty(clara)), (None, ValueParty(clara))))),
          "Transfer",
          assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty(clara)))))
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
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((None, ValueParty(clara)), (Some("this_is_not_the_one"), ValueParty(clara)))
            )),
          "Transfer",
          assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty(clara)))))
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
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((None, ValueParty(clara)), (None, ValueParty(clara))))),
          "Transfer",
          assertAsVersionedValue(
            ValueRecord(None, ImmArray((Some[Name]("this_is_not_the_one"), ValueParty(clara)))))
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
      val someValue = assertAsVersionedValue(
        ValueRecord(
          Some(id),
          ImmArray(Some[Name]("recField") -> ValueOptional(Some(ValueText("foo")))))
      )
      val noneValue = assertAsVersionedValue(
        ValueRecord(Some(id), ImmArray(Some[Name]("recField") -> ValueOptional(None)))
      )
      val typ = TTyConApp(id, ImmArray.empty)

      translator
        .translateValue(typ, someValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldBe
        Right(SRecord(id, Name.Array("recField"), ArrayList(SOptional(Some(SText("foo"))))))

      translator
        .translateValue(typ, noneValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldBe
        Right(SRecord(id, Name.Array("recField"), ArrayList(SOptional(None))))

    }

    "returns correct error when resuming" in {
      val translator = new CommandPreprocessor(ConcurrentCompiledPackages.apply())
      val id = Identifier(basicTestsPkgId, "BasicTests:MyRec")
      val wrongRecord = assertAsVersionedValue(
        ValueRecord(Some(id), ImmArray(Some[Name]("wrongLbl") -> ValueText("foo"))))
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
      CreateCommand(
        id,
        assertAsVersionedValue(
          ValueRecord(Some(id), ImmArray((Some[Name]("p"), ValueParty(party))))))

    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult = engine
      .submit(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      interpretResult shouldBe 'right
    }

    "reinterpret to the same result" in {
      val Right(tx) = interpretResult
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Set(party), txRoots, let)
          .consume(lookupContract, lookupPackage, lookupKey)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val Right(tx) = interpretResult
      val validated = engine
        .validate(tx, let)
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
      interpretResult.map(_.usedPackages.contains(basicTestsPkgId)) shouldBe Right(true)
    }
  }

  "exercise command" should {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val command =
      ExerciseCommand(
        templateId,
        "1",
        "Hello",
        assertAsVersionedValue(ValueRecord(Some(hello), ImmArray.empty)))

    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(
        r =>
          engine
            .interpret(
              validating = false,
              checkSubmitterInMaintainers = true,
              submitters = Set(party),
              commands = r,
              time = let)
            .consume(lookupContract, lookupPackage, lookupKey))
    val Right(tx) = interpretResult

    "be translated" in {
      val submitResult = engine
        .submit(Commands(party, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      interpretResult shouldBe 'right
      submitResult shouldBe interpretResult
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Set(party), txRoots, let)
          .consume(lookupContract, lookupPackage, lookupKey)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let)
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
      val events = Event.collectEvents(tx, blindingInfo.explicitDisclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains party)
      partyEvents.size shouldBe 1
      partyEvents(0) match {
        case _: ExerciseEvent[Tx.NodeId, ContractId, Tx.Value[ContractId]] => succeed
        case _ => fail("expected exercise")
      }
    }
  }

  "exercise-by-key command with missing key" should {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val command = ExerciseByKeyCommand(
      templateId,
      assertAsVersionedValue(
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(43))))),
      "SumToK",
      assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueInt64(5)))))
    )

    val res = commandTranslator
      .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
      .consume(lookupContractWithKey, lookupPackage, lookupKey)
    res shouldBe 'right

    "fail at submission" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractWithKey, lookupPackage, lookupKey)
      submitResult.left.value.msg should startWith("dependency error: couldn't find key")
    }
  }

  "exercise-by-key command with existing key" should {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:WithKey")
    val let = Time.Timestamp.now()
    val command = ExerciseByKeyCommand(
      templateId,
      assertAsVersionedValue(
        ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42))))),
      "SumToK",
      assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueInt64(5)))))
    )

    val res = commandTranslator
      .preprocessCommands(Commands(alice, ImmArray(command), let, "test"))
      .consume(lookupContractWithKey, lookupPackage, lookupKey)
    res shouldBe 'right
    val result =
      res.flatMap(
        r =>
          engine
            .interpret(
              validating = false,
              checkSubmitterInMaintainers = true,
              submitters = Set(alice),
              commands = r,
              time = let)
            .consume(lookupContractWithKey, lookupPackage, lookupKey))
    val tx = result.right.value

    "be translated" in {
      val submitResult = engine
        .submit(Commands(alice, ImmArray(command), let, "test"))
        .consume(lookupContractWithKey, lookupPackage, lookupKey)
      submitResult shouldBe result
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Set(alice), txRoots, let)
          .consume(lookupContractWithKey, lookupPackage, lookupKey)
      (result |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let)
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
      val events = Event.collectEvents(tx, blindingInfo.explicitDisclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains alice)
      partyEvents.size shouldBe 1
      partyEvents(0) match {
        case _: ExerciseEvent[Tx.NodeId, ContractId, Tx.Value[ContractId]] => succeed
        case _ => fail("expected exercise")
      }
    }
  }

  "create-and-exercise command" should {
    val templateId = Identifier(basicTestsPkgId, "BasicTests:Simple")
    val hello = Identifier(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val command =
      CreateAndExerciseCommand(
        templateId,
        assertAsVersionedValue(
          ValueRecord(Some(templateId), ImmArray(Some[Name]("p") -> ValueParty(party)))),
        "Hello",
        assertAsVersionedValue(ValueRecord(Some(hello), ImmArray.empty))
      )

    val res = commandTranslator
      .preprocessCommands(Commands(party, ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(
        r =>
          engine
            .interpret(
              validating = false,
              checkSubmitterInMaintainers = true,
              submitters = Set(party),
              commands = r,
              time = let)
            .consume(lookupContract, lookupPackage, lookupKey))
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
          .reinterpret(Set(party), txRoots, let)
          .consume(lookupContract, lookupPackage, lookupKey)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "be validated" in {
      val validated = engine
        .validate(tx, let)
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
      val list = ValueList(FrontStack.empty[Value[AbsoluteContractId]])
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), assertAsVersionedValue(list))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack.empty))
    }

    "translate singleton" in {
      val list = ValueList(FrontStack(ValueInt64(1)))
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), assertAsVersionedValue(list))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldEqual Right(SList(FrontStack(ImmArray(SInt64(1)))))
    }

    "translate average list" in {
      val list = ValueList(
        FrontStack(ValueInt64(1), ValueInt64(2), ValueInt64(3), ValueInt64(4), ValueInt64(5)))
      val res = commandTranslator
        .translateValue(TList(TBuiltin(BTInt64)), assertAsVersionedValue(list))
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
          assertAsVersionedValue(nested))
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
          assertAsVersionedValue(rec))
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
          assertAsVersionedValue(rec))
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
          assertAsVersionedValue(rec))
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
          assertAsVersionedValue(rec))
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
          assertAsVersionedValue(rec))
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
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'left
    }

  }

  "exercise callable command" should {
    val originalCoid = "1"
    val templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout")
    val let = Time.Timestamp.now()
    val command = ExerciseCommand(
      templateId,
      originalCoid,
      "Transfer",
      assertAsVersionedValue(
        ValueRecord(None, ImmArray((Some[Name]("newReceiver"), ValueParty(clara))))))

    val res = commandTranslator
      .preprocessCommands(Commands(bob, ImmArray(command), let, "test"))
      .consume(lookupContractForPayout, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(
        r =>
          engine
            .interpret(
              validating = false,
              checkSubmitterInMaintainers = true,
              submitters = Set(bob),
              commands = r,
              time = let)
            .consume(lookupContractForPayout, lookupPackage, lookupKey))
    "be translated" in {
      val submitResult = engine
        .submit(Commands(bob, ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      interpretResult shouldBe 'right
      submitResult shouldBe interpretResult
    }

    val Right(tx) = interpretResult
    val Right(blindingInfo) =
      Blinding.checkAuthorizationAndBlind(tx, Set(bob))

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine
          .reinterpret(Set(bob), txRoots, let)
          .consume(lookupContractForPayout, lookupPackage, lookupKey)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "blinded correctly" in {

      // Bob sees both the archive and the create
      val bobView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, bob, tx)
      bobView.nodes.size shouldBe 2

      bobView.nodes(Tx.NodeId.unsafeFromIndex(0)) match {
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
          coid shouldBe AbsoluteContractId(originalCoid)
          consuming shouldBe true
          actingParties shouldBe Set(bob)
          children shouldBe ImmArray(Tx.NodeId.unsafeFromIndex(1))
          choice shouldBe "Transfer"
        case _ => fail("exercise expected first for Bob")
      }

      bobView.nodes(Tx.NodeId.unsafeFromIndex(1)) match {
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, clara, tx)

      claraView.nodes.size shouldBe 1
      claraView.nodes(Tx.NodeId.unsafeFromIndex(1)) match {
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set(alice, clara)
        case _ => fail("create event is expected")
      }
    }

    "events generated correctly" in {
      val Right(tx) = interpretResult
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set(bob))
      val events = Event.collectEvents(tx, blindingInfo.explicitDisclosure)
      val partyEvents = events.filter(_.witnesses contains bob)
      partyEvents.roots.length shouldBe 1
      val bobExercise = partyEvents.events(partyEvents.roots(0))
      bobExercise shouldBe
        ExerciseEvent(
          contractId = AbsoluteContractId(originalCoid),
          templateId = Identifier(basicTestsPkgId, "BasicTests:CallablePayout"),
          choice = "Transfer",
          choiceArgument = assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Transfer")),
              ImmArray((Some[Name]("newReceiver"), ValueParty(clara))))),
          actingParties = Set(bob),
          isConsuming = true,
          children = ImmArray(Tx.NodeId.unsafeFromIndex(1)),
          stakeholders = Set(bob, alice),
          witnesses = Set(bob, alice),
          exerciseResult = Some(
            assertAsVersionedValue(
              ValueContractId(RelativeContractId(Tx.NodeId.unsafeFromIndex(1)))))
        )
      val bobVisibleCreate = partyEvents.events(Tx.NodeId.unsafeFromIndex(1))
      bobVisibleCreate shouldBe
        CreateEvent(
          RelativeContractId(Tx.NodeId.unsafeFromIndex(1)),
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

    val fetchedStrCid = "1"
    val fetchedCid = AbsoluteContractId(fetchedStrCid)
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTArgs = ImmArray(
      (Some[Name]("sig1"), ValueParty(alice)),
      (Some[Name]("sig2"), ValueParty(bob)),
      (Some[Name]("obs"), ValueParty(clara))
    )

    val fetcherStrTid = "BasicTests:Fetcher"
    val fetcherTid = Identifier(basicTestsPkgId, fetcherStrTid)

    val fetcher1StrCid = "2"
    val fetcher1Cid = AbsoluteContractId(fetcher1StrCid)
    val fetcher1TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(alice)),
      (Some[Name]("obs"), ValueParty(bob)),
      (Some[Name]("fetcher"), ValueParty(clara)),
    )

    val fetcher2StrCid = "3"
    val fetcher2Cid = AbsoluteContractId(fetcher2StrCid)
    val fetcher2TArgs = ImmArray(
      (Some[Name]("sig"), ValueParty(party)),
      (Some[Name]("obs"), ValueParty(alice)),
      (Some[Name]("fetcher"), ValueParty(party)),
    )

    def makeContract[Cid](tid: Ref.QualifiedName, targs: ImmArray[(Option[Name], Value[Cid])]) =
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

    def actFetchActors[Nid, Cid, Val](n: GenNode[Nid, Cid, Val]): Set[Party] = {
      n match {
        case NodeFetch(_, _, _, actingParties, _, _) => actingParties.getOrElse(Set.empty)
        case _ => Set()
      }
    }

    def txFetchActors[Nid, Cid, Val](tx: GenTx[Nid, Cid, Val]): Set[Party] =
      tx.fold(GenTx.AnyOrder, Set[Party]()) {
        case (actors, (_, n)) => actors union actFetchActors(n)
      }

    def runExample(cid: String, exerciseActor: Party) = {
      val command = ExerciseCommand(
        fetcherTid,
        cid,
        "DoFetch",
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((Some[Name]("cid"), ValueContractId(fetchedCid))))))

      val res = commandTranslator
        .preprocessCommands(Commands(exerciseActor, ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)

      res.flatMap(
        r =>
          engine
            .interpret(
              validating = false,
              checkSubmitterInMaintainers = true,
              submitters = Set(exerciseActor),
              commands = r,
              time = let)
            .consume(lookupContract, lookupPackage, lookupKey))

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" in {

      val Right(tx) = runExample(fetcher1StrCid, clara)
      txFetchActors(tx) shouldBe Set(alice, clara)
    }

    "not propagate the parent's signatories nor actors when not stakeholders" in {

      val Right(tx) = runExample(fetcher2StrCid, party)
      txFetchActors(tx) shouldBe Set()
    }

    "be retained when reinterpreting single fetch nodes" in {
      val Right(tx) = runExample(fetcher1StrCid, clara)
      val fetchNodes =
        tx.fold(GenTx.TopDown, Seq[(NodeId, GenNode.WithTxValue[NodeId, ContractId])]()) {
          case (ns, (nid, n @ NodeFetch(_, _, _, _, _, _))) => ns :+ ((nid, n))
          case (ns, _) => ns
        }
      fetchNodes.foreach {
        case (nid, n) =>
          val fetchTx = GenTx(Map(nid -> n), ImmArray(nid), Set(basicTestsPkgId))
          val Right(reinterpreted) = engine
            .reinterpret(n.requiredAuthorizers, Seq(n), let)
            .consume(lookupContract, lookupPackage, lookupKey)
          (fetchTx isReplayedBy reinterpreted) shouldBe true
      }

    }
  }

}

object EngineTest {

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private implicit def toContractId(s: String): ContractIdString =
    ContractIdString.assertFromString(s)

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

}
