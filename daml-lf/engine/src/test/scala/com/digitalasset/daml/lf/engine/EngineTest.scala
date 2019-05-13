// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import java.util
import java.io.File

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction => GenTx, Transaction => Tx}
import com.digitalasset.daml.lf.value.Value
import Value._
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.UniversalArchiveReader
import com.digitalasset.daml.lf.command._
import com.digitalasset.daml.lf.value.ValueVersions.assertAsVersionedValue
import org.scalatest.{Matchers, WordSpec}
import scalaz.std.either._
import scalaz.syntax.apply._

import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product"
  ))
class EngineTest extends WordSpec with Matchers {

  import EngineTest._

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages =
      UniversalArchiveReader().readFile(new File(resource)).get
    val packagesMap = Map(packages.all.map {
      case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
    }: _*)
    val (mainPkgId, mainPkgArchive) = packages.main
    val mainPkg = Decode.readArchivePayloadAndVersion(mainPkgId, mainPkgArchive)._1._2
    (mainPkgId, mainPkg, packagesMap)
  }

  private val (basicTestsPkgId, basicTestsPkg, allPackages) = loadPackage(
    "daml-lf/tests/BasicTests.dar")

  private[this] def makeAbsoluteContractId(coid: ContractId): AbsoluteContractId =
    coid match {
      case rcoid: RelativeContractId =>
        AbsoluteContractId("0:" + rcoid.txnid.index.toString)
      case acoid: AbsoluteContractId => acoid
    }

  private[this] def makeValueWithAbsoluteContractId(
      v: Tx.Value[ContractId]): Tx.Value[AbsoluteContractId] =
    v.mapContractId(makeAbsoluteContractId)

  def lookupContract(@deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(
      ContractInst(
        DefinitionRef(basicTestsPkgId, "BasicTests:Simple"),
        assertAsVersionedValue(
          ValueRecord(
            Some(DefinitionRef(basicTestsPkgId, "BasicTests:Simple")),
            ImmArray((Some("p"), ValueParty("Party"))))),
        ""
      ))
  }

  def lookupContractForPayout(
      @deprecated("shut up unused arguments warning", "blah") id: AbsoluteContractId)
    : Option[ContractInst[Tx.Value[AbsoluteContractId]]] = {
    Some(
      ContractInst(
        DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout"),
        assertAsVersionedValue(
          ValueRecord(
            Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray(
              (Some("giver"), ValueParty("Alice")),
              (Some("receiver"), ValueParty("Bob"))
            ))),
        ""
      ))
  }

  def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  def lookupKey(@deprecated("", "") key: GlobalKey): Option[AbsoluteContractId] =
    sys.error("TODO keys in EngineTest")

  // TODO make these two per-test, so that we make sure not to pollute the package cache and other possibly mutable stuff
  val engine = Engine()
  val commandTranslator = CommandPreprocessor(ConcurrentCompiledPackages())

  "valid data variant identifier" should {
    "found and return the argument types" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:Tree")
      val Right((_, DataVariant(variants))) =
        PackageLookup.lookupVariant(basicTestsPkg, id.qualifiedName)
      variants.find(_._1 == "Leaf") shouldBe Some(("Leaf", TVar("a")))
    }
  }

  "valid data record identifier" should {
    "found and return the argument types" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:MyRec")
      val Right((_, DataRecord(fields, _))) =
        PackageLookup.lookupRecord(basicTestsPkg, id.qualifiedName)
      fields shouldBe ImmArray(("foo", TBuiltin(BTText)))
    }
  }

  "valid template Identifier" should {
    "return the right argument type" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
      val Right((_, DataRecord(fields, tpl))) =
        PackageLookup.lookupRecord(basicTestsPkg, id.qualifiedName)
      fields shouldBe ImmArray(("p", TBuiltin(BTParty)))
      tpl.isEmpty shouldBe false
    }
  }

  "command translation" should {
    "translate create commands argument including labels" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(
          id,
          assertAsVersionedValue(ValueRecord(Some(id), ImmArray((Some("p"), ValueParty("Party"))))))

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create commands argument without labels" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(
          id,
          assertAsVersionedValue(ValueRecord(Some(id), ImmArray((None, ValueParty("Party"))))))

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create commands argument wrong label" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
      val let = Time.Timestamp.now()
      val command =
        CreateCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(Some(id), ImmArray((Some("this_is_not_the_one"), ValueParty("Party"))))))

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate exercise commands argument including labels" in {
      val originalCoid = "1"
      val templateId = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        "Bob",
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((Some("newReceiver"), ValueParty("Clara")))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate exercise commands argument without labels" in {
      val originalCoid = "1"
      val templateId = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command = ExerciseCommand(
        templateId,
        originalCoid,
        "Transfer",
        "Bob",
        assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty("Clara")))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "translate create-and-exercise commands argument including labels" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some("giver"), ValueParty("Clara")),
                (Some("receiver"), ValueParty("Clara"))))),
          "Transfer",
          assertAsVersionedValue(
            ValueRecord(None, ImmArray((Some("newReceiver"), ValueParty("Clara"))))),
          "Clara"
        )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      res shouldBe 'right

    }

    "translate create-and-exercise commands argument without labels" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((None, ValueParty("Clara")), (None, ValueParty("Clara"))))),
          "Transfer",
          assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty("Clara"))))),
          "Clara"
        )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "not translate create-and-exercise commands argument wrong label in create arguments" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (None, ValueParty("Clara")),
                (Some("this_is_not_the_one"), ValueParty("Clara")))
            )),
          "Transfer",
          assertAsVersionedValue(ValueRecord(None, ImmArray((None, ValueParty("Clara"))))),
          "Clara"
        )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "not translate create-and-exercise commands argument wrong label in choice arguments" in {
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
      val let = Time.Timestamp.now()
      val command =
        CreateAndExerciseCommand(
          id,
          assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray((None, ValueParty("Clara")), (None, ValueParty("Clara"))))),
          "Transfer",
          assertAsVersionedValue(
            ValueRecord(None, ImmArray((Some("this_is_not_the_one"), ValueParty("Clara"))))),
          "Clara"
        )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'left
    }

    "translate Optional values" in {
      val (optionalPkgId, optionalPkg @ _, allOptionalPackages) =
        loadPackage("daml-lf/tests/Optional.dar")

      val translator = CommandPreprocessor(ConcurrentCompiledPackages.apply())

      val id = DefinitionRef(optionalPkgId, "Optional:Rec")
      val someValue = assertAsVersionedValue(
        ValueRecord(Some(id), ImmArray(Some("recField") -> ValueOptional(Some(ValueText("foo")))))
      )
      val noneValue = assertAsVersionedValue(
        ValueRecord(Some(id), ImmArray(Some("recField") -> ValueOptional(None)))
      )
      val typ = TTyConApp(id, ImmArray.empty)

      translator
        .translateValue(typ, someValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldBe
        Right(SRecord(id, Array("recField"), ArrayList(SOptional(Some(SText("foo"))))))

      translator
        .translateValue(typ, noneValue)
        .consume(lookupContract, allOptionalPackages.get, lookupKey) shouldBe
        Right(SRecord(id, Array("recField"), ArrayList(SOptional(None))))

    }

    "returns correct error when resuming" in {
      val translator = CommandPreprocessor(ConcurrentCompiledPackages.apply())
      val id = DefinitionRef(basicTestsPkgId, "BasicTests:MyRec")
      val wrongRecord = assertAsVersionedValue(
        ValueRecord(Some(id), ImmArray(Some("wrongLbl") -> ValueText("foo"))))
      translator
        .translateValue(
          TTyConApp(id, ImmArray.empty),
          wrongRecord
        )
        .consume(lookupContract, lookupPackage, lookupKey) shouldBe 'left
    }
  }

  "minimal create command" should {
    val id = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
    val let = Time.Timestamp.now()
    val command =
      CreateCommand(
        id,
        assertAsVersionedValue(ValueRecord(Some(id), ImmArray((Some("p"), ValueParty("Party"))))))

    val res = commandTranslator
      .preprocessCommands(Commands(ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult = engine
      .submit(Commands(ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)

    "be translated" in {
      interpretResult shouldBe 'right
    }

    "reinterpret to the same result" in {
      val Right(tx) = interpretResult
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine.reinterpret(txRoots, let).consume(lookupContract, lookupPackage, lookupKey)
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
    val templateId = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
    val hello = DefinitionRef(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val command =
      ExerciseCommand(
        templateId,
        "1",
        "Hello",
        "Party",
        assertAsVersionedValue(ValueRecord(Some(hello), ImmArray.empty)))

    val res = commandTranslator
      .preprocessCommands(Commands(ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(r => engine.interpret(r, let).consume(lookupContract, lookupPackage, lookupKey))
    val Right(tx) = interpretResult

    "be translated" in {
      val submitResult = engine
        .submit(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)
      interpretResult shouldBe 'right
      submitResult shouldBe interpretResult
    }

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine.reinterpret(txRoots, let).consume(lookupContract, lookupPackage, lookupKey)
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

    "post-commit validation passes" in {
      val validated = engine
        .validatePartial(
          tx.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId),
          Some("Party"),
          let,
          "Party",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) =>
          fail(e.msg)
        case Right(()) =>
          ()
      }
    }

    "post-commit validation fails with missing root node" in {
      val validated = engine
        .validatePartial(
          tx.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId)
            .copy(nodes = Map.empty),
          Some("Party"),
          let,
          "Party",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e)
            if e.msg == "invalid transaction, root refers to non-existing node NodeId(0)" =>
          ()
        case otherwise @ _ =>
          fail("expected failing validation on missing node")
      }
    }

    "post-commit validation fails with authorization failure" in {
      val validated = engine
        .validatePartial(
          tx.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId),
          Some(Party.assertFromString("non-submitting-party")),
          let,
          "Party",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContract, lookupPackage, lookupKey)
      validated match {
        case Left(e) if e.msg == "Post-commit validation failure: unauthorized transaction" =>
          ()
        case e =>
          fail(s"expected failing validation with authentication failure $e")
      }
    }

    "events are collected" in {
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set("Party"))
      val events = Event.collectEvents(tx, blindingInfo.explicitDisclosure)
      val partyEvents = events.events.values.toList.filter(_.witnesses contains "Party")
      partyEvents.size shouldBe 1
      partyEvents(0) match {
        case _: ExerciseEvent[Tx.NodeId, ContractId, Tx.Value[ContractId]] => succeed
        case _ => fail("expected exercise")
      }
    }
  }

  "create-and-exercise command" should {
    val templateId = DefinitionRef(basicTestsPkgId, "BasicTests:Simple")
    val hello = DefinitionRef(basicTestsPkgId, "BasicTests:Hello")
    val let = Time.Timestamp.now()
    val command =
      CreateAndExerciseCommand(
        templateId,
        assertAsVersionedValue(
          ValueRecord(Some(templateId), ImmArray(Some("p") -> ValueParty("Party")))),
        "Hello",
        assertAsVersionedValue(ValueRecord(Some(hello), ImmArray.empty)),
        "Party"
      )

    val res = commandTranslator
      .preprocessCommands(Commands(ImmArray(command), let, "test"))
      .consume(lookupContract, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(r => engine.interpret(r, let).consume(lookupContract, lookupPackage, lookupKey))
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
        engine.reinterpret(txRoots, let).consume(lookupContract, lookupPackage, lookupKey)
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
          TTyConApp(DefinitionRef(basicTestsPkgId, "BasicTests:Nesting0"), ImmArray.empty),
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
        Some(DefinitionRef(basicTestsPkgId, "BasicTests:MyNestedRec")),
        ImmArray(
          (Some("bar"), ValueText("bar")),
          (
            Some("nested"),
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:MyRec")),
              ImmArray(
                (Some("foo"), ValueText("bar"))
              )))
        )
      )

      val Right(DDataType(_, ImmArray(), _)) = PackageLookup
        .lookupDataType(basicTestsPkg, "BasicTests:MyNestedRec")
      val res = commandTranslator
        .translateValue(
          TTyConApp(DefinitionRef(basicTestsPkgId, "BasicTests:MyNestedRec"), ImmArray.empty),
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)
      res shouldBe 'right
    }

    "work with fields with type parameters" in {
      val rec = ValueRecord(
        Some(DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((Some("p"), ValueParty("Alice")), (Some("v"), ValueOptional(Some(ValueInt64(42)))))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
        .translateValue(
          TTyConApp(
            DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters"),
            ImmArray.empty),
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "work with fields with labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((Some("v"), ValueOptional(Some(ValueInt64(42)))), (Some("p"), ValueParty("Alice")))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
        .translateValue(
          TTyConApp(
            DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters"),
            ImmArray.empty),
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "fail with fields with labels, with repetitions" in {
      val rec = ValueRecord(
        Some(Identifier(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((Some("p"), ValueParty(alice)), (Some("p"), ValueParty(bob)))
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
        Some(DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueParty("Alice")), (None, ValueOptional(Some(ValueInt64(42)))))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
        .translateValue(
          TTyConApp(
            DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters"),
            ImmArray.empty),
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'right
    }

    "fail with fields without labels, in the wrong order" in {
      val rec = ValueRecord(
        Some(DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters")),
        ImmArray((None, ValueOptional(Some(ValueInt64(42)))), (None, ValueParty("Alice")))
      )

      val Right(DDataType(_, ImmArray(), _)) =
        PackageLookup.lookupDataType(basicTestsPkg, "BasicTests:TypeWithParameters")
      val res = commandTranslator
        .translateValue(
          TTyConApp(
            DefinitionRef(basicTestsPkgId, "BasicTests:TypeWithParameters"),
            ImmArray.empty),
          assertAsVersionedValue(rec))
        .consume(lookupContract, lookupPackage, lookupKey)

      res shouldBe 'left
    }

  }

  "exercise callable command" should {
    val originalCoid = "1"
    val templateId = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")
    val let = Time.Timestamp.now()
    val command = ExerciseCommand(
      templateId,
      originalCoid,
      "Transfer",
      "Bob",
      assertAsVersionedValue(
        ValueRecord(None, ImmArray((Some("newReceiver"), ValueParty("Clara")))))
    )

    val res = commandTranslator
      .preprocessCommands(Commands(ImmArray(command), let, "test"))
      .consume(lookupContractForPayout, lookupPackage, lookupKey)
    res shouldBe 'right
    val interpretResult =
      res.flatMap(r =>
        engine.interpret(r, let).consume(lookupContractForPayout, lookupPackage, lookupKey))
    "be translated" in {
      val submitResult = engine
        .submit(Commands(ImmArray(command), let, "test"))
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      interpretResult shouldBe 'right
      submitResult shouldBe interpretResult
    }

    val Right(tx) = interpretResult
    val Right(blindingInfo) =
      Blinding.checkAuthorizationAndBlind(tx, Set("Bob"))

    "reinterpret to the same result" in {
      val txRoots = tx.roots.map(id => tx.nodes(id)).toSeq
      val reinterpretResult =
        engine.reinterpret(txRoots, let).consume(lookupContractForPayout, lookupPackage, lookupKey)
      (interpretResult |@| reinterpretResult)(_ isReplayedBy _) shouldBe Right(true)
    }

    "blinded correctly" in {

      // bob sees both the archive and the create
      val bobView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, "Bob", tx)
      bobView.nodes.size shouldBe 2

      val postCommitForBob = engine
        .validatePartial(
          bobView.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId),
          Some("Bob"),
          let,
          "Bob",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      postCommitForBob shouldBe 'right

      bobView.nodes(Tx.NodeId.unsafeFromIndex(0)) match {
        case NodeExercises(coid, _, choice, _, consuming, actingParties, _, _, _, _, children, _) =>
          coid shouldBe AbsoluteContractId(originalCoid)
          consuming shouldBe true
          actingParties shouldBe Set("Bob")
          children shouldBe ImmArray(Tx.NodeId.unsafeFromIndex(1))
          choice shouldBe "Transfer"
        case _ => fail("exercise expected first for Bob")
      }

      bobView.nodes(Tx.NodeId.unsafeFromIndex(1)) match {
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set("Alice", "Clara")
        case _ => fail("create event is expected")
      }

      // clara only sees create
      val claraView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, "Clara", tx)

      val postCommitForClara = engine
        .validatePartial(
          claraView.mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId),
          None,
          let,
          "Clara",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContractForPayout, lookupPackage, lookupKey)

      postCommitForClara shouldBe 'right

      claraView.nodes.size shouldBe 1
      claraView.nodes(Tx.NodeId.unsafeFromIndex(1)) match {
        case NodeCreate(_, coins, _, _, stakeholders, _) =>
          coins.template shouldBe templateId
          stakeholders shouldBe Set("Alice", "Clara")
        case _ => fail("create event is expected")
      }
    }

    "post-commit fail when values are tweaked" in {
      val claraView = Blinding.divulgedTransaction(blindingInfo.localDisclosure, "Clara", tx)
      val tweakedRec =
        assertAsVersionedValue(ValueRecord(
          Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
          ImmArray((Some("giver"), ValueParty("Clara")), (Some("receiver"), ValueParty("Clara")))))
      val tweaked = claraView
        .mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId)
        .nodes + (
        Tx.NodeId.unsafeFromIndex(1) ->
          NodeCreate(
            AbsoluteContractId("0:1"),
            ContractInst(templateId, tweakedRec, ""),
            None,
            Set("Alice"),
            Set("Alice", "Clara"),
            None
          )
      )

      val postCommitForClara = engine
        .validatePartial(
          claraView
            .mapContractIdAndValue(makeAbsoluteContractId, makeValueWithAbsoluteContractId)
            .copy(nodes = tweaked),
          Some("Clara"),
          let,
          "Clara",
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId
        )
        .consume(lookupContractForPayout, lookupPackage, lookupKey)
      postCommitForClara shouldBe 'left
      val Left(err) = postCommitForClara
      err.msg should startWith(
        "Post-commit validation failure: transaction nodes are in disagreement")

    }

    "JIRA-6256 post commit validation for multi root-transaction" in {
      val record =
        assertAsVersionedValue(ValueRecord(
          Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
          ImmArray((Some("giver"), ValueParty("Clara")), (Some("receiver"), ValueParty("Clara")))))
      val node1 = NodeCreate(
        AbsoluteContractId("0:0"),
        ContractInst(templateId, record, ""),
        None,
        Set("Clara"),
        Set("Clara", "Clara"),
        None,
      )

      val node2 = node1.copy(coid = AbsoluteContractId("0:1"))

      val tx = GenTx[NodeId, AbsoluteContractId, VersionedValue[AbsoluteContractId]](
        nodes = Map(
          Tx.NodeId.unsafeFromIndex(0) -> node1,
          Tx.NodeId.unsafeFromIndex(1) -> node2
        ),
        roots = ImmArray(Tx.NodeId.unsafeFromIndex(0), Tx.NodeId.unsafeFromIndex(1)),
        usedPackages = Set.empty,
      )

      val validationResult = engine
        .validatePartial(
          tx,
          None,
          let,
          Party.assertFromString("giver"),
          makeAbsoluteContractId,
          makeValueWithAbsoluteContractId)
        .consume(lookupContractForPayout, lookupPackage, lookupKey)

      val Right(r) = validationResult
      r shouldEqual (())

    }

    "events generated correctly" in {
      val Right(tx) = interpretResult
      val Right(blindingInfo) =
        Blinding.checkAuthorizationAndBlind(tx, Set("Bob"))
      val events = Event.collectEvents(tx, blindingInfo.explicitDisclosure)
      val partyEvents = events.filter(_.witnesses contains "Bob")
      partyEvents.roots.length shouldBe 1
      val bobExercise = partyEvents.events(partyEvents.roots(0))
      bobExercise shouldBe
        ExerciseEvent(
          contractId = AbsoluteContractId(originalCoid),
          templateId = DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout"),
          choice = "Transfer",
          choiceArgument = assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:Transfer")),
              ImmArray((Some("newReceiver"), ValueParty("Clara"))))),
          actingParties = Set("Bob"),
          isConsuming = true,
          children = ImmArray(Tx.NodeId.unsafeFromIndex(1)),
          stakeholders = Set("Bob", "Alice"),
          witnesses = Set("Bob", "Alice"),
          exerciseResult = Some(
            assertAsVersionedValue(
              ValueContractId(RelativeContractId(Tx.NodeId.unsafeFromIndex(1)))))
        )
      val bobVisibleCreate = partyEvents.events(Tx.NodeId.unsafeFromIndex(1))
      bobVisibleCreate shouldBe
        CreateEvent(
          RelativeContractId(Tx.NodeId.unsafeFromIndex(1)),
          DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout"),
          assertAsVersionedValue(
            ValueRecord(
              Some(DefinitionRef(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some("giver"), ValueParty("Alice")),
                (Some("receiver"), ValueParty("Clara"))))),
          Set("Clara", "Alice"),
          Set("Bob", "Clara", "Alice"),
        )
    }
  }

  "dynamic fetch actors" should {
    // Basic test: an exercise (on a "Fetcher" contract) with a single consequence, a fetch of the "Fetched" contract
    // Test a couple of scenarios, with different combination of signatories/observers/actors on the parent action

    val fetchedStrCid = "1"
    val fetchedCid = AbsoluteContractId(fetchedStrCid)
    val fetchedStrTid = "BasicTests:Fetched"
    val fetchedTArgs = ImmArray(
      (Some("sig1"), ValueParty("Alice")),
      (Some("sig2"), ValueParty("Bob")),
      (Some("obs"), ValueParty("Clara"))
    )

    val fetcherStrTid = "BasicTests:Fetcher"
    val fetcherTid = DefinitionRef(basicTestsPkgId, fetcherStrTid)

    val fetcher1StrCid = "2"
    val fetcher1Cid = AbsoluteContractId(fetcher1StrCid)
    val fetcher1TArgs = ImmArray(
      (Some("sig"), ValueParty("Alice")),
      (Some("obs"), ValueParty("Bob")),
      (Some("fetcher"), ValueParty("Clara")),
    )

    val fetcher2StrCid = "3"
    val fetcher2Cid = AbsoluteContractId(fetcher2StrCid)
    val fetcher2TArgs = ImmArray(
      (Some("sig"), ValueParty("Party")),
      (Some("obs"), ValueParty("Alice")),
      (Some("fetcher"), ValueParty("Party")),
    )

    def makeContract[Cid](tid: Ref.QualifiedName, targs: ImmArray[(Option[String], Value[Cid])]) =
      ContractInst(
        DefinitionRef(basicTestsPkgId, tid),
        assertAsVersionedValue(ValueRecord(Some(DefinitionRef(basicTestsPkgId, tid)), targs)),
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
        exerciseActor,
        assertAsVersionedValue(
          ValueRecord(None, ImmArray((Some("cid"), ValueContractId(fetchedCid)))))
      )

      val res = commandTranslator
        .preprocessCommands(Commands(ImmArray(command), let, "test"))
        .consume(lookupContract, lookupPackage, lookupKey)

      res.flatMap(r => engine.interpret(r, let).consume(lookupContract, lookupPackage, lookupKey))

    }

    "propagate the parent's signatories and actors (but not observers) when stakeholders" in {

      val Right(tx) = runExample(fetcher1StrCid, "Clara")
      txFetchActors(tx) shouldBe Set("Alice", "Clara")
    }

    "not propagate the parent's signatories nor actors when not stakeholders" in {

      val Right(tx) = runExample(fetcher2StrCid, "Party")
      txFetchActors(tx) shouldBe Set()
    }
  }

}

object EngineTest {

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toIdentifier(s: String): Identifier =
    Identifier.assertFromString(s)

  private implicit def toParty(s: String): Party =
    Party.assertFromString(s)

  private def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

}
