// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.speedy.Pretty._
import com.digitalasset.daml.lf.speedy.SError._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.types.LedgerForScenarios
import com.digitalasset.daml.lf.value.Value
import Value._
import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Path, Paths}
import java.io.PrintStream

import com.digitalasset.daml.lf.speedy.SExpr.LfDefRef
import com.digitalasset.daml.lf.{PureCompiledPackages, UniversalArchiveReader}
import com.digitalasset.daml.lf.validation.Validation
import org.jline.builtins.Completers
import org.jline.reader.{History, LineReader, LineReaderBuilder}
import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, StringsCompleter}
import org.jline.reader.impl.history.DefaultHistory

import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import scala.util.parsing.combinator._

object Main extends App {
  // idempotent; force stdout to output in UTF-8 -- in theory it should pick it up from
  // the locale, but in practice it often seems to _not_ do that.
  val out = new PrintStream(System.out, true, "UTF-8")
  System.setOut(out)

  def usage(): Unit = {
    println(
      """
     |usage: daml-lf-speedy COMMAND ARGS...
     |
     |commands:
     |  repl [file]             Run the interactive repl. Load the given packages if any.
     |  test <name> [file]      Load given packages and run the named scenario with verbose output.
     |  testAll [file]          Load the given packages and run all scenarios.
     |  validate [file]         Load the given packages and validate them.
     |  [file]                  Same as 'repl' when all given files exist.
    """.stripMargin)

  }

  def defaultCommand(possibleFile: String): Unit =
    if (!Paths.get(possibleFile).toFile.isFile) {
      usage()
      System.exit(1)
    } else
      Repl.repl(possibleFile)

  if (args.isEmpty) {
    usage()
    System.exit(1)
  } else {
    var replArgs = args.toList
    var allowDev = false
    replArgs match {
      case "--decode-lfdev" :: rest =>
        replArgs = rest
        allowDev = true
      case _ => ()
    }
    replArgs match {
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case List("repl", file) => Repl.repl(file)
      case List("testAll", file) =>
        if (!Repl.testAll(allowDev, file)._1) System.exit(1)
      case List("test", id, file) =>
        if (!Repl.test(allowDev, id, file)._1) System.exit(1)
      case List("validate", file) =>
        if (!Repl.validate(allowDev, file)._1) System.exit(1)
      case List(possibleFile) =>
        defaultCommand(possibleFile)
      case _ =>
        usage()
        System.exit(1)
    }
  }
}

// The DAML-LF Read-Eval-Print-Loop
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  ))
object Repl {

  def repl(): Unit = repl(initialState())
  def repl(darFile: String): Unit = repl(load(darFile))
  def repl(state0: State): Unit = {
    var state = state0
    state.history.load
    println("DAML-LF -- REPL")
    try {
      while (!state.quit) {
        val line = state.reader.readLine("daml> ")
        state = dispatch(state, line)
      }
    } catch {
      case _: org.jline.reader.EndOfFileException => Unit
    }
    state.history.save
  }

  def testAll(allowDev: Boolean, file: String): (Boolean, State) = {
    val state = load(file)
    cmdValidate(state)
    cmdTestAll(state)
  }

  def test(allowDev: Boolean, id: String, file: String): (Boolean, State) = {
    val state = load(file)
    cmdValidate(state)
    invokeScenario(state, Seq(id))
  }

  def validate(allowDev: Boolean, file: String): (Boolean, State) = {
    val state = load(file)
    cmdValidate(state)
  }

  def cmdValidate(state: State): (Boolean, State) = {
    val validationResults = state.packages.keys.map(Validation.checkPackage(state.packages, _))
    validationResults collectFirst {
      case Left(e) =>
        println(s"Context: ${e.context}")
        println(s"Error: ${e.getStackTrace.mkString("\n")}")
        false -> state
    } getOrElse true -> state
  }

  // --------------------------------------------------------

  case class State(
      packages: Map[PackageId, Package],
      packageFiles: Seq[String],
      scenarioRunner: ScenarioRunnerHelper,
      reader: LineReader,
      history: History,
      quit: Boolean
  )

  case class ScenarioRunnerHelper(packages: Map[PackageId, Package]) {
    private val build = Speedy.Machine
      .newBuilder(PureCompiledPackages(packages).right.get)
      .fold(err => sys.error(err.toString), identity)
    def run(expr: Expr): (
        Speedy.Machine,
        Either[(SError, LedgerForScenarios.Ledger), (Double, Int, LedgerForScenarios.Ledger)]) = {
      val mach = build(expr)
      (mach, ScenarioRunner(mach).run())
    }
  }

  case class Command(help: String, action: (State, Seq[String]) => State)

  final val commands = ListMap(
    ":help" -> Command("show this help", (s, _) => { usage(); s }),
    ":reset" -> Command("reset the REPL.", (s, _) => initialState()),
    ":list" -> Command("list loaded packages.", (s, _) => { list(s); s }),
    ":speedy" -> Command("compile given expression to speedy and print it", (s, args) => {
      speedyCompile(s, args); s
    }),
    ":quit" -> Command("quit the REPL.", (s, _) => s.copy(quit = true)),
    ":scenario" -> Command(
      "execute the scenario expression pointed to by given identifier.",
      (s, args) => { invokeScenario(s, args); s }),
    ":testall" -> Command("run all loaded scenarios.", (s, _) => {
      cmdTestAll(s); s
    }),
    ":validate" -> Command("validate all the packages", (s, _) => { cmdValidate(s); s }),
  )
  final val commandCompleter = new ArgumentCompleter(new StringsCompleter(commands.keys.asJava))

  final class DalfFileCompleter extends Completers.FileNameCompleter {
    override def accept(path: Path): Boolean = {
      if (path.toFile.isDirectory)
        true
      else
        path.toString.endsWith(".dalf")
    }
  }

  final val loadCompleter = {
    val cmpl = new ArgumentCompleter(new StringsCompleter(":load"), new DalfFileCompleter())
    cmpl.setStrict(true)
    cmpl
  }

  def initialState(): State =
    rebuildReader(
      State(
        packages = Map.empty,
        packageFiles = Seq(),
        ScenarioRunnerHelper(Map.empty),
        reader = null,
        history = new DefaultHistory(),
        quit = false
      ))

  def definitionCompleter(state: State): StringsCompleter = {
    val names =
      for {
        pkg <- state.packages.values
        module <- pkg.modules
        (modName, mod) = module
        dfnName <- mod.definitions.keys
      } yield QualifiedName(modName, dfnName).toString
    new StringsCompleter(names.asJava)
  }

  def rebuildReader(state: State): State =
    state.copy(
      reader = LineReaderBuilder.builder
        .appName("daml-lf-repl")
        .completer(
          new AggregateCompleter(loadCompleter, definitionCompleter(state), commandCompleter))
        .variable(
          LineReader.HISTORY_FILE,
          Paths.get(System.getProperty("user.home"), ".daml-lf.history"))
        .history(state.history)
        .build
    )

  def dispatch(state: State, line: String): State = {
    line.split(" ").toList match {
      case Nil => state
      case "" :: _ => state
      case cmdS :: args =>
        commands.get(cmdS) match {
          case Some(cmd) => cmd.action(state, args)
          case None => invokePure(state, cmdS, args); state
        }
    }
  }

  def list(state: State): Unit = {
    state.packages.foreach {
      case (pkgId, pkg) =>
        println(pkgId)
        pkg.modules.foreach {
          case (mname, mod) =>
            println("  " + mname.toString + ":")
            val maxLen =
              if (mod.definitions.nonEmpty) mod.definitions.map(_._1.toString.length).max
              else 0
            mod.definitions.toList.sortBy(_._1.toString).foreach {
              case (name, defn) =>
                val paddedName = name.toString.padTo(maxLen, ' ')
                val typ = prettyDefinitionType(defn, pkgId, mod.name)
                println(s"    $paddedName ∷ $typ")
            }
        }
    }
  }

  def prettyDefinitionType(defn: Definition, pkgId: PackageId, modId: ModuleName): String =
    defn match {
      case DDataType(_, _, _) => "<data type>" // FIXME(JM): pp this
      case DValue(typ, _, _, _) => prettyType(typ, pkgId, modId)
    }

  def prettyQualified(pkgId: PackageId, modId: ModuleName, m: Identifier): String = {
    if (pkgId == m.packageId)
      if (modId == m.qualifiedName.module)
        m.qualifiedName.name.toString
      else
        m.qualifiedName.toString
    else
      m.qualifiedName.toString + "@" + m.packageId
  }

  def prettyType(typ: Type, pkgId: PackageId, modId: ModuleName): String = {
    val precTApp = 2
    val precTFun = 1
    val precTForall = 0

    def maybeParens(needParens: Boolean, s: String): String =
      if (needParens) s"($s)" else s

    def prettyType(t0: Type, prec: Int = precTForall): String = t0 match {
      case TVar(n) => n
      case TTyCon(con) =>
        prettyQualified(pkgId, modId, con)
      case TBuiltin(bt) => bt.toString.stripPrefix("BT")
      case TApp(TApp(TBuiltin(BTArrow), param), result) =>
        maybeParens(
          prec > precTFun,
          prettyType(param, precTFun + 1) + " → " + prettyType(result, precTFun))
      case TApp(fun, arg) =>
        maybeParens(
          prec > precTApp,
          prettyType(fun, precTApp) + " " + prettyType(arg, precTApp + 1))
      case TForall((v, _), body) =>
        maybeParens(prec > precTForall, "∀" + v + prettyForAll(body))
      case TTuple(fields) =>
        "(" + fields
          .map { case (n, t) => n + ": " + prettyType(t, precTForall) }
          .toSeq
          .mkString(", ") + ")"
    }

    def prettyForAll(t: Type): String = t match {
      case TForall((v, _), body) => " " + v + prettyForAll(body)
      case _ => ". " + prettyType(t, precTForall)
    }

    prettyType(typ)
  }

  // Load DAML-LF packages from a set of files.
  def load(darFile: String): State = {
    val state = initialState()
    try {
      val packages =
        UniversalArchiveReader().readFile(new File(darFile)).get
      val packagesMap = Map(packages.all.map {
        case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
      }: _*)
      val (mainPkgId, mainPkgArchive) = packages.main
      val mainPkg = Decode.readArchivePayloadAndVersion(mainPkgId, mainPkgArchive)._1._2
      val npkgs = packagesMap.size
      val ndefs =
        packagesMap.flatMap(_._2.modules.values.map(_.definitions.size)).sum
      println(s"$ndefs definitions from $npkgs package(s) loaded.")

      rebuildReader(
        state.copy(
          packages = packagesMap,
          scenarioRunner = ScenarioRunnerHelper(packagesMap)
        ))
    } catch {
      case ex: Throwable => {
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        println("Failed to load packages: " + ex.toString + ", stack trace: " + sw.toString)
        state
      }
    }
  }

  def speedyCompile(state: State, args: Seq[String]): Unit = {
    val compiler = Compiler(state.packages)
    val defs = compiler.compilePackages(state.packages.keys)
    defs.get(idToRef(state, args(0))) match {
      case None =>
        println("Error: definition '" + args(0) + "' not found. Try :list."); usage
      case Some(expr) =>
        println(Pretty.SExpr.prettySExpr(0)(expr).render(80))
    }
  }

  // Invoke the given top-level function with given arguments.
  // The identifier can be fully-qualified (Foo.Bar@<package id>). If package is not
  // specified, the last used package is used.
  // If the resulting type is a scenario it is automatically executed.
  def invokePure(state: State, id: String, args: Seq[String]): Unit = {
    //try {
    val argExprs = ValueParser.parseArgs(args).map(valueToExpr)
    lookup(state, id) match {
      case None =>
        println("Error: definition '" + id + "' not found. Try :list."); usage
      case Some(DValue(_, _, body, _)) =>
        val expr = argExprs.foldLeft(body)((e, arg) => EApp(e, arg))

        val machine =
          Speedy.Machine.fromExpr(expr, PureCompiledPackages(state.packages).right.get, false)
        var count = 0
        val startTime = System.nanoTime()
        var errored = false
        while (!machine.isFinal && !errored) {
          machine.print(count)
          machine.step match {
            case SResultError(err) =>
              println(prettyError(err, machine.ptx).render(128))
              errored = true
            case SResultContinue =>
              ()
            case other =>
              sys.error("unimplemented callback: " + other.toString)
          }
          count += 1
        }
        val endTime = System.nanoTime()
        val diff = (endTime - startTime) / 1000 / 1000
        machine.print(count)
        println(s"steps: $count")
        println(s"time: ${diff}ms")
        if (!errored) {
          val result = machine.ctrl match {
            case Speedy.CtrlValue(sv) =>
              prettyValue(true)(sv.toValue).render(128)
            case x => x.toString
          }
          println("result:")
          println(result)
        }
      case Some(_) =>
        println("Error: " + id + " not a value.")
    }
  }

  def buildExpr(state: State, idAndArgs: Seq[String]): Option[Expr] =
    idAndArgs match {
      case id :: args =>
        lookup(state, id) match {
          case None =>
            println("Error: " + id + " not found.")
            None
          case Some(DValue(_, _, body, _)) =>
            val argExprs = args.map(ValueParser.parse).map(valueToExpr)
            Some(argExprs.foldLeft(body)((e, arg) => EApp(e, arg)))
          case Some(_) =>
            println("Error: " + id + " is not a value.")
            None
        }
      case _ =>
        usage(); None
    }

  def invokeScenario(state: State, idAndArgs: Seq[String]): (Boolean, State) = {
    buildExpr(state, idAndArgs)
      .map { expr =>
        val (machine, errOrLedger) =
          state.scenarioRunner.run(expr)
        errOrLedger match {
          case Left((err, ledger @ _)) =>
            println(prettyError(err, machine.ptx).render(128))
            (false, state)
          case Right((diff @ _, steps @ _, ledger)) =>
            // NOTE(JM): cannot print this, output used in tests.
            //println(s"done in ${diff.formatted("%.2f")}ms, ${steps} steps")
            println(prettyLedger(ledger).render(128))
            (true, state)
        }
      }
      .getOrElse((false, state))
  }

  def cmdTestAll(state0: State): (Boolean, State) = {
    val allScenarios =
      for {
        pkg <- state0.packages.values
        module <- pkg.modules
        (modName, mod) = module
        definition <- mod.definitions
        (dfnName, dfn) = definition
        bodyScenario <- List(dfn).collect { case DValue(TScenario(_), _, body, _) => body }
      } yield QualifiedName(modName, dfnName).toString -> bodyScenario
    var failures = 0
    var successes = 0
    val state = state0
    var totalTime = 0.0
    var totalSteps = 0
    allScenarios.foreach {
      case (name, body) =>
        print(name + ": ")
        val (machine, errOrLedger) = state.scenarioRunner.run(body)
        errOrLedger match {
          case Left((err, ledger @ _)) =>
            println(
              "failed at " +
                prettyLoc(machine.lastLocation).render(128) +
                ": " + prettyError(err, machine.ptx).render(128))
            failures += 1
          case Right((diff, steps, ledger @ _)) =>
            successes += 1
            totalTime += diff
            totalSteps += steps
            println(s"ok in ${diff.formatted("%.2f")}ms, $steps steps")
        }
    }
    println(
      s"\n$successes passed, $failures failed, total time ${totalTime.formatted("%.2f")}ms, total steps $totalSteps.")
    (failures == 0, state)
  }

  private val unknownPackageId = PackageId.assertFromString("-unknownPackage-")

  def idToRef(state: State, id: String): LfDefRef = {
    val defaultPackageId =
      state.packages.headOption
        .map(_._1)
        .getOrElse(unknownPackageId)

    val (defRef, packageId): (String, PackageId) =
      id.split("@").toList match {
        case defRef :: packageId :: Nil => (defRef, PackageId.assertFromString(packageId))
        case _ => (id, defaultPackageId)
      }
    val qualName = QualifiedName.fromString(defRef) match {
      case Left(err) => sys.error(s"Cannot parse qualified name $defRef: $err")
      case Right(x) => x
    }
    LfDefRef(DefinitionRef(packageId, qualName))
  }

  def lookup(state: State, id: String): Option[Definition] = {
    val (defRef, optPackageId): (String, Option[PackageId]) =
      id.split("@").toList match {
        case defRef :: packageId :: Nil =>
          (defRef, Some(PackageId.assertFromString(packageId)))
        case _ => (id, None)
      }
    val qualName = QualifiedName.fromString(defRef) match {
      case Left(err) => sys.error(s"Cannot parse qualified name $defRef: $err")
      case Right(x) => x
    }
    optPackageId match {
      case Some(packageId) =>
        state.packages
          .get(packageId)
          .flatMap(pkg => pkg.modules.get(qualName.module))
          .flatMap(_.definitions.get(qualName.name))
      case None =>
        state.packages.view
          .flatMap { case (pkgId @ _, pkg) => pkg.modules.get(qualName.module).toList }
          .flatMap(_.definitions.get(qualName.name).toList)
          .headOption

    }
  }

  def usage(): Unit = {
    val cmds = commands
      .map {
        case (name: String, cmd: Command) =>
          val help: String = cmd.help
          f"| $name%-25s $help"
      }
      .mkString("\n")
    println(s"""
      |DAML-LF Read-Eval-Print-Loop. Supported commands:
      |
      $cmds
      | <function> <args>...      call the given pure function with given arguments.
    """.stripMargin)
  }

  object ValueParser extends RegexParsers {
    // FIXME(JM): Parse types as well to allow creation of nominal records?
    val dummyId =
      Ref.Identifier(
        PackageId.assertFromString("-dummy package-"),
        QualifiedName.assertFromString("Dummy:Dummy"))

    def pDecimal: Parser[Value[Nothing]] = """\d+\.\d+""".r ^^ { s =>
      ValueDecimal(Decimal.assertFromString(s))
    }
    def pInt64: Parser[Value[Nothing]] = """\d+""".r ^^ { s =>
      ValueInt64(s.toLong)
    }
    def pText: Parser[Value[Nothing]] = """\"(\w*)\"""".r ^^ { s =>
      ValueText(s.stripPrefix("\"").stripSuffix("\""))
    }
    def pTrue: Parser[Value[Nothing]] = "true" ^^ { _ =>
      ValueBool(true)
    }
    def pFalse: Parser[Value[Nothing]] = "false" ^^ { _ =>
      ValueBool(false)
    }
    def pVariant: Parser[Value[Nothing]] =
      """[A-Z][a-z]*""".r ~ ("(" ~> pValue <~ ")").? ^^ {
        case variant ~ optValue =>
          ValueVariant(Some(dummyId), Name.assertFromString(variant), optValue.getOrElse(ValueUnit))
      }
    def pList: Parser[Value[Nothing]] =
      """\[\s*""".r ~> (pValue <~ """\s*,\s*""".r).* ~ pValue.? <~ """\s*\]""".r ^^ {
        case front ~ Some(last) => ValueList(FrontStack(ImmArray(front :+ last)))
        case _ => ValueList(FrontStack.empty)
      }

    def pField: Parser[(Name, Value[Nothing])] =
      ("""(\w+)""".r ~ """\s*=\s*""".r ~ pValue) ^^ {
        case field ~ _ ~ value => Name.assertFromString(field) -> value
      }
    def pFields: Parser[List[(Name, Value[Nothing])]] =
      ("""\s*""".r ~> pField <~ """\ *,\ *""".r).* ~ pField.? ^^ {
        case fs ~ Some(last) => fs :+ last
        case _ => List()
      }
    def pRecord: Parser[Value[Nothing]] = "{" ~> pFields <~ "}" ^^ { fs =>
      ValueRecord(Some(dummyId), ImmArray(fs.map {
        case (lbl, v) => (Some(lbl), v)
      }))
    }

    def pValue: Parser[Value[Nothing]] =
      pDecimal | pInt64 | pVariant | pRecord | pList | pText

    def pValues: Parser[Seq[Value[Nothing]]] = (pValue <~ """\s*""".r).*

    case class ParseError(error: String) extends RuntimeException(error)

    def parse(s: String): Value[Nothing] =
      parseAll(pValue, s) match {
        case Success(v, _) => v
        case err => throw ParseError(err.toString)
      }

    def parseArgs(s: Seq[String]): Seq[Value[Nothing]] =
      parseAll(pValues, s.mkString(" ")) match {
        case Success(v, _) => v
        case err => throw ParseError(err.toString)
      }
  }

  def debugParse(args: Seq[String]): Unit =
    try {
      println(
        ValueParser
          .parseArgs(args)
          .map(x => prettyValue(true)(x).render(128))
          .mkString("\n"))
    } catch {
      case ValueParser.ParseError(err) => println("parse error: " + err)
    }

  def valueToExpr(v: Value[AbsoluteContractId]): Expr = {
    // FIXME(JM): Fix the handling of types.
    val dummyTyCon =
      TypeConName(
        PackageId.assertFromString("-dummy-"),
        QualifiedName.assertFromString("Dummy:Dummy"))
    val dummyTyApp = TypeConApp(dummyTyCon, ImmArray.empty)
    v match {
      case ValueText(s) => EPrimLit(PLText(s))
      case ValueInt64(i) => EPrimLit(PLInt64(i))
      case ValueDecimal(d) => EPrimLit(PLDecimal(d))
      case ValueVariant(_, variant, value) =>
        EVariantCon(dummyTyApp, variant, valueToExpr(value))
      case ValueRecord(_, fs) =>
        ETupleCon(fs.map(kv => (kv._1.get, valueToExpr(kv._2))))
      case ValueTuple(fs) =>
        ETupleCon(fs.map(kv => (kv._1, valueToExpr(kv._2))))
      case ValueUnit => EPrimCon(PCUnit)
      case ValueBool(b) => EPrimCon(if (b) PCTrue else PCFalse)
      case ValueList(xs) =>
        ECons(TTyCon(dummyTyCon), xs.map(valueToExpr).toImmArray, ENil(TTyCon(dummyTyCon)))
      case ValueParty(p) => EPrimLit(PLParty(p))
      case ValueTimestamp(t) => EPrimLit(PLTimestamp(t))
      case ValueDate(t) => EPrimLit(PLDate(t))
      case ValueContractId(acoid) => EContractId(acoid.coid, dummyTyCon)
      case ValueOptional(Some(e)) => ESome(TTyCon(dummyTyCon), valueToExpr(e))
      case ValueOptional(None) => ENone(TTyCon(dummyTyCon))
      case ValueMap(_) =>
        sys.error(s"Cannot represent Map in expressions")
    }
  }
}
