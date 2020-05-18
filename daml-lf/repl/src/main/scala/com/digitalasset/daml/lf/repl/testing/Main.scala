// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package testing

import com.daml.lf.data._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.language.Util._
import com.daml.lf.speedy.Pretty._
import com.daml.lf.speedy.SError._
import com.daml.lf.speedy.SResult._
import com.daml.lf.types.Ledger
import com.daml.lf.speedy.SExpr.LfDefRef
import com.daml.lf.validation.Validation
import com.daml.lf.testing.parser
import com.daml.lf.language.LanguageVersion
import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Path, Paths}
import java.io.PrintStream

import org.jline.builtins.Completers
import org.jline.reader.{History, LineReader, LineReaderBuilder}
import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, StringsCompleter}
import org.jline.reader.impl.history.DefaultHistory

import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._

object Main extends App {
  // idempotent; force stdout to output in UTF-8 -- in theory it should pick it up from
  // the locale, but in practice it often seems to _not_ do that.
  val out = new PrintStream(System.out, true, "UTF-8")
  System.setOut(out)

  case class ProfileArgs(scenarioId: String, inputFile: String, outputFile: String)

  def usage(): Unit = {
    println(
      """
     |usage: daml-lf-speedy COMMAND ARGS...
     |
     |commands:
     |  repl [file]             Run the interactive repl. Load the given packages if any.
     |  test <name> [file]      Load given packages and run the named scenario with verbose output.
     |  testAll [file]          Load the given packages and run all scenarios.
     |  profile <name> [infile] [outfile]  Run the name scenario and write a profile in speedscope.app format
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
      case List("profile", scenarioId, inputFile, outputFile) =>
        if (!Repl.profile(allowDev, scenarioId, inputFile, outputFile)._1) System.exit(1)
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

  private val nextSeed =
    // We use a static seed to get reproducible run
    crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey("lf-repl"))

  def repl(): Unit = repl(initialState())
  def repl(darFile: String): Unit = repl(load(darFile) getOrElse initialState())
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

  private implicit class StateOp(val x: (Boolean, State)) extends AnyVal {
    def fMap(f: State => (Boolean, State)): (Boolean, State) =
      x match {
        case (true, state) => f(state)
        case _ => x
      }

    def getOrElse(default: => State) =
      x match {
        case (true, state) => state
        case _ => default
      }
  }

  def testAll(allowDev: Boolean, file: String): (Boolean, State) =
    load(file) fMap cmdValidate fMap cmdTestAll

  def test(allowDev: Boolean, id: String, file: String): (Boolean, State) =
    load(file) fMap cmdValidate fMap (x => invokeScenario(x, Seq(id)))

  def profile(
      allowDev: Boolean,
      scenarioId: String,
      inputFile: String,
      outputFile: String,
  ): (Boolean, State) =
    load(inputFile, Compiler.FullProfile) fMap cmdValidate fMap (state =>
      cmdProfile(state, scenarioId, outputFile))

  def validate(allowDev: Boolean, file: String): (Boolean, State) =
    load(file) fMap cmdValidate

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

  case class ScenarioRunnerHelper(
      packages: Map[PackageId, Package],
      profiling: Compiler.ProfilingMode) {
    private val build = Speedy.Machine
      .newBuilder(
        PureCompiledPackages(packages, profiling).right.get,
        Time.Timestamp.MinValue,
        nextSeed())
      .fold(err => sys.error(err.toString), identity)
    def run(expr: Expr)
      : (Speedy.Machine, Either[(SError, Ledger.Ledger), (Double, Int, Ledger.Ledger, SValue)]) = {
      val machine = build(expr)
      (machine, ScenarioRunner(machine).run())
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

  def initialState(profiling: Compiler.ProfilingMode = Compiler.NoProfile): State =
    rebuildReader(
      State(
        packages = Map.empty,
        packageFiles = Seq(),
        ScenarioRunnerHelper(Map.empty, profiling),
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
    line.trim.split(" ").toList match {
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
            val maxLen =
              if (mod.definitions.nonEmpty) mod.definitions.map(_._1.toString.length).max
              else 0
            mod.definitions.toList.sortBy(_._1.toString).foreach {
              case (name, defn) =>
                val paddedName = name.toString.padTo(maxLen, ' ')
                val typ = prettyDefinitionType(defn, pkgId, mod.name)
                println(s"    ${mname.toString}:$paddedName ∷ $typ")
            }
        }
    }
  }

  def prettyDefinitionType(defn: Definition, pkgId: PackageId, modId: ModuleName): String =
    defn match {
      case DTypeSyn(_, _) => "<type synonym>" // FIXME: pp this
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
      case TSynApp(syn, args) =>
        maybeParens(
          prec > precTApp,
          prettyQualified(pkgId, modId, syn)
            + " " + args.map(t => prettyType(t, precTApp + 1)).toSeq.mkString(" ")
        )
      case TVar(n) => n
      case TNat(n) => n.toString
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
      case TStruct(fields) =>
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
  def load(
      darFile: String,
      profiling: Compiler.ProfilingMode = Compiler.NoProfile,
  ): (Boolean, State) = {
    val state = initialState(profiling)
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

      true -> rebuildReader(
        state.copy(
          packages = packagesMap,
          scenarioRunner = ScenarioRunnerHelper(packagesMap, profiling)
        ))
    } catch {
      case ex: Throwable => {
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        println("Failed to load packages: " + ex.toString + ", stack trace: " + sw.toString)
        (false, state)
      }
    }
  }

  def speedyCompile(state: State, args: Seq[String]): Unit = {
    val defs = assertRight(Compiler.compilePackages(state.packages, Compiler.NoProfile))
    defs.get(idToRef(state, args(0))) match {
      case None =>
        println("Error: definition '" + args(0) + "' not found. Try :list."); usage
      case Some(expr) =>
        println(Pretty.SExpr.prettySExpr(0)(expr).render(80))
    }
  }

  implicit val parserParameters: parser.ParserParameters[Repl.this.type] =
    parser.ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-dummy-"),
      languageVersion = LanguageVersion.defaultV1,
    )

  // Invoke the given top-level function with given arguments.
  // The identifier can be fully-qualified (Foo.Bar@<package id>). If package is not
  // specified, the last used package is used.
  // If the resulting type is a scenario it is automatically executed.
  def invokePure(state: State, id: String, args: Seq[String]): Unit = {

    parser.parseExprs[this.type](args.mkString(" ")) match {

      case Left(error) =>
        println(s"Error: cannot parser arguments '${args.mkString(" ")}'")

      case Right(argExprs) =>
        lookup(state, id) match {
          case None =>
            println("Error: definition '" + id + "' not found. Try :list.")
            usage
          case Some(DValue(_, _, body, _)) =>
            val expr = argExprs.foldLeft(body)((e, arg) => EApp(e, arg))

            val machine =
              Speedy.Machine.fromExpr(
                expr = expr,
                compiledPackages = PureCompiledPackages(state.packages).right.get,
                scenario = false,
                submissionTime = Time.Timestamp.now(),
                initialSeeding = InitialSeeding.NoSeed,
              )
            val startTime = System.nanoTime()
            val valueOpt = machine.run match {
              case SResultError(err) =>
                println(prettyError(err, machine.ptx).render(128))
                None
              case SResultFinalValue(v) =>
                Some(v)
              case other =>
                sys.error("unimplemented callback: " + other.toString)
            }
            val endTime = System.nanoTime()
            val diff = (endTime - startTime) / 1000 / 1000
            machine.print(1)
            println(s"time: ${diff}ms")
            valueOpt match {
              case None => ()
              case Some(value) =>
                val result = prettyValue(true)(value.toValue).render(128)
                println("result:")
                println(result)
            }
          case Some(_) =>
            println("Error: " + id + " not a value.")
        }
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
            val argExprs = args.map(s => assertRight(parser.parseExpr(s)))
            Some(argExprs.foldLeft(body)((e, arg) => EApp(e, arg)))
          case Some(_) =>
            println("Error: " + id + " is not a value.")
            None
        }
      case _ =>
        usage()
        None
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
          case Right((diff @ _, steps @ _, ledger, value @ _)) =>
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
          case Right((diff, steps, ledger @ _, value @ _)) =>
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

  def cmdProfile(state: State, scenarioId: String, outputFile: String): (Boolean, State) = {
    buildExpr(state, Seq(scenarioId))
      .map { expr =>
        println("Warming up JVM for 10s...")
        val start = System.nanoTime()
        while (System.nanoTime() - start < 10L * 1000 * 1000 * 1000) {
          state.scenarioRunner.run(expr)
        }
        println("Collecting profile...")
        val (machine, errOrLedger) =
          state.scenarioRunner.run(expr)
        errOrLedger match {
          case Left((err, ledger @ _)) =>
            println(prettyError(err, machine.ptx).render(128))
            (false, state)
          case Right((diff @ _, steps @ _, ledger, value @ _)) =>
            println("Writing profile...")
            machine.profile.name = scenarioId
            machine.profile.writeSpeedscopeJson(outputFile)
            (true, state)
        }
      }
      .getOrElse((false, state))
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
        for {
          pkg <- state.packages.get(packageId)
          module <- pkg.modules.get(qualName.module)
          defn <- module.definitions.get(qualName.name)
        } yield defn
      case None =>
        state.packages.values.view
          .flatMap(pkg =>
            for {
              module <- pkg.modules.get(qualName.module).toList
              defn <- module.definitions.get(qualName.name).toList
            } yield defn)
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

  private def assertRight[X](e: Either[String, X]): X =
    e.fold(err => throw new RuntimeException(err), identity)
}
