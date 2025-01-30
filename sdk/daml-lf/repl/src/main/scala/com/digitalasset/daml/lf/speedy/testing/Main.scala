// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package testing

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.archive.UniversalArchiveDecoder
import com.digitalasset.daml.lf.language.LanguageVersion.AllVersions
import com.digitalasset.daml.lf.speedy.Pretty._
import com.digitalasset.daml.lf.speedy.SResult._
import com.digitalasset.daml.lf.speedy.SExpr.LfDefRef
import com.digitalasset.daml.lf.validation.Validation
import com.digitalasset.daml.lf.testing.parser
import com.digitalasset.daml.lf.language.{
  LanguageMajorVersion,
  PackageInterface,
  LanguageVersion => LV,
}
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.stablepackages.StablePackages

import java.io.{File, PrintWriter, StringWriter}
import java.nio.file.{Path, Paths}
import java.io.PrintStream
import org.jline.builtins.Completers
import org.jline.reader.{History, LineReader, LineReaderBuilder}
import org.jline.reader.impl.completer.{AggregateCompleter, ArgumentCompleter, StringsCompleter}
import org.jline.reader.impl.history.DefaultHistory

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object Main extends App {
  // idempotent; force stdout to output in UTF-8 -- in theory it should pick it up from
  // the locale, but in practice it often seems to _not_ do that.
  val out = new PrintStream(System.out, true, "UTF-8")
  System.setOut(out)

  def usage(): Unit =
    println("""
        |usage: daml-lf-speedy COMMAND ARGS...
        |
        |commands:
        |  repl [file]             Run the interactive repl. Load the given packages if any.
        |  validate [file]         Load the given packages and validate them.
        |  [file]                  Same as 'repl' when all given files exist.
    """.stripMargin)

  val (replArgs, compilerConfig) = args.toList match {
    case "--dev" :: rest =>
      rest -> Repl.devCompilerConfig(LV.default.major)
    case list =>
      list -> Repl.defaultCompilerConfig(LV.default.major)
  }
  val repl = new Repl(LV.default.major)
  replArgs match {
    case "-h" :: _ | "--help" :: _ =>
      usage()
    case List("repl", file) =>
      repl.repl(compilerConfig, file)
    case List("validate", file) =>
      if (!repl.validate(compilerConfig, file)._1) System.exit(1)
    case List(possibleFile) if Paths.get(possibleFile).toFile.isFile =>
      repl.repl(compilerConfig, possibleFile)
    case _ =>
      usage()
      System.exit(1)
  }
}

// The Daml-LF Read-Eval-Print-Loop
class Repl(majorLanguageVersion: LanguageMajorVersion) {

  import Repl._

  def repl(compilerConfig: Compiler.Config, darFile: String): Unit =
    repl(load(compilerConfig, darFile) getOrElse initialState(compilerConfig))

  def repl(state0: State): Unit = {
    var state = state0
    state.history.load
    println("Daml-LF -- REPL")
    try {
      while (!state.quit) {
        val line = state.reader.readLine("daml> ")
        state = dispatch(state, line)
      }
    } catch {
      case _: org.jline.reader.EndOfFileException => ()
    }
    state.history.save
  }

  def validate(compilerConfig: Compiler.Config, file: String): (Boolean, State) =
    load(compilerConfig, file) chain
      cmdValidate

  def cmdValidate(state: State): (Boolean, State) = {
    val (validationResults, validationTime) = time(state.packages.map { case (pkgId, pkg) =>
      Validation.checkPackage(
        stablePackages = StablePackages(majorLanguageVersion),
        pkgInterface = PackageInterface(state.packages),
        pkgId = pkgId,
        pkg = pkg,
      )
    })
    System.err.println(s"${state.packages.size} package(s) validated in $validationTime ms.")
    validationResults collectFirst { case Left(e) =>
      println(s"Context: ${e.context}")
      println(s"Error: ${e.getStackTrace.mkString("\n")}")
      false -> state
    } getOrElse true -> state
  }

  // --------------------------------------------------------

  case class Command(help: String, action: (State, Seq[String]) => State)

  def commands = ListMap(
    ":help" -> Command("show this help", (s, _) => { usage(); s }),
    ":reset" -> Command(
      "reset the REPL.",
      (_, _) => initialState(defaultCompilerConfig(majorLanguageVersion)),
    ),
    ":list" -> Command("list loaded packages.", (s, _) => { list(s); s }),
    ":speedy" -> Command(
      "compile given expression to speedy and print it",
      { (s, args) =>
        speedyCompile(s, args); s
      },
    ),
    ":quit" -> Command("quit the REPL.", (s, _) => s.copy(quit = true)),
    ":devmode" -> Command(
      "switch in devMode. This reset the state of REPL.",
      (_, _) => initialState(devCompilerConfig(majorLanguageVersion)),
    ),
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

  def initialState(compilerCompiler: Compiler.Config): State =
    rebuildReader(
      State(
        packages = Map.empty,
        packageFiles = Seq(),
        ScriptRunnerHelper(Map.empty, compilerCompiler, 5.seconds),
        reader = null,
        history = new DefaultHistory(),
        quit = false,
      )
    )

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
          new AggregateCompleter(loadCompleter, definitionCompleter(state), commandCompleter)
        )
        .variable(
          LineReader.HISTORY_FILE,
          Paths.get(System.getProperty("user.home"), ".daml-lf.history"),
        )
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
    state.packages.foreach { case (pkgId, pkg) =>
      println(pkgId)
      pkg.modules.foreach { case (mname, mod) =>
        val maxLen =
          if (mod.definitions.nonEmpty) mod.definitions.map(_._1.toString.length).max
          else 0
        mod.definitions.toList.sortBy(_._1.toString).foreach { case (name, defn) =>
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
      case DValue(typ, _) => prettyType(typ, pkgId, modId)
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
            + " " + args.map(t => prettyType(t, precTApp + 1)).toSeq.mkString(" "),
        )
      case TVar(n) => n
      case TNat(n) => n.toString
      case TTyCon(con) =>
        prettyQualified(pkgId, modId, con)
      case TBuiltin(bt) => bt.toString.stripPrefix("BT")
      case TApp(TApp(TBuiltin(BTArrow), param), result) =>
        maybeParens(
          prec > precTFun,
          prettyType(param, precTFun + 1) + " → " + prettyType(result, precTFun),
        )
      case TApp(fun, arg) =>
        maybeParens(
          prec > precTApp,
          prettyType(fun, precTApp) + " " + prettyType(arg, precTApp + 1),
        )
      case TForall((v, _), body) =>
        maybeParens(prec > precTForall, "∀" + v + prettyForAll(body))
      case TStruct(fields) =>
        "(" + fields.iterator
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

  // Load Daml-LF packages from a set of files.
  def load(
      compilerConfig: Compiler.Config,
      darFile: String,
  ): (Boolean, State) = {
    val state = initialState(compilerConfig)
    try {
      val (packagesMap, loadingTime) =
        time(UniversalArchiveDecoder.assertReadFile(new File(darFile)).all.toMap)

      val npkgs = packagesMap.size
      val ndefs =
        packagesMap.flatMap(_._2.modules.values.map(_.definitions.size)).sum

      System.err.println(s"$ndefs definition(s) from $npkgs package(s) loaded in $loadingTime ms.")

      true -> rebuildReader(
        state.copy(
          packages = packagesMap,
          scriptRunner =
            state.scriptRunner.copy(packages = packagesMap, compilerConfig = compilerConfig),
        )
      )
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
    val defs = assertRight(
      Compiler.compilePackages(
        PackageInterface(state.packages),
        state.packages,
        state.scriptRunner.compilerConfig,
      )
    )
    defs.get(idToRef(state, args(0))) match {
      case None =>
        println("Error: definition '" + args(0) + "' not found. Try :list.")
        usage()
      case Some(defn) =>
        println(Pretty.SExpr.prettySExpr(0)(defn.body).render(80))
    }
  }

  implicit val parserParameters: parser.ParserParameters[Repl.this.type] =
    parser.ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-dummy-"),
      languageVersion = LV.default,
    )

  // Invoke the given top-level function with given arguments.
  // The identifier can be fully-qualified (Foo.Bar@<package id>). If package is not
  // specified, the last used package is used.
  // If the resulting type is a script it is automatically executed.
  def invokePure(state: State, id: String, args: Seq[String]): Unit = {

    parser.parseExprs[this.type](args.mkString(" ")) match {

      case Left(error @ _) =>
        println(s"Error: cannot parser arguments '${args.mkString(" ")}'")

      case Right(argExprs) =>
        lookup(state, id) match {
          case None =>
            println("Error: definition '" + id + "' not found. Try :list.")
            usage()
          case Some(DValue(_, body)) =>
            val expr = argExprs.foldLeft(body)((e, arg) => EApp(e, arg))

            val compiledPackages = PureCompiledPackages.assertBuild(
              state.packages,
              Compiler.Config.Default(majorLanguageVersion),
            )
            val machine = Speedy.Machine.fromPureExpr(compiledPackages, expr)
            val startTime = System.nanoTime()
            val valueOpt = machine.run() match {
              case SResultError(err) =>
                println(prettyError(err).render(128))
                None
              case SResultFinal(v) =>
                Some(v)
              case other =>
                sys.error("unimplemented callback: " + other.toString)
            }
            val endTime = System.nanoTime()
            val diff = (endTime - startTime) / 1000 / 1000
            println(s"time: ${diff}ms")
            valueOpt match {
              case None => ()
              case Some(value) =>
                val result = prettyValue(true)(value.toUnnormalizedValue).render(128)
                println("result:")
                println(result)
            }
          case Some(_) =>
            println("Error: " + id + " not a value.")
        }
    }
  }

  def buildExprFromTest(state: State, idAndArgs: Seq[String]): Option[Expr] =
    idAndArgs match {
      case id :: args =>
        lookup(state, id) match {
          case None =>
            println("Error: " + id + " not found.")
            None
          case Some(DValue(_, body)) =>
            val argExprs = args.map(s => assertRight(parser.parseExpr(s)))
            Some(argExprs.foldLeft(body)((e, arg) => EApp(e, arg)))
          case Some(_) =>
            println("Error: " + id + " is not a test.")
            None
        }
      case _ =>
        usage()
        None
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
            } yield defn
          )
          .headOption

    }
  }

  def usage(): Unit = {
    val cmds = commands
      .map { case (name: String, cmd: Command) =>
        val help: String = cmd.help
        f"| $name%-25s $help"
      }
      .mkString("\n")
    println(s"""
      |Daml-LF Read-Eval-Print-Loop. Supported commands:
      |
      $cmds
      | <function> <args>...      call the given pure function with given arguments.
    """.stripMargin)
  }

  private def assertRight[X](e: Either[String, X]): X =
    e.fold(err => throw new RuntimeException(err), identity)
}

object Repl {
  implicit def logContext: LoggingContext = LoggingContext.ForTesting

  def defaultCompilerConfig(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
    Compiler.Config(
      allowedLanguageVersions = LV.StableVersions(majorLanguageVersion),
      packageValidation = Compiler.FullPackageValidation,
      profiling = Compiler.NoProfile,
      stacktracing = Compiler.FullStackTrace,
    )

  def devCompilerConfig(majorLanguageVersion: LanguageMajorVersion): Compiler.Config =
    defaultCompilerConfig(majorLanguageVersion).copy(allowedLanguageVersions =
      LV.AllVersions(majorLanguageVersion)
    )

  private implicit class StateOp(val x: (Boolean, State)) extends AnyVal {

    def chain(f: State => (Boolean, State)): (Boolean, State) =
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

  case class State(
      packages: Map[PackageId, Package],
      packageFiles: Seq[String],
      scriptRunner: ScriptRunnerHelper,
      reader: LineReader,
      history: History,
      quit: Boolean,
  ) {
    def compilerConfig: Compiler.Config = scriptRunner.compilerConfig
  }

  case class ScriptRunnerHelper(
      packages: Map[PackageId, Package],
      compilerConfig: Compiler.Config,
      timeout: Duration,
  ) {

    val (compiledPackages, compileTime) =
      time(PureCompiledPackages.assertBuild(packages, compilerConfig))

    System.err.println(s"${packages.size} package(s) compiled in $compileTime ms.")

    val transactionVersions =
      if (compilerConfig.allowedLanguageVersions.intersects(AllVersions(LanguageMajorVersion.V2))) {
        transaction.TransactionVersion.DevVersions
      } else {
        transaction.TransactionVersion.StableVersions
      }
  }

  private def time[R](block: => R): (R, Long) = {
    val startTime = System.nanoTime()
    val result = block // call-by-name
    val endTime = System.nanoTime()
    result -> Duration.fromNanos(endTime - startTime).toMillis
  }
}
