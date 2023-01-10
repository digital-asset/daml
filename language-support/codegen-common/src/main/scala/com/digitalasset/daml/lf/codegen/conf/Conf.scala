// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.conf

import java.nio.file.{Path, Paths}

import ch.qos.logback.classic.Level
import com.daml.buildinfo.BuildInfo
import com.daml.lf.data.Ref.{PackageName, PackageVersion}
import scopt.{OptionParser, Read}

sealed trait PackageReference extends Product with Serializable

object PackageReference {
  // TODO (MK) https://github.com/digital-asset/daml/issues/9934
  // We probably want to allow package id references here
  // but this needs to be supported in damlc first.
  final case class NameVersion(name: PackageName, version: PackageVersion)
      extends PackageReference {
    override final def toString(): String =
      s"$name-$version"
  }
}

/** @param darFiles The [[Set]] of Daml-LF [[Path]]s to convert into code. It MUST contain
  *                        all the Daml-LF packages dependencies.
  * @param outputDirectory The directory where the code will be generated
  * @param decoderPkgAndClass the fully qualified name of the generated decoder class (optional)
  */
final case class Conf(
    darFiles: Map[Path, Option[String]] = Map(),
    outputDirectory: Path,
    modulePrefixes: Map[PackageReference, String] = Map.empty,
    decoderPkgAndClass: Option[(String, String)] = None,
    verbosity: Level = Level.ERROR,
    roots: List[String] = Nil,
)

object Conf {

  private[conf] final val PackageAndClassRegex =
    """(?:(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}+(?:\.\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}+)*)\.)(\p{javaJavaIdentifierStart}\p{javaJavaIdentifierPart}+)""".r

  def parse(args: Array[String]): Option[Conf] =
    parser.parse(args, Conf(Map.empty, Paths.get(".")))

  def parser: OptionParser[Conf] = new scopt.OptionParser[Conf]("codegen") {
    head("codegen", BuildInfo.Version)
    note("Code generator for the Daml ledger bindings.\n")

    arg[(Path, Option[String])]("<DAR-file[=package-prefix]>...")(
      optTupleRead(readPath, Read.stringRead)
    )
      .unbounded()
      .action((p, c) => c.copy(darFiles = c.darFiles + p))
      .required()
      .text(
        "DAR file to use as input of the codegen with an optional, but recommend, package prefix for the generated sources."
      )

    opt[Path]('o', "output-directory")(readPath)
      .action((p, c) => c.copy(outputDirectory = p))
      .required()
      .text("Output directory for the generated sources")

    opt[(String, String)]('d', "decoderClass")(readClassName)
      .action((className, c) => c.copy(decoderPkgAndClass = Some(className)))
      .text("Fully Qualified Class Name of the optional Decoder utility")

    opt[Level]('V', "verbosity")(readVerbosity)
      .action((l, c) => c.copy(verbosity = l))
      .text("Verbosity between 0 (only show errors) and 4 (show all messages) -- defaults to 0")

    opt[String]('r', "root")(Read.stringRead)
      .unbounded()
      .action((rexp, c) => c.copy(roots = rexp :: c.roots))
      .text(
        "Regular expression for fully-qualified names of templates to generate -- defaults to .*"
      )

    help("help").text("This help text")

  }

  private[conf] val readPath: scopt.Read[Path] = scopt.Read.stringRead.map(s => Paths.get(s))

  val readClassName: scopt.Read[(String, String)] = scopt.Read.stringRead.map {
    case PackageAndClassRegex(p, c) => (p, c)
    case _ =>
      throw new IllegalArgumentException("Expected a Full Qualified Class Name")
  }

  val readVerbosity: scopt.Read[Level] = scopt.Read.stringRead.map {
    case "0" => Level.ERROR
    case "1" => Level.WARN
    case "2" => Level.INFO
    case "3" => Level.DEBUG
    case "4" => Level.TRACE
    case _ =>
      throw new IllegalArgumentException(
        "Expected a verbosity value between 0 (least verbose) and 4 (most verbose)"
      )
  }

  private[conf] def optTupleRead[A: Read, B: Read]: Read[(A, Option[B])] =
    new Read[(A, Option[B])] {
      override def arity: Int = 2

      override def reads: String => (A, Option[B]) = { s: String =>
        s.split('=').toList match {
          case Nil =>
            throw new IllegalArgumentException("Expected a key with an optional value: key[=value]")
          case key :: Nil => (implicitly[Read[A]].reads(key), None)
          case key :: value :: Nil =>
            (implicitly[Read[A]].reads(key), Some(implicitly[Read[B]].reads(value)))
          case _ =>
            throw new IllegalArgumentException("Expected a key with an optional value: key[=value]")
        }
      }
    }

}
