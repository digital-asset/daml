// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.bazeltools.BazelRunfiles.{rlocation}
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.data.Ref.{DefinitionRef, Identifier, QualifiedName}
import com.daml.lf.data.Time
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.Speedy._

import java.io.File

// Explore the execution of speedy machine on small examples taken from a dar file

object ExploreDar extends App {
  PlaySpeedy.main(args.toList)
}

object PlaySpeedy {

  def usage(): Unit = {
    println("""
     |usage: explore-dar [NAME] [--arg INT]
    """.stripMargin)
  }

  def parseArgs(args0: List[String]): Config = {
    var funcName: String = "triangle"
    var argValue: Long = 10
    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case "--arg" :: x :: args =>
        argValue = x.toLong
        loop(args)
      case x :: args =>
        funcName = x
        loop(args)
    }
    loop(args0)
    Config(funcName, argValue)
  }

  final case class Config(
      funcName: String,
      argValue: Long,
  )

  def main(args0: List[String]) = {

    println("Start...")
    val base = "Examples"
    val config = parseArgs(args0)
    val dar = s"daml-lf/interpreter/perf/${base}.dar"
    val darFile = new File(rlocation(dar))

    println("Loading dar...")
    val packages = UniversalArchiveReader().readFile(darFile).get
    val packagesMap =
      packages.all.map {
        case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
      }.toMap

    println("Compiling packages...")
    val compiledPackages: CompiledPackages = PureCompiledPackages(packagesMap).right.get

    val machine: Machine = {
      println(s"Setup machine for: ${config.funcName}(${config.argValue})")
      val expr = {
        val ref: DefinitionRef =
          Identifier(
            packages.main._1,
            QualifiedName.assertFromString(s"${base}:${config.funcName}"))
        val func = SEVal(LfDefRef(ref))
        val arg = SEValue(SInt64(config.argValue))
        SEApp(func, Array(arg))
      }
      Machine.fromSExpr(
        expr,
        compiledPackages,
        Time.Timestamp.now(),
        InitialSeeding.TransactionSeed(txSeed),
        Set.empty,
      )
    }

    val result: SValue = {
      println("Run...")
      machine.run() match {
        case SResultFinalValue(value) => value
        case res => throw new MachineProblem(s"Unexpected result from machine $res")
      }
    }

    println(s"Final-value: $result")
  }

  private val txSeed = crypto.Hash.hashPrivateKey("ExploreDar")

  final case class MachineProblem(s: String) extends RuntimeException(s, null, false, false)

}
