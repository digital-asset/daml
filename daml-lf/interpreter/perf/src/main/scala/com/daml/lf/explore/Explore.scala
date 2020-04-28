// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.lf.data._
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.Speedy._

// Explore the execution of speedy machine on small examples.
// Count & classify steps
// Show lightweight trace with: --trace

object Explore extends App {
  PlaySpeedy.main(args.toList)
}

object PlaySpeedy {

  def main(args0: List[String]) = {
    val config: Config = parseArgs(args0)
    val compiler: Compiler = Compiler(Map.empty)

    val e: SExpr = compiler.unsafeCompile(examples(config.exampleName))
    val m: Machine = makeMachine(e)
    runMachine(config, m)
  }

  final case class Config(
      exampleName: String,
  )

  def usage(): Unit = {
    println("""
     |usage: explore [EXAMPLE-NAME]
    """.stripMargin)
  }

  def parseArgs(args0: List[String]): Config = {

    var exampleName: String = "thrice-thrice"

    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case name :: args =>
        exampleName = name
        loop(args)
    }
    loop(args0)
    Config(exampleName)
  }

  private val txSeed = crypto.Hash.hashPrivateKey("SpeedyExplore")

  def makeMachine(sexpr: SExpr): Machine = {
    val compiledPackages: CompiledPackages = PureCompiledPackages(Map.empty).right.get
    val compiler = Compiler(compiledPackages.packages)
    Machine.fromSExpr(
      sexpr,
      compiledPackages,
      Time.Timestamp.now(),
      InitialSeeding(Some(txSeed)),
      Set.empty,
    )
  }

  def runMachine(config: Config, machine: Machine): Unit = {

    println(s"example name: ${config.exampleName}")

    machine.run() match {
      case SResultFinalValue(value) => {
        println(s"final-value: $value")
      }
      case res => throw new MachineProblem(s"Unexpected result from machine $res")
    }
  }

  final case class MachineProblem(s: String) extends RuntimeException(s, null, false, false)

  def examples: Map[String, SExpr] = {

    def num(n: Long): SExpr = SEValue(SInt64(n))

    // The trailing numeral is the number of args at the scala level

    def decrement1(x: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), Array(x, SEValue(SInt64(1))))
    val decrement = SEAbs(1, decrement1(SEVar(1)))

    def subtract2(x: SExpr, y: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), Array(x, y))
    val subtract = SEAbs(2, subtract2(SEVar(2), SEVar(1)))

    def twice2(f: SExpr, x: SExpr): SExpr = SEApp(f, Array(SEApp(f, Array(x))))
    val twice = SEAbs(2, twice2(SEVar(2), SEVar(1)))

    def thrice2(f: SExpr, x: SExpr): SExpr = SEApp(f, Array(SEApp(f, Array(SEApp(f, Array(x))))))
    val thrice = SEAbs(2, thrice2(SEVar(2), SEVar(1)))

    Map(
      "sub" -> subtract2(num(11), num(33)),
      "sub/sub" -> subtract2(subtract2(num(1), num(3)), subtract2(num(5), num(10))),
      "subF" -> SEApp(subtract, Array(num(88), num(55))),
      "thrice" -> SEApp(thrice, Array(decrement, num(0))),
      "thrice-thrice" -> SEApp(thrice, Array(thrice, decrement, num(0))),
    )
  }

}
