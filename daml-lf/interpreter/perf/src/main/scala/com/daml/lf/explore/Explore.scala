// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SResult._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.Speedy._

// Explore the execution of speedy machine on small examples.

object Explore extends App {
  PlaySpeedy.main(args.toList)
}

object PlaySpeedy {

  def main(args0: List[String]) = {
    val config: Config = parseArgs(args0)
    val compiler: Compiler = Compiler(Map.empty, Compiler.FullStackTrace, Compiler.NoProfile)

    val names: List[String] = config.names match {
      case Nil => examples.toList.map(_._1)
      case xs => xs
    }

    names.foreach { name =>
      val (expected, expr) = examples(name)
      val anf = compiler.unsafeClosureConvert(expr)
      val machine = Speedy.Machine.fromPureAExpr(noPackages, anf)
      runMachine(name, machine, expected)
    }
  }

  final case class Config(
      names: List[String],
  )

  def usage(): Unit = {
    println("""
     |usage: explore [EXAMPLES]
     |default: run all known examples
    """.stripMargin)
  }

  def parseArgs(args0: List[String]): Config = {
    var names: List[String] = Nil
    def loop(args: List[String]): Unit = args match {
      case Nil => {}
      case "-h" :: _ => usage()
      case "--help" :: _ => usage()
      case name :: args =>
        names = names ++ List(name)
        loop(args)
    }
    loop(args0)
    Config(names)
  }

  private val noPackages =
    data.assertRight(PureCompiledPackages(Map.empty, Compiler.NoStackTrace, Compiler.NoProfile))

  def runMachine(name: String, machine: Machine, expected: Int): Unit = {

    println(s"example name: $name")

    machine.run() match {
      case SResultFinalValue(value) =>
        println(s"final-value: $value")
        value match {
          case SInt64(got) =>
            if (got != expected) {
              throw new MachineProblem(s"Expected final integer to be $expected, but got $got")
            }
          case _ =>
            throw new MachineProblem(s"Expected final-value to be an integer")
        }
      case res =>
        throw new MachineProblem(s"Unexpected result from machine $res")
    }
  }

  final case class MachineProblem(s: String) extends RuntimeException(s)

  def examples: Map[String, (Int, SExpr)] = {

    def num(n: Long): SExpr = SEValue(SInt64(n))

    // The trailing numeral is the number of args at the scala level

    def decrement1(x: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), Array(x, SEValue(SInt64(1))))
    val decrement = SEAbs(1, decrement1(SEVar(1)))

    def subtract2(x: SExpr, y: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), Array(x, y))
    val subtract = SEAbs(2, subtract2(SEVar(2), SEVar(1)))

    val identity = SEAbs(1, SEVar(1))

    def twice2(f: SExpr, x: SExpr): SExpr = SEApp(f, Array(SEApp(f, Array(x))))
    val twice = SEAbs(2, twice2(SEVar(2), SEVar(1)))

    def thrice2(f: SExpr, x: SExpr): SExpr = SEApp(f, Array(SEApp(f, Array(SEApp(f, Array(x))))))
    val thrice = SEAbs(2, thrice2(SEVar(2), SEVar(1)))

    val examples = List(
      (
        "sub", //11-33
        -22,
        subtract2(num(11), num(33))),
      (
        "sub/sub", // (1-3)-(5-10)
        3,
        subtract2(subtract2(num(1), num(3)), subtract2(num(5), num(10)))),
      (
        "subF", //88-55
        33,
        SEApp(subtract, Array(num(88), num(55)))),
      (
        "id-id", // id id 77
        77,
        SEApp(identity, Array(identity, num(77)))),
      (
        "thrice", // thrice (\x -> x - 1) 0
        -3,
        SEApp(thrice, Array(decrement, num(0)))),
      (
        "thrice-thrice", //thrice thrice (\x -> x - 1) 0
        -27,
        SEApp(thrice, Array(thrice, decrement, num(0)))),
      (
        "free", // let (a,b,c) = (30,100,21) in twice (\x -> x - (a-c)) b
        82,
        SELet(
          Array(num(30), num(100), num(21)),
          SEApp(
            twice,
            Array(SEAbs(1, subtract2(SEVar(1), subtract2(SEVar(4), SEVar(2)))), SEVar(2)))) //100
      )
    )

    val res = examples.map { case (k, x, e) => (k, (x, e)) }.toMap

    res
  }

}
