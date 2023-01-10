// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package explore

import com.daml.lf.language.PackageInterface
import com.daml.lf.speedy.SExpr0._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.Speedy._
import com.daml.logging.LoggingContext

// Explore the execution of speedy machine on small examples.

object Explore extends App {
  PlaySpeedy.main(args.toList)
}

object PlaySpeedy {

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private[this] val compilerConfig =
    Compiler.Config.Default.copy(stacktracing = Compiler.FullStackTrace)

  def main(args0: List[String]) = {
    val config: Config = parseArgs(args0)
    val compiler: Compiler = new Compiler(PackageInterface.Empty, compilerConfig)

    val names: List[String] = config.names match {
      case Nil => examples.toList.map(_._1)
      case xs => xs
    }

    names.foreach { name =>
      val (expected, expr) = examples(name)
      val converted = compiler.unsafeClosureConvert(expr)
      val machine = Machine.fromPureSExpr(PureCompiledPackages.Empty, converted)
      runMachine(machine, expected)
    }
  }

  final case class Config(
      names: List[String]
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

  def runMachine(machine: PureMachine, expected: Int): Unit = {
    machine.runPure().toTry.get match {
      case SInt64(got) =>
        if (got != expected) {
          throw MachineProblem(s"Expected final integer to be $expected, but got $got")
        }
      case _ =>
        throw MachineProblem(s"Expected final-value to be an integer")
    }
  }

  final case class MachineProblem(s: String) extends RuntimeException(s, null, false, false)

  def examples: Map[String, (Int, SExpr)] = {

    def num(n: Long): SExpr = SEValue(SInt64(n))
    def mkVar(level: Int) = SEVarLevel(level)

    // The trailing numeral is the number of args at the scala mkVar

    def decrement1(x: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), List(x, SEValue(SInt64(1))))
    val decrement = SEAbs(1, decrement1(mkVar(0)))

    def subtract2(x: SExpr, y: SExpr): SExpr = SEApp(SEBuiltin(SBSubInt64), List(x, y))
    val subtract = SEAbs(2, subtract2(mkVar(0), mkVar(1)))

    def twice2(f: SExpr, x: SExpr): SExpr = SEApp(f, List(SEApp(f, List(x))))
    val twice = SEAbs(2, twice2(mkVar(3), mkVar(4)))

    def thrice2(f: SExpr, x: SExpr): SExpr = SEApp(f, List(SEApp(f, List(SEApp(f, List(x))))))
    val thrice = SEAbs(2, thrice2(mkVar(0), mkVar(1)))

    val examples = List(
      (
        "sub", // 11-33
        -22,
        subtract2(num(11), num(33)),
      ),
      (
        "sub/sub", // (1-3)-(5-10)
        3,
        subtract2(subtract2(num(1), num(3)), subtract2(num(5), num(10))),
      ),
      (
        "subF", // 88-55
        33,
        SEApp(subtract, List(num(88), num(55))),
      ),
      (
        "thrice", // thrice (\x -> x - 1) 0
        -3,
        SEApp(thrice, List(decrement, num(0))),
      ),
      (
        "thrice-thrice", // thrice thrice (\x -> x - 1) 0
        -27,
        SEApp(thrice, List(thrice, decrement, num(0))),
      ),
      (
        "free", // let (a,b,c) = (30,100,21) in twice (\x -> x - (a-c)) b
        82,
        SELet(
          List(num(30)),
          SELet(
            List(num(100)),
            SELet(
              List(num(21)),
              SEApp(
                twice,
                List(
                  SEAbs(1, subtract2(mkVar(3), subtract2(mkVar(0), mkVar(2)))),
                  mkVar(1),
                ),
              ),
            ), // 100
          ),
        ),
      ),
    )

    val res = examples.map { case (k, x, e) => (k, (x, e)) }.toMap

    res
  }

}
