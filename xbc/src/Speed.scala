// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

object Speed extends App {

  sealed trait Conf
  object Conf {
    final case class FixedN(group: String, version: String, n: Long) extends Conf
    final case class IncreasingN(group: String, version: String) extends Conf
    final case class WholeGroup(group: String) extends Conf
    final case class PlayGenBytecode() extends Conf
    final case class TryCompileToBytecode() extends Conf
  }

  def conf: Conf = {
    def xv(v: String) = v match {
      case "hs" => "byhand-scala"
      case "hj" => "byhand-java-v2"
      case "i" => "interpreter"
      case "iv" => "interpreter-boxed-value"
      case "ik" => "interpreter-cps"
      case "c" => "compiled-via-bytecode"
      case s => s
    }
    //val defaultConf = Conf.WholeGroup("nfib")
    val defaultConf = Conf.TryCompileToBytecode()
    args.toList match {
      case Nil =>
        defaultConf
      case "play" :: Nil =>
        Conf.PlayGenBytecode()
      case "compile" :: Nil =>
        Conf.TryCompileToBytecode()
      case group :: Nil =>
        Conf.WholeGroup(group)
      case group :: version :: Nil =>
        Conf.IncreasingN(group, xv(version))
      case group :: version :: n :: Nil =>
        Conf.FixedN(group, xv(version), n.toLong)
      case _ =>
        sys.error(s"\n**unexpected command line: $args")
    }
  }

  println(s"Running: $conf...");

  type FUT = (Long => Long) // function under test

  // Examples should be scalable:
  // - the computational effort should scale exponentially with the input N
  // - the result should be indicative of the computation effort

  val nfibVersions: List[(String, FUT)] = {

    val interpreter: FUT = (x: Long) => {
      def prog = Lang.Examples.nfibProgram(x)
      Interpret.standard(prog)
    }
    val interpreter_boxed_value: FUT = (x: Long) => {
      def prog = Lang.Examples.nfibProgram(x)
      val box = InterpretB.run(prog)
      BoxedValue.getNumber(box) match {
        case None => sys.error(s"\n**nfib_ib, expected number, got: box")
        case Some(x) => x
      }
    }
    val interpreter_cps: FUT = (x: Long) => {
      def prog = Lang.Examples.nfibProgram(x)
      InterpretK.cps(prog)
    }

    val scala: FUT = ByHandScala.nfib(_)
    val java1: FUT = ByHandJava.nfib_v1(_)
    val java2: FUT = ByHandJava.nfib_v2(_)

    val compiled1: FUT = (x: Long) => {
      def prog = Lang.Examples.nfibProgram(x)
      //println("compile...")
      val bc: ByteCode = Compiler.compile(prog)
      //println("dump...")
      bc.dump() //find in /tmp
      //println("run...")
      bc.run0()
    }
    val _ = compiled1

    List(
      //NICK: add baseline for speedy on same example
      "interpreter-cps" -> interpreter_cps,
      "interpreter-boxed-value" -> interpreter_boxed_value,
      "interpreter" -> interpreter,
      "byhand-scala" -> scala,
      "byhand-java-v1" -> java1,
      "byhand-java-v2" -> java2,
      "compiled-via-bytecode" -> compiled1,
    )
  }

  val tripVersions: List[(String, FUT)] = {
    val interpreter: FUT = (x: Long) => {
      val y = lpower(2, x)
      def prog = Lang.Examples.tripProgram(y)
      Interpret.standard(prog) / 3L
    }
    val interpreter_cps: FUT = (x: Long) => {
      val y = lpower(2, x)
      def prog = Lang.Examples.tripProgram(y)
      InterpretK.cps(prog) / 3L
    }
    val scala: FUT = (x: Long) => {
      val y = lpower(2, x) // make it scalable
      ByHandScala.trip(y) / 3L
    }
    List(
      "interpreter-cps" -> interpreter_cps,
      "interpreter" -> interpreter,
      "byhand-scala" -> scala,
    )
  }

  // map of groups of FUTs
  def m: Map[String, (Option[Double], List[(String, FUT)])] = Map(
    "nfib" -> (None, nfibVersions),
    "trip" -> (None, tripVersions),
  )

  def getVersions(group: String): (Option[Double], List[String]) = {
    m.get(group) match {
      case None => sys.error(s"\n**unknown group: $group")
      case Some((optBaseline, g)) => (optBaseline, (g.map(_._1)))
    }
  }

  def getFUT(group: String, version: String): FUT = {
    m.get(group) match {
      case None => sys.error(s"\n**unknown group: $group")
      case Some((_, g)) =>
        g.toMap.get(version) match {
          case None => sys.error(s"\n**unknown version: $version")
          case Some(f) => f
        }
    }
  }

  conf match {

    case Conf.PlayGenBytecode() =>
      val code1 = Play.makeCodeToPrintMessage("Hello, world!")
      code1.dump()
      List((55L, 13L), (13L, 55L)).foreach { case (a, b) =>
        val res: Long = code1.run2(a, b)
        println(s"PlayGenBytecode($a,$b) -> $res")
      }

    case Conf.TryCompileToBytecode() =>
      import Lang._
      val prog =
        Program(
          defs = Map(
            ("square", 1) -> Mul(Arg(0), Arg(0)),
            ("foo", 2) -> Sub(
              FnCall("square", List(Arg(0))),
              FnCall("square", List(Arg(1))),
            ),
          ),
          main = FnCall("foo", List(Num(10L), Num(7L))),
        )
      println(s"TryCompileToBytecode: $prog")
      val res1 = Interpret.standard(prog)
      println(s"eval via Interpreter --> $res1")

      val bc: ByteCode = Compiler.compile(prog)
      bc.dump() //find in /tmp
      val res2 = bc.run0()

      println(s"eval via Compiled bytecode --> $res2")
      if (res1 != res2) {
        sys.error("\n**interpret VS bytecode: result differ")
      }

    case Conf.FixedN(group, version, n) =>
      printHeader()
      val setup = Setup(group, version, n)
      while (true) {
        val outcome = runTest(setup)
        printOutcomeLine(outcome, 1.0)
      }

    case Conf.IncreasingN(group, version) =>
      printHeader()
      var n: Long = 15L
      while (true) {
        val setup = Setup(group, version, n)
        val outcome = runTest(setup)
        printOutcomeLine(outcome, 1.0)
        n += 1
      }

    case Conf.WholeGroup(group) =>
      printHeader()

      def runVersion(version: String): Outcome = {
        def loop(n: Long): Outcome = {
          val setup = Setup(group, version, n)
          val outcome = runTest(setup)
          if (outcome.dur_s > 0.3) {
            outcome
          } else {
            loop(n + 1)
          }
        }
        loop(20L)
      }

      val (optBaseline, versions) = getVersions(group)
      versions match {
        case Nil =>
          sys.error(s"\n**no versions for group: $group")
        case leadVersion :: versions =>
          val leadOutcome = runVersion(leadVersion)
          val baselineSpeed = optBaseline match {
            case None => leadOutcome.speed
            case Some(baseline) => baseline
          }
          val leadRelative = leadOutcome.speed / baselineSpeed
          printOutcomeLine(leadOutcome, leadRelative)

          versions.foreach { version =>
            val outcome = runVersion(version)
            val relative = outcome.speed / baselineSpeed
            printOutcomeLine(outcome, relative)
          }
      }
  }

  case class Setup(group: String, version: String, n: Long)

  case class Outcome(setup: Setup, res: Long, dur_s: Double, speed: Double)

  def runTest(setup: Setup): Outcome = {
    setup match {
      case Setup(group, version, n) =>
        val functionUnderTest = getFUT(group, version)
        val start = System.currentTimeMillis()
        val res = functionUnderTest(n)
        val end = System.currentTimeMillis()
        val dur_ms = end - start
        val dur_s = dur_ms.toFloat / 1000.0
        val speed = res / (1000 * dur_ms).toDouble
        Outcome(setup, res, dur_s, speed)
    }
  }

  def printHeader() = {
    println(s"size result   duration  speed     relative  name")
    println(s"N   #ops         secs   ops/us    speedup   group:version")
    println(s"----------------------------------------------------------")
  }

  def printOutcomeLine(outcome: Outcome, relative0: Double) = {

    def pad(s: String, max: Int): String = {
      val ss = s.size
      val n = if (ss < max) max - ss else 0
      val padding: String = List.fill(n)(' ').mkString
      s + padding
    }

    outcome match {
      case Outcome(setup, res0, dur0, speed0) =>
        setup match {
          case Setup(group, version, n0) =>
            val n = pad(s"$n0", 3)
            val res = pad(s"$res0", 12)
            val dur = pad(f"$dur0%.2f", 6)
            val speed = pad(f"$speed0%.2f", 9)
            val relative = pad(f"$relative0%.2f", 9)
            val gv = s"$group:$version"
            println(s"$n $res $dur $speed $relative $gv")
        }
    }
  }

  def lpower(base: Long, exponent: Long): Long = {
    if (exponent < 0) {
      sys.error(s"\n**lpower, negative exponent: $exponent")
    } else {
      def powerLoop(i: Long): Long = if (i == 0) 1 else base * powerLoop(i - 1)
      powerLoop(exponent)
    }
  }

}
