// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package perf

import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.data.Ref.{Identifier, Location, Party, QualifiedName}
import com.daml.lf.data.Time
import com.daml.lf.language.Ast.EVal
import com.daml.lf.speedy.SResult._
import com.daml.lf.transaction.{NodeId, GlobalKey, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.lf.scenario.{ScenarioLedger, ScenarioRunner}
import com.daml.lf.speedy.Speedy.Machine
import java.io.File
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

class CollectAuthority {
  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(state: CollectAuthorityState): Unit = {
    state.run()
  }
}

@State(Scope.Benchmark)
class CollectAuthorityState {

  @Param(Array("//daml-lf/scenario-interpreter/CollectAuthority.dar"))
  private[perf] var dar: String = _
  @Param(Array("CollectAuthority:test"))
  private[perf] var scenario: String = _

  var machine: Machine = null
  var the_sexpr: SExpr = null

  @Setup(Level.Trial)
  def init(): Unit = {
    val darFile = new File(if (dar.startsWith("//")) rlocation(dar.substring(2)) else dar)
    val packages = UniversalArchiveDecoder.assertReadFile(darFile)

    val compilerConfig =
      Compiler.Config.Default.copy(
        stacktracing = Compiler.NoStackTrace
      )

    val compiledPackages = PureCompiledPackages.assertBuild(packages.all.toMap, compilerConfig)
    val expr = EVal(Identifier(packages.main._1, QualifiedName.assertFromString(scenario)))

    machine = Machine.fromScenarioExpr(
      compiledPackages,
      expr,
    )
    the_sexpr = machine.ctrl

    // fill the caches!
    setup()
  }

  // Caches for Party creation & Ledger interaction performed during the setup run.
  // The maps are indexed by step number.
  private var cachedParty: Map[Int, Party] = Map()
  private var cachedCommit: Map[Int, SValue] = Map()
  private var cachedContract: Map[Int, ContractInst[Value.VersionedValue]] = Map()

  // This is function that we benchmark
  def run(): Unit = {
    machine.setExpressionToEvaluate(the_sexpr)
    var step = 0
    var finalValue: SValue = null
    while (finalValue == null) {
      step += 1
      machine.run() match {
        case SResultScenarioGetParty(_, callback) => callback(cachedParty(step))
        case SResultScenarioSubmit(committers, commands, location, mustFail, callback) =>
          assert(!mustFail)
          val api = new CannedLedgerApi(step, cachedContract)
          ScenarioRunner.submit(
            machine.compiledPackages,
            api,
            committers,
            Set.empty,
            SExpr.SEValue(commands),
            location,
            crypto.Hash.hashPrivateKey(step.toString),
          ) match {
            case ScenarioRunner.Commit(_, value, _) =>
              callback(value)
            case ScenarioRunner.SubmissionError(err, _) => crash(s"Submission failed $err")
          }
        case SResultNeedContract(_, _, _, _) =>
          crash("Off-ledger need contract callback")
        case SResultFinalValue(v) => finalValue = v
        case r => crash(s"bench run: unexpected result from speedy: ${r}")
      }
    }
  }

  // This is the initial setup run (not benchmarked), where we cache the results of
  // interacting with the ledger, so they can be reused during the benchmark runs.

  def setup(): Unit = {
    var ledger: ScenarioLedger = ScenarioLedger.initialLedger(Time.Timestamp.Epoch)
    var step = 0
    var finalValue: SValue = null
    while (finalValue == null) {
      step += 1
      machine.run() match {
        case SResultScenarioGetParty(partyText, callback) =>
          Party.fromString(partyText) match {
            case Right(res) =>
              cachedParty = cachedParty + (step -> res)
              callback(res)
            case Left(msg) =>
              crash(s"Party.fromString failed: $msg")
          }
        case SResultScenarioSubmit(committers, commands, location, mustFail, callback) =>
          assert(!mustFail)
          val api = new CachedLedgerApi(step, ledger)
          ScenarioRunner.submit(
            machine.compiledPackages,
            api,
            committers,
            Set.empty,
            SExpr.SEValue(commands),
            location,
            crypto.Hash.hashPrivateKey(step.toString),
          ) match {
            case ScenarioRunner.SubmissionError(err, _) => crash(s"Submission failed $err")
            case ScenarioRunner.Commit(result, value, _) =>
              ledger = result.newLedger
              cachedCommit = cachedCommit + (step -> value)
              callback(value)
              cachedContract ++= api.cachedContract
              step = api.step
          }
        case SResultFinalValue(v) =>
          finalValue = v
        case r =>
          crash(s"setup run: unexpected result from speedy: ${r}")
      }
    }
  }

  def crash(reason: String) = {
    System.err.println("Benchmark failed: " + reason)
    System.exit(1)
  }

}

class CachedLedgerApi(initStep: Int, ledger: ScenarioLedger)
    extends ScenarioRunner.ScenarioLedgerApi(ledger) {
  var step = initStep
  var cachedContract: Map[Int, ContractInst[Value.VersionedValue]] = Map()
  override def lookupContract(
      coid: ContractId,
      actAs: Set[Party],
      readAs: Set[Party],
      callback: ContractInst[Value.VersionedValue] => Unit,
  ): Either[scenario.Error, Unit] = {
    step += 1
    super.lookupContract(
      coid,
      actAs,
      readAs,
      { coinst => cachedContract += step -> coinst; callback(coinst) },
    )
  }
}

class CannedLedgerApi(
    initStep: Int,
    cachedContract: Map[Int, ContractInst[Value.VersionedValue]],
) extends ScenarioRunner.LedgerApi[Unit] {
  var step = initStep
  override def lookupContract(
      coid: ContractId,
      actAs: Set[Party],
      readAs: Set[Party],
      callback: ContractInst[Value.VersionedValue] => Unit,
  ): Either[scenario.Error, Unit] = {
    step += 1
    val coinst = cachedContract(step)
    Right(callback(coinst))
  }
  override def lookupKey(
      machine: Machine,
      gk: GlobalKey,
      actAs: Set[Party],
      readAs: Set[Party],
      callback: Option[ContractId] => Boolean,
  ) =
    throw new RuntimeException("Keys are not supported in the benchmark")
  override def currentTime = throw new RuntimeException("getTime is not supported in the benchmark")

  override def commit(
      committers: Set[Party],
      readAs: Set[Party],
      location: Option[Location],
      tx: SubmittedTransaction,
      locationInfo: Map[NodeId, Location],
  ) = Right(())
}
