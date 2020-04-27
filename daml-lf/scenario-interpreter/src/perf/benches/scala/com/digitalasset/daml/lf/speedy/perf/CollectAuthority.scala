// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package perf

import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.archive.{Decode, UniversalArchiveReader}
import com.daml.lf.data._
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.Pretty._
import java.io.File
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@State(Scope.Thread)
class CollectAuthorityState {
  private val darFile = new File(rlocation("daml-lf/scenario-interpreter/CollectAuthority.dar"))
  private val packages = UniversalArchiveReader().readFile(darFile).get
  private val packagesMap = Map(packages.all.map {
    case (pkgId, pkgArchive) => Decode.readArchivePayloadAndVersion(pkgId, pkgArchive)._1
  }: _*)

  // NOTE(MH): We use a static seed to get reproducible runs.
  private val seeding = crypto.Hash.secureRandom(crypto.Hash.hashPrivateKey("scenario-perf"))
  private val buildMachine = Speedy.Machine
    .newBuilder(
      PureCompiledPackages(packagesMap).right.get,
      Time.Timestamp.MinValue,
      Some(seeding()))
    .fold(err => sys.error(err.toString), identity)
  private val expr = EVal(
    Identifier(packages.main._1, QualifiedName.assertFromString("CollectAuthority:test")))
  // NOTE(MH): We run the machine once to initialize all data that is shared
  // between runs.
  run()

  def run(): Int = {
    val machine = buildMachine(expr)
    ScenarioRunner(machine).run() match {
      case Left((err, _)) => sys.error(prettyError(err, machine.ptx).render(80))
      case Right((_, steps, _)) => steps
    }
  }
}

class CollectAuthority {
  @Benchmark @BenchmarkMode(Array(Mode.AverageTime)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def bench(state: CollectAuthorityState): Int = {
    state.run()
  }
}
