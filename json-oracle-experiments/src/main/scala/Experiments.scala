// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import cats.effect._
import cats.implicits._
import com.daml.testing.oracle.OracleAround
import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts

object Experiments extends OracleAround {
  def main(args: Array[String]): Unit = {
    connectToOracle()
    val user = createNewRandomUser()
    println(user)
    implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContexts.synchronous)
    val xa = Transactor.fromDriverManager[IO](
      "oracle.jdbc.OracleDriver",
      oracleJdbcUrl,
      user.name,
      user.pwd,
      Blocker.liftExecutionContext(ExecutionContexts.synchronous),
    )
    sql"""
      CREATE TABLE contract (contract_id VARCHAR(255) NOT NULL CONSTRAINT contract_k PRIMARY KEY, tpid NUMBER(19,0), payload CLOB NOT NULL CONSTRAINT ensure_json_payload CHECK (payload IS JSON), signatories CLOB NOT NULL CONSTRAINT ensure_json_signatories CHECK (signatories IS JSON), observers CLOB NOT NULL CONSTRAINT ensure_json_observers CHECK (observers IS JSON))
    """.update.run.transact(xa).unsafeRunSync()
    sql"CREATE MATERIALIZED VIEW LOG ON contract".update.run.transact(xa).unsafeRunSync()
    sql"CREATE INDEX contract_tpid_idx ON contract (tpid)".update.run.transact(xa).unsafeRunSync()
    sql"""
      CREATE MATERIALIZED VIEW stakeholders BUILD IMMEDIATE REFRESH FAST ON COMMIT AS
      SELECT contract_id, stakeholder, tpid FROM contract,
                 json_table(json_array(signatories, observers), '$$[*][*]'
                    columns (stakeholder VARCHAR(255) path '$$'))
    """.update.run.transact(xa).unsafeRunSync()

    sql"""
      CREATE INDEX stakeholder_idx on stakeholders (stakeholder, tpid)

    """.update.run.transact(xa).unsafeRunSync()

    val params =
      for {
        party <- (0 until 20).map(i => s"p$i")
        tpid <- 0 until 20
      } yield (party, tpid)
    params.zipWithIndex.foreach { case ((party, tpid), i) =>
      // println(i)
      val n = 100
      val contracts = (0 until n).map { j =>
        (s"#${i * n + j}", tpid, "{\"x\": 0}", s"""["$party"]""", "[]")
      }.toList
      val start = System.nanoTime
      Update[(String, Int, String, String, String)](
        """
        INSERT INTO CONTRACT (contract_id, tpid, payload, signatories, observers)
        VALUES (?, ?, ?, ?, ?)
    """
      ).updateMany(contracts).transact(xa).unsafeRunSync()
      val end_ = System.nanoTime
      println(s"$i\t${end_ - start}")
    }
    ()
  }
}
