package com.daml.ledger.api.rewrite.testtool

import com.daml.ledger.api.rewrite.testtool.infrastructure.{LedgerSession, LedgerTestSuite}

package object tests {

  val all: Map[String, LedgerSession => LedgerTestSuite] = Map(
    "DivulgenceIT" -> (new Divulgence(_)),
    "Identity" -> (new Identity(_))
  )

}
