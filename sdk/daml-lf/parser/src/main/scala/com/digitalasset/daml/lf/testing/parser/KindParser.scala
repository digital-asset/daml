// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._

private[daml] object KindParser {

  lazy val kind0: Parser[Kind] =
    `*` ^^ (_ => KStar) |
      Id("nat") ^^ (_ => KNat) |
      (`(` ~> kind <~ `)`)

  lazy val kind: Parser[Kind] = rep1sep(kind0, `->`) ^^ (_.reduceRight(KArrow))

}
