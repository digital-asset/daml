-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

import Data.Set (Set)


-- DATATYPES-START
data Action c p =
    Create c
  | Exercise {
      actors :: Set p,
      contract :: c,
      kind :: Kind,
      consequences :: Transaction c p
    }

type Transaction c p = [Action c p]

data Kind = Consuming | NonConsuming
-- DATATYPES-END

-- COMMIT-LEDGER-START
type Commit c p = (Set p, Transaction c p)
type Ledger c p = [Commit c p]
-- COMMIT-LEDGER-END

