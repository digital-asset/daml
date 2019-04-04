-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Solver for constraints.
module DA.Daml.LF.Auth.Solve(
    solve
    ) where

import DA.Daml.LF.Auth.Type
import DA.Daml.LF.Auth.Environment


solve :: Environment -> [Ctx] -> Pred -> Bool
solve _ _ _ = False -- TODO Try a little harder at solving
