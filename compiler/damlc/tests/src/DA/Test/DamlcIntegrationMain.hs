-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Test driver for Daml-GHC CompilerService.
-- For each file, compile it with GHC, convert it,
-- typecheck with LF, test it.  Test annotations are documented as 'Ann'.
module DA.Test.DamlcIntegrationMain
  ( main
  ) where

import DA.Test.DamlcIntegration qualified as Lib

main :: IO ()
main = Lib.main
