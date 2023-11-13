-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | For compiler level warnings on packages that aren't included as standard. This includes, at the very least, Daml.Script and Triggers
module DA.Daml.LFConversion.ExternalWarnings (topLevelWarnings) where

import "ghc-lib" GhcPlugins as GHC
import DA.Daml.LFConversion.ConvertM
import DA.Daml.LFConversion.ExternalWarnings.Script qualified as Script

topLevelWarnings :: (Var, GHC.Expr Var) -> ConvertM ()
topLevelWarnings bind = mapM_ ($ bind) 
  [ Script.topLevelWarnings
  ]
