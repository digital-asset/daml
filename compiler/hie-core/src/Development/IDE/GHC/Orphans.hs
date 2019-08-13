-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Orphan instances for GHC.
--   Note that the 'NFData' instances may not be law abiding.
module Development.IDE.GHC.Orphans() where

import GHC
import GhcPlugins
import Development.IDE.GHC.Compat
import qualified StringBuffer as SB
import Control.DeepSeq
import Data.Hashable
import Development.IDE.GHC.Util


-- Orphan instances for types from the GHC API.
instance Show CoreModule where show = prettyPrint
instance NFData CoreModule where rnf = rwhnf


instance Show InstalledUnitId where
    show = installedUnitIdString

instance NFData InstalledUnitId where rnf = rwhnf

instance NFData SB.StringBuffer where rnf = rwhnf

instance Show Module where
    show = moduleNameString . moduleName

instance Show (GenLocated SrcSpan ModuleName) where show = prettyPrint

instance NFData (GenLocated SrcSpan ModuleName) where
    rnf = rwhnf

instance Show ModSummary where
    show = show . ms_mod

instance Show ParsedModule where
    show = show . pm_mod_summary

instance NFData ModSummary where
    rnf = rwhnf

instance Show HscEnv where
    show _ = "HscEnv"

instance NFData HscEnv where
    rnf = rwhnf

instance NFData ParsedModule where
    rnf = rwhnf

instance Hashable InstalledUnitId where
  hashWithSalt salt = hashWithSalt salt . installedUnitIdString

instance Show HieFile where
    show = show . hie_module

instance NFData HieFile where
    rnf = rwhnf
