-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

-- | Attempt at hiding the GHC version differences we can.
module Development.IDE.Compat(
    HieFile(..),
    mkHieFile,
    writeHieFile,
    readHieFile
    ) where

#ifndef GHC_STABLE
import HieBin
import HieAst
import HieTypes
#else

import GHC
import GhcPlugins
import NameCache
import Avail
import TcRnTypes


mkHieFile :: ModSummary -> TcGblEnv -> RenamedSource -> Hsc HieFile
mkHieFile = undefined

writeHieFile :: FilePath -> HieFile -> IO ()
writeHieFile = undefined

readHieFile :: NameCache -> FilePath -> IO (HieFile, ())
readHieFile _ _ = return (HieFile () [], ())

data HieFile = HieFile {hie_module :: (), hie_exports :: [AvailInfo]}

#endif
