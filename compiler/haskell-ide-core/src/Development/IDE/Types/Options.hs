-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE CPP #-}

-- | Options
module Development.IDE.Types.Options
  ( CompileOpts(..)
  ) where

import Development.IDE.UtilGHC
import           GHC hiding (parseModule, typecheckModule)
import           GhcPlugins                     as GHC hiding (PackageState, fst3, (<>))


data CompileOpts = CompileOpts
  { optPreprocessor :: GHC.ParsedSource -> ([(GHC.SrcSpan, String)], GHC.ParsedSource)
  , optRunGhcSession :: forall a. Maybe ParsedModule -> PackageState -> Ghc a -> IO a
  -- ^ Setup a GHC session using a given package state. If a `ParsedModule` is supplied,
  -- the import path should be setup for that module.
  , optWriteIface :: Bool

  , optMbPackageName :: Maybe String

  , optPackageDbs :: [FilePath]
  , optHideAllPkgs :: Bool
  , optPackageImports :: [(String, ModRenaming)]

  , optThreads :: Int
  , optShakeProfiling :: Maybe FilePath
  }
