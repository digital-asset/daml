-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Desugar (desugar) where

import DA.Daml.Compiler.Output (diagnosticsLogger)
import DA.Daml.Options.Types (Options, getLogger)
import Data.Text (Text)
import Development.IDE.Core.API (runActionSync, setFilesOfInterest)
import Development.IDE.Core.IdeState.Daml (withDamlIdeState)
import Development.IDE.Core.Rules (GetParsedModule (..))
import Development.IDE.Core.RuleTypes (GhcSession (..))
import Development.IDE.Core.Shake (use_)
import Development.IDE.GHC.Util (hscEnv)
import Development.IDE.Types.Location (toNormalizedFilePath')

import qualified Data.HashSet as HashSet
import qualified Data.Text as T

import "ghc-lib" GHC (pm_parsed_source)
import "ghc-lib-parser" HscTypes (hsc_dflags)

import qualified "ghc-lib-parser" Outputable as GHC

desugar :: Options -> FilePath -> IO Text
desugar opts inputFile = do
  loggerH <- getLogger opts "daml-desugar"
  inputFile <- pure $ toNormalizedFilePath' inputFile
  withDamlIdeState opts loggerH diagnosticsLogger $ \ide -> do
    setFilesOfInterest ide (HashSet.singleton inputFile)
    runActionSync ide $ do
      dflags <- hsc_dflags . hscEnv <$> use_ GhcSession inputFile
      parsed <- pm_parsed_source <$> use_ GetParsedModule inputFile
      pure . ensureNewlineAtEof . T.pack . GHC.showSDoc dflags . GHC.ppr $ parsed

ensureNewlineAtEof :: Text -> Text
ensureNewlineAtEof t
  | Just (_, '\n') <- T.unsnoc t = t
  | otherwise                    = t <> "\n"
