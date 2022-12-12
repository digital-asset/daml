-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module DA.Daml.Desugar.Tests(mkTestTree) where

import Control.Monad (filterM)
import DA.Daml.Desugar (desugar)
import DA.Daml.LF.Ast.Version (versionDev)
import DA.Daml.Options.Types (EnableScenarioService(..), Options(..), defaultOptions)
import Data.List.Extra (nubOrd)
import Data.Text (Text)
import System.Directory (doesFileExist, listDirectory, makeAbsolute)
import System.FilePath (dropExtension, replaceExtensions, takeExtensions, (<.>), (</>))
import Test.Tasty.Golden (goldenVsStringDiff)

import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text.Encoding as TE
import qualified Test.Tasty.Extended as Tasty

mkTestTree :: FilePath -> IO Tasty.TestTree
mkTestTree testDir = do
  let isExpectationFile filePath =
        ".EXPECTED" == takeExtensions (dropExtension filePath)
  expectFiles <- filter isExpectationFile <$> listDirectory testDir

  let goldenSrcs = nubOrd $ map (flip replaceExtensions "daml") expectFiles
  goldenTests <- mapM (fileTest . (testDir </>))  goldenSrcs

  pure $ Tasty.testGroup "DA.Daml.Desugar" $ concat goldenTests

runDamlDesugar :: FilePath -> IO Text
runDamlDesugar input = desugar opts input
  where
    opts = (defaultOptions Nothing)
      { optScenarioService = EnableScenarioService False
      , optDamlLfVersion = versionDev
      }

-- | For the given file <name>.daml (assumed), this test checks if
-- <name>.EXPECTED.desugared-daml exists, and produces output accordingly.
fileTest :: FilePath -> IO [Tasty.TestTree]
fileTest damlFile = do

  damlFileAbs <- makeAbsolute damlFile
  let basename = dropExtension damlFileAbs
      expected = [basename <.> "EXPECTED" <.> "desugared-daml"]

  expectations <- filterM doesFileExist expected

  if null expectations
    then pure []
    else do
      desugared <- runDamlDesugar damlFile

      pure $ flip map expectations $ \expectation ->
        goldenVsStringDiff ("File: " <> expectation) diff expectation $
          pure $ BSL.fromStrict $ TE.encodeUtf8 desugared
  where
    diff ref new = [POSIX_DIFF, "--strip-trailing-cr", ref, new]
