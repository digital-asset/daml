-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module DA.Test.DamlRenamer (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Monad (filterM)
import DA.Bazel.Runfiles (exe, locateRunfiles, mainWorkspace)
import Data.List.Extra (nubOrd)
import System.Directory (doesFileExist, listDirectory, makeAbsolute)
import System.Environment.Blank (setEnv)
import System.Exit (ExitCode (..))
import System.FilePath (dropExtension, replaceExtensions, takeExtensions, (<.>), (</>))
import System.IO.Extra (withTempDir)
import System.Process (readProcessWithExitCode)
import Test.Tasty.Golden (goldenVsStringDiff)

import qualified Data.ByteString.Lazy as BSL
import qualified Test.Tasty.Extended as Tasty

main :: IO ()
main = do
  setEnv "TASTY_NUM_THREADS" "1" True
  damlc <- locateRunfiles $ mainWorkspace </> "compiler" </> "damlc" </> exe "damlc"
  testDir <- locateRunfiles $ mainWorkspace </> "compiler/damlc/tests/daml-test-files"
  print (damlc, testDir)
  Tasty.deterministicMain =<< allTests damlc testDir

allTests :: FilePath -> FilePath -> IO Tasty.TestTree
allTests damlc testDir = Tasty.testGroup "All Daml GHC tests using Tasty" <$> sequence
  [ mkTestTree damlc testDir
  ]

mkTestTree :: FilePath -> FilePath -> IO Tasty.TestTree
mkTestTree damlc testDir = do
  let isExpectationFile filePath =
        ".EXPECTED" == takeExtensions (dropExtension filePath)
  expectFiles <- filter isExpectationFile <$> listDirectory testDir

  let goldenSrcs = nubOrd $ map (flip replaceExtensions "daml") expectFiles
  goldenTests <- mapM (fileTest damlc . (testDir </>)) goldenSrcs

  pure $ Tasty.testGroup "DA.Daml.Desugar" $ concat goldenTests

-- | For the given file <name>.daml (assumed), this test checks if
-- <name>.EXPECTED.renamed-daml exists, and produces output accordingly.
fileTest :: FilePath -> FilePath -> IO [Tasty.TestTree]
fileTest damlc damlFile = do

  damlFileAbs <- makeAbsolute damlFile
  let basename = dropExtension damlFileAbs
      expected = [basename <.> "EXPECTED" <.> "renamed-daml"]

  expectations <- filterM doesFileExist expected

  if null expectations
    then pure []
    else do
      pure $ flip map expectations $ \expectation ->
        goldenVsStringDiff ("File: " <> expectation) diff expectation $
          runDamlRename damlc damlFile
  where
    diff ref new = [POSIX_DIFF, "--strip-trailing-cr", ref, new]

-- | Shells out to @damlc@ to obtain the parsed, desugared and renamed output
--   for the given @damlFile@.
runDamlRename :: FilePath -> FilePath -> IO BSL.ByteString
runDamlRename damlc damlFile = do
  withTempDir $ \tempDir -> do
    (exitCode, _stdout, stderr) <- readProcessWithExitCode
      damlc
      [ "compile"
      , "--ghc-option=-ddump-to-file"
      , "--ghc-option=-dumpdir=" <> tempDir
      , "--ghc-option=-ddump-file-prefix=o."
      , "--ghc-option=-ddump-rn"
      , "--ghc-option=-dsuppress-uniques"
      , damlFile
      ]
      ""
    let dumpFile = tempDir </> "o.dump-rn"
    case exitCode of
      ExitSuccess -> BSL.readFile dumpFile
      _ -> error $ unlines
        [ "The following command failed: "
        , "\tdamlc compile --ghc-option=-ddump-rn " <> damlFile
        , "stderr:"
        , "\t" <> stderr
        ]
