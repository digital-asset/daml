-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE CPP #-}

module DA.Test.DamlRenamer (main) where

import Control.Monad (filterM)
import DA.Bazel.Runfiles (exe, locateRunfiles, mainWorkspace)
import Data.List.Extra (nubOrd)
import Data.Text (Text)
import System.Directory (doesFileExist, listDirectory, makeAbsolute)
import System.Environment.Blank (setEnv)
import System.Exit (ExitCode (..))
import System.FilePath (dropExtension, replaceExtensions, takeExtensions, (<.>), (</>))
import System.Process (readProcessWithExitCode)
import Test.Tasty.Golden (goldenVsStringDiff)

import qualified Data.ByteString.Lazy as BSL
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
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
      renamed <- runDamlRename damlc damlFile

      pure $ flip map expectations $ \expectation ->
        goldenVsStringDiff ("File: " <> expectation) diff expectation $
          pure $ BSL.fromStrict $ TE.encodeUtf8 renamed
  where
    diff ref new = [POSIX_DIFF, "--strip-trailing-cr", ref, new]

-- | Shells out to @damlc@ to obtain the parsed, desugared and renamed output
--   for the given @damlFile@.
runDamlRename :: FilePath -> FilePath -> IO Text
runDamlRename damlc damlFile = do
  (exitCode, _stdout, stderr) <- readProcessWithExitCode
    damlc
    [ "compile"
    , "--ghc-option=-ddump-rn"
    , damlFile
    ]
    ""
  case exitCode of
    ExitSuccess -> pure $ extract stderr
    _ -> error $ unlines
      [ "The following command failed: "
      , "\tdamlc compile --ghc-option=-ddump-rn " <> damlFile
      , "stderr:"
      , "\t" <> stderr
      ]

  where
    -- This extracts the actual renamed code from the output,
    -- dropping the shake warning header and terminal color formatting
    extract
      = T.unlines
      . fmap (T.drop 2) -- the part we're interested in is indented with two spaces.
      . T.lines
      . fst . T.breakOn "\n  \ESC"
      . snd . T.breakOnEnd "==================== Renamer ====================\n"
      . T.pack
