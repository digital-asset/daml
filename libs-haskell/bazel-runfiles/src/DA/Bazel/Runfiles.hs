-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- There is a library in rules_haskell that does something similar
-- but since we also want support for locating runfiles in JARs
-- it is simpler to have all code located here.
module DA.Bazel.Runfiles
  ( locateRunfiles
  , locateRunfilesMb
  , mainWorkspace
  ) where

import Control.Monad.Trans.Maybe
import Data.Foldable
import Data.List
import Data.List.Split (splitOn)
import System.Directory
import System.Environment
import System.FilePath

mainWorkspace :: String
mainWorkspace = "com_github_digital_asset_daml"

locateRunfiles :: FilePath -> IO FilePath
locateRunfiles target = do
    dirOrError <- locateRunfilesMb target
    case dirOrError of
        Left e -> error e
        Right d -> pure d

locateRunfilesMb :: FilePath -> IO (Either String FilePath)
locateRunfilesMb target = do
    execPath <- getExecutablePath
    mbDir <- runMaybeT . asum . map MaybeT $
        [ do let jarResources = takeDirectory execPath </> "resources"
             hasJarResources <- doesDirectoryExist jarResources
             pure $ if hasJarResources
                 then Just jarResources
                 else Nothing
        , do let runfilesDir = execPath <> ".runfiles"
             hasTarget <- doesPathExist (runfilesDir </> target)
             hasManifest <- doesFileExist (runfilesDir </> "MANIFEST")
             if hasTarget
                then pure $ Just(runfilesDir </> target)
             else if hasManifest
                then lookupTargetInManifestFile (runfilesDir </> "MANIFEST") target
             else pure Nothing
        , do mbDir <- lookupEnv "RUNFILES_DIR"
             pure (fmap (</> target) mbDir)
        ]
    pure $ maybe (Left $ "Could not locate runfiles for target: " <> target) Right mbDir

lookupTargetInManifestFile :: FilePath -> FilePath -> IO (Maybe FilePath)
lookupTargetInManifestFile manifestPath target = do
    manifestFile <- readFile manifestPath
    let manifest = map lineToTuple (lines manifestFile)
    let targetNormalised = intercalate "/" (splitOn "\\" (normalise target))
    pure $ asum [findExact targetNormalised manifest, findDir targetNormalised manifest]

lineToTuple :: FilePath -> (FilePath, FilePath)
lineToTuple line = case splitOn " " line of
    [a, b] -> (a, b)
    _ -> error $ "Expected a line with two entries separated by space but got " <> show line

-- | Given a list of entries in the `MANIFEST` file, try to find an exact match for the given path.
findExact :: FilePath -> [(FilePath, FilePath)] -> Maybe FilePath
findExact path entries = fmap snd (find (\(k,_) -> k == path) entries)

-- | The `MANIFEST` file only contains file paths not directories so use this to lookup a directory.
findDir :: FilePath -> [(FilePath, FilePath)] -> Maybe FilePath
findDir path entries = do
    (k, v) <- find (\(k, v) -> path `isPrefixOf` k && drop (length path) k `isSuffixOf` v) entries
    -- The length of the file suffix after the directory
    let fileLength = length k - length path
    pure $ take (length v - fileLength) v
