-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

module DA.Daml.Options.Packaging.Metadata
  ( PackageDbMetadata(..),
    writeMetadata,
    readMetadata,
  ) where

import Data.Aeson
import DA.Daml.Options.Types (projectPackageDatabase)
import Development.IDE.Types.Location
import GHC.Generics
import qualified "ghc-lib-parser" Module as Ghc
import System.FilePath

-- | Metadata about an initialized package db. We write this to a JSON
-- file in the package db after the package db has been initialized.
--
-- While we can technically reconstruct all this information by
-- reading the DARs again, this is unnecessarily wasteful.
--
-- In the future, we should also be able to include enough metadata in here
-- to decide whether we need to reinitialize the package db or not.
data PackageDbMetadata = PackageDbMetadata
  { directDependencies :: [Ghc.UnitId]
  -- ^ Unit ids of direct dependencies. These are exposed by default
  } deriving Generic

instance ToJSON PackageDbMetadata
instance FromJSON PackageDbMetadata

-- Orphan instances for converting UnitIds to/from JSON.
instance ToJSON Ghc.UnitId where
    toJSON unitId = toJSON (Ghc.unitIdString unitId)

instance FromJSON Ghc.UnitId where
    parseJSON s = Ghc.stringToUnitId <$> parseJSON s

-- | Given the path to the project root, write out the package db metadata.
writeMetadata :: NormalizedFilePath -> PackageDbMetadata -> IO ()
writeMetadata projectRoot metadata = do
    encodeFile (metadataFile projectRoot) metadata

-- | Given the path to the project root, read the package db metadata.
-- Throws an exception if the file does not exist or
-- the format cannot be parsed.
readMetadata :: NormalizedFilePath -> IO PackageDbMetadata
readMetadata projectRoot = do
    errOrRes <- eitherDecodeFileStrict' (metadataFile projectRoot)
    case errOrRes of
        Right metadata -> pure metadata
        Left err -> fail ("Could not decode package metadata: " <> err)

-- | Given the path to the project root return the path
-- where the metadata is stored.
metadataFile :: NormalizedFilePath -> FilePath
metadataFile projectRoot =
    fromNormalizedFilePath projectRoot </>
    projectPackageDatabase </>
    "metadata.json"

