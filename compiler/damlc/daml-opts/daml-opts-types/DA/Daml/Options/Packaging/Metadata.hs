-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

module DA.Daml.Options.Packaging.Metadata
  ( PackageDbMetadata(..),
    writeMetadata,
    readMetadata,
    renamingToFlag,
    metadataFile,
  ) where

import Data.Aeson
import DA.Daml.Package.Config ()
import Data.Map.Strict (Map)
import qualified Data.Text as T
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Options.Types
    ( projectPackageDatabase
    , ModRenaming(..)
    , PackageArg(..)
    , PackageFlag(..)
    )
import Development.IDE.Types.Location
import GHC.Generics
import qualified "ghc-lib-parser" Module as Ghc
import System.FilePath
import GHC.Fingerprint

-- | Metadata about an initialized package db. We write this to a JSON
-- file in the package db after the package db has been initialized.
--
-- While we can technically reconstruct all this information by
-- reading the DARs again, this is unnecessarily wasteful.
data PackageDbMetadata = PackageDbMetadata
  { directDependencies :: [Ghc.UnitId]
  -- ^ Unit ids of direct dependencies. These are exposed by default
  , moduleRenamings :: Map Ghc.UnitId (Ghc.ModuleName, [LF.ModuleName])
  -- ^ Map from GHC unit id to the prefix and a list of all modules in this package.
  -- We do not bother differentiating between exposed and unexposed modules
  -- since we already warn on non-exposed modules anyway and this
  -- is intended for data-dependencies where everything is exposed.
  , fingerprintDependencies :: Fingerprint
  -- ^ Hash over all dependency dars. We use this to check whether we need to reinitialize the
  -- package db or not.
  } deriving Generic

instance ToJSON Fingerprint
instance FromJSON Fingerprint

renamingToFlag :: Ghc.UnitId -> Ghc.ModuleName -> [LF.ModuleName] -> PackageFlag
renamingToFlag unitId prefix modules =
    ExposePackage
        ("Prefix " <> Ghc.unitIdString unitId <> " with " <> Ghc.moduleNameString prefix)
        (UnitIdArg unitId)
        ModRenaming
          { modRenamingWithImplicit = False
          , modRenamings =
            [ ( Ghc.mkModuleName s
              , Ghc.mkModuleName (Ghc.moduleNameString prefix ++ "." ++ s))
            | m <- modules
            , let s = T.unpack (LF.moduleNameString m)
            ]
          }

instance ToJSON PackageDbMetadata
instance FromJSON PackageDbMetadata

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
