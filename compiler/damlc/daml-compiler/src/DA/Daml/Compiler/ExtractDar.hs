-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Compiler.ExtractDar
  ( ExtractedDar(..)
  , extractDar
  , getEntry
  ) where

import "zip-archive" Codec.Archive.Zip qualified as ZipArchive
import Control.Monad.Extra
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.UTF8 qualified as BSUTF8
import Data.List.Extra
import System.FilePath
import DA.Daml.LF.Reader

data ExtractedDar = ExtractedDar
    { edSdkVersions :: String
    , edMain :: ZipArchive.Entry
    , edConfFiles :: [ZipArchive.Entry]
    , edDalfs :: [ZipArchive.Entry]
    , edSrcs :: [ZipArchive.Entry]
    }

-- | Extract a dar archive
extractDar :: FilePath -> IO ExtractedDar
extractDar fp = do
    bs <- BSL.readFile fp
    let archive = ZipArchive.toArchive bs
    manifest <- getEntry manifestPath archive
    dalfManifest <- either fail pure $ readDalfManifest archive
    mainDalfEntry <- getEntry (mainDalfPath dalfManifest) archive
    sdkVersion <-
        case parseManifestFile $ BSL.toStrict $ ZipArchive.fromEntry manifest of
            Left err -> fail err
            Right manifest ->
                case lookup "Sdk-Version" manifest of
                    Nothing -> fail "No Sdk-Version entry in manifest"
                    Just version -> pure $! trim $ BSUTF8.toString version
    let confFiles =
            [ e
            | e <- ZipArchive.zEntries archive
            , ".conf" `isExtensionOf` ZipArchive.eRelativePath e
            ]
    let srcs =
            [ e
            | e <- ZipArchive.zEntries archive
            , takeExtension (ZipArchive.eRelativePath e) `elem`
                  [".daml", ".hie", ".hi"]
            ]
    dalfs <- forM (dalfPaths dalfManifest) $ \p -> getEntry p archive
    pure (ExtractedDar sdkVersion mainDalfEntry confFiles dalfs srcs)

-- | Get an entry from a dar or fail.
getEntry :: FilePath -> ZipArchive.Archive -> IO ZipArchive.Entry
getEntry fp dar =
    maybe (fail $ "Package does not contain " <> fp) pure $
    ZipArchive.findEntryByPath fp dar
