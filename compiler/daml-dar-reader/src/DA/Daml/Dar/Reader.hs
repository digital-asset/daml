-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Dar.Reader
    ( Format(..)
    , getDarInfo
    , inspectDar
    , InspectInfo(..)
    , DalfInfo(..)
    , collectInfo
    ) where

import "zip-archive" Codec.Archive.Zip qualified as ZipArchive
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.Proto3.Archive.Decode qualified as Archive
import DA.Daml.LF.Reader
import DA.Pretty (renderPretty)
import Data.Aeson
import Data.Aeson.Encode.Pretty
import Data.Bifunctor
import Data.ByteString qualified as B
import Data.ByteString.Lazy qualified as BSL
import Data.Either.Extra
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.List
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Builder qualified as TL
import System.Exit
import System.FilePath
import System.IO

data Format = PlainText | Json

data InspectInfo = InspectInfo
    { files :: [FilePath]
    , packages :: HashMap LF.PackageId DalfInfo
    , mainPackageId :: LF.PackageId
    }

instance ToJSON InspectInfo where
    toJSON InspectInfo{..} = object
      [ "files" .= files
      , "packages" .= packages
      , "main_package_id" .= mainPackageId
      ]

data DalfInfo = DalfInfo
  { dalfFilePath :: FilePath
  , dalfPackageName :: Maybe LF.PackageName
  , dalfPackageVersion :: Maybe LF.PackageVersion
  , dalfPackage :: LF.Package
  }

instance ToJSON DalfInfo where
    toJSON DalfInfo{..} = object
      [ "path" .= dalfFilePath
      , "name" .= dalfPackageName
      , "version" .= dalfPackageVersion
      ]

collectInfo :: ZipArchive.Archive -> Either String InspectInfo
collectInfo archive = do
    DalfManifest{..} <- readDalfManifest archive
    main@(mainPkgId, _) <- decodeEntry mainDalfPath
    deps <- mapM (\path -> (path,) <$> decodeEntry path) (delete mainDalfPath dalfPaths)
    pure InspectInfo
        { files = [ZipArchive.eRelativePath e | e <- ZipArchive.zEntries archive]
        , packages = HashMap.fromList (map handleDalf $ (mainDalfPath, main) : deps)
        , mainPackageId = mainPkgId
        }
  where
    handleDalf :: (FilePath, (LF.PackageId, LF.Package)) -> (LF.PackageId, DalfInfo)
    handleDalf (path, (pkgId, pkg)) =
        ( pkgId
        , DalfInfo
              path
              (LF.packageName <$> LF.packageMetadata pkg)
              (LF.packageVersion <$> LF.packageMetadata pkg)
              pkg
        )
    decodeEntry :: FilePath -> Either String (LF.PackageId, LF.Package)
    decodeEntry path = do
        entry <- maybeToEither
            ("Could not find " <> path <> " in DAR")
            (ZipArchive.findEntryByPath path archive)
        first renderPretty $
            Archive.decodeArchive Archive.DecodeAsDependency
            (BSL.toStrict $ ZipArchive.fromEntry entry)

renderInfo :: Format -> InspectInfo -> Text
renderInfo PlainText InspectInfo{..} = T.unlines $ concat
  [ [ "DAR archive contains the following files:"
    , ""
    ]
  , map T.pack files
  , [ ""
    , "DAR archive contains the following packages:"
    , ""
    ]
  , map (\(pkgId, DalfInfo{..}) -> T.pack $
             dropExtension (takeFileName dalfFilePath) <> " " <>
             show (LF.unPackageId pkgId)
        )
        -- Sort to not depend on the hash of the package id.
        (sortOn (dalfFilePath . snd) $ HashMap.toList packages)
  ]
renderInfo Json info =
    TL.toStrict (TL.toLazyText (encodePrettyToTextBuilder info))
    <> "\n"

inspectDar :: FilePath -> Format -> IO ()
inspectDar inFile format = do
    getDarInfo inFile >>= T.putStr . renderInfo format

getDarInfo :: FilePath -> IO InspectInfo
getDarInfo dar = do
    bytes <- B.readFile dar
    let dar = ZipArchive.toArchive $ BSL.fromStrict bytes
    case collectInfo dar of
        Left err -> do
            hPutStrLn stderr "Failed to read dar:"
            hPutStrLn stderr err
            exitFailure
        Right info -> pure info
