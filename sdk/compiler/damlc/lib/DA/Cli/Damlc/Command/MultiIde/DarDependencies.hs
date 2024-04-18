-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Cli.Damlc.Command.MultiIde.DarDependencies (resolveSourceLocation) where

import "zip-archive" Codec.Archive.Zip (Archive (..), Entry(..), toArchive, toEntry, fromArchive, fromEntry, findEntryByPath, deleteEntryFromArchive)
import Control.Monad (forM_, unless, void)
import DA.Cli.Damlc.Command.MultiIde.Types (MultiIdeState (..), PackageSourceLocation (..))
import DA.Daml.Compiler.Dar (breakAt72Bytes, mkConfFile)
import qualified DA.Daml.LF.Ast.Base as LF
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.LF.Proto3.Archive (DecodingMode (..), decodeArchive)
import DA.Daml.LF.Reader (DalfManifest(..), readManifest, readDalfManifest)
import DA.Daml.Project.Consts (projectConfigName)
import Data.Bifunctor (second)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.ByteString as BS
import Data.List (delete, intercalate, isPrefixOf)
import Data.List.Extra (dropEnd)
import Data.List.Split (splitOn)
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.NameMap as NM
import qualified Data.Text as T
import System.Directory (createDirectoryIfMissing, doesDirectoryExist)
import System.FilePath

import qualified Module as Ghc

-- Given a dar, attempts to recreate the package structure for the IDE, with all files set to read-only.
unpackDar :: MultiIdeState -> FilePath -> IO ()
unpackDar miState darPath = do
  debugPrint miState $ "Unpacking dar: " <> darPath
  archiveWithSource <- toArchive <$> BSL.readFile darPath
  manifest <- either fail pure $ readDalfManifest archiveWithSource
  rawManifest <- either fail pure $ readManifest archiveWithSource
  let (archive, damlFiles) = extractDarSourceFiles archiveWithSource

  mainDalf <- maybe (fail "Couldn't find main dalf in dar") pure $ findEntryByPath (mainDalfPath manifest) archive

  let mainPackageId = extractPackageIdFromEntry mainDalf
      darUnpackLocation = unpackedDarPath miState mainPackageId

  void $ flip Map.traverseWithKey damlFiles $ \path content -> do
    let fullPath = darUnpackLocation </> "daml" </> path
    createDirectoryIfMissing True (takeDirectory fullPath)
    BSL.writeFile fullPath content

  let mainDalfContent = BSL.toStrict $ fromEntry mainDalf
      ignoredPrefixes = ["daml-stdlib", "daml-prim", "daml-script", "daml3-script", takeBaseName $ eRelativePath mainDalf]
      -- Filter dalfs first such that none start with `daml-stdlib` or `daml-prim`, `daml-script` or `daml3-script`
      -- then that the package id of the dalf isn't in the LF for the main package
      dalfsToExpand =
        flip filter (zEntries archive) $ \entry ->
          takeExtension (eRelativePath entry) == ".dalf"
            && not (any (\prefix -> prefix `isPrefixOf` takeBaseName (eRelativePath entry)) ignoredPrefixes)
            && BS.isInfixOf (BSC.pack $ extractPackageIdFromEntry entry) mainDalfContent
      -- Rebuild dalfs into full dars under dars directory
      darDepArchives = 
        fmap (\entry -> 
          ( darUnpackLocation </> "dars" </> takeBaseName (eRelativePath entry) <.> "dar"
          , rebuildDarFromDalfEntry archive rawManifest (dalfPaths manifest) (eRelativePath mainDalf) entry
          )
        ) dalfsToExpand
  
  -- Write dar files
  forM_ darDepArchives $ \(path, archive) -> do
    createDirectoryIfMissing True (takeDirectory path)
    BSL.writeFile path $ fromArchive archive

  (_, mainPkg) <- either (fail . show) pure $ decodeArchive DecodeAsMain mainDalfContent

  let isSdkPackage pkgName entry = pkgName == intercalate "-" (dropEnd 2 $ splitOn "-" $ takeBaseName $ eRelativePath entry)
      includesSdkPackage pkgName = any (isSdkPackage pkgName) $ zEntries archive
      sdkPackages = ["daml-script", "daml3-script", "daml-trigger"]
      deps = ["daml-prim", "daml-stdlib"] <> filter includesSdkPackage sdkPackages
      damlYamlContent = unlines $
        [ "sdk-version: " <> sdkVersion manifest
        , "name: " <> T.unpack (LF.unPackageName $ LF.packageName $ LF.packageMetadata mainPkg)
        , "version: " <> T.unpack (LF.unPackageVersion $ LF.packageVersion $ LF.packageMetadata mainPkg)
        , "source: daml"
        , "build-options:"
        , "  - --target=" <> LF.renderVersion (LF.packageLfVersion mainPkg)
        , "dependencies:"
        ]
        <> fmap ("  - " <>) deps
        <> ["data-dependencies: "]
        <> fmap (\(path, _) -> "  - " <> makeRelative darUnpackLocation path) darDepArchives

  writeFile (darUnpackLocation </> projectConfigName) damlYamlContent

extractPackageIdFromEntry :: Entry -> String
extractPackageIdFromEntry = extractPackageIdFromDalfPath . eRelativePath

extractPackageIdFromDalfPath :: FilePath -> String
extractPackageIdFromDalfPath = last . splitOn "-" . takeBaseName

unpackedDarPath :: MultiIdeState -> String -> FilePath
unpackedDarPath miState pkgId = multiPackageHome miState </> ".daml" </> "unpacked-dars" </> pkgId

-- Pull out every daml file into a mapping from path to content
-- Return an archive without these files or any hi/hie files
extractDarSourceFiles :: Archive -> (Archive, Map FilePath BSL.ByteString)
extractDarSourceFiles archive = foldr handleEntry (archive, Map.empty) $ zEntries archive
  where
    handleEntry :: Entry -> (Archive, Map FilePath BSL.ByteString) -> (Archive, Map FilePath BSL.ByteString)
    handleEntry entry (archive', damlFiles) =
      case takeExtension $ eRelativePath entry of
        ".daml" -> (deleteEntryFromArchive (eRelativePath entry) archive', Map.insert (joinPath $ tail $ splitPath $ eRelativePath entry) (fromEntry entry) damlFiles)
        ".hi" -> (deleteEntryFromArchive (eRelativePath entry) archive', damlFiles)
        ".hie" -> (deleteEntryFromArchive (eRelativePath entry) archive', damlFiles)
        _ -> (archive', damlFiles)

-- Recreate the conf file from a dalf
readDalfConf :: Entry -> (FilePath, BSL.ByteString)
readDalfConf entry =
  let (pkgId, pkg) = either (error . show) id $ decodeArchive DecodeAsMain $ BSL.toStrict $ fromEntry entry
      moduleNames = Ghc.mkModuleName . T.unpack . T.intercalate "." . LF.unModuleName <$> NM.names (LF.packageModules pkg)
      pkgName = LF.packageName $ LF.packageMetadata pkg
      pkgVersion = LF.packageVersion $ LF.packageMetadata pkg
      -- TODO[SW]: the `depends` list is empty right now, as we don't have the full dar dependency tree.
   in second BSL.fromStrict $ mkConfFile pkgName (Just pkgVersion) [] Nothing moduleNames pkgId

-- Copies all dalf files over, changing their directory to match the new main package
-- Updates the Name, Main-Dalf and Dalfs fields in the manifest to reflect the new main package/dalf locations
-- Updates the <package>/data/<package>.conf file to reflect the new package (note that the "depends" field is a little tricky)
rebuildDarFromDalfEntry :: Archive -> [(BS.ByteString, BS.ByteString)] -> [FilePath] -> FilePath -> Entry -> Archive
rebuildDarFromDalfEntry archive rawManifest dalfPaths topDalfPath mainEntry = archive {zEntries = mapMaybe mapEntry $ zEntries archive}
  where
    mapEntry :: Entry -> Maybe Entry
    mapEntry entry =
      case takeExtension $ eRelativePath entry of
        -- Need to remove the top level dar
        ".dalf" | eRelativePath entry == topDalfPath -> Nothing
        ".dalf" -> Just $ entry {eRelativePath = updatePathToMainEntry $ eRelativePath entry}
        ".MF" -> Just $ toEntry (eRelativePath entry) (eLastModified entry) $ serialiseRawManifest $ overwriteRawManifestFields rawManifest
          [ ("Name", BSC.pack mainEntryId)
          , ("Main-Dalf", BSC.pack $ updatePathToMainEntry $ eRelativePath mainEntry)
          , ("Dalfs", BS.intercalate ", " $ BSC.pack . updatePathToMainEntry <$> dalfPathsWithoutTop)
          ]
        ".conf" ->
          let (confFileName, confContent) = readDalfConf mainEntry
           in Just $ toEntry
                (replaceFileName (updatePathToMainEntry $ eRelativePath entry) confFileName)
                (eLastModified entry)
                confContent
        _ -> Just entry
    dalfPathsWithoutTop = delete topDalfPath dalfPaths
    mainEntryName = takeBaseName $ eRelativePath mainEntry
    mainEntryId = intercalate "-" $ init $ splitOn "-" mainEntryName
    updatePathToMainEntry = joinPath . (mainEntryName :) . tail . splitPath
    serialiseRawManifest :: [(BS.ByteString, BS.ByteString)] -> BSL.ByteString
    serialiseRawManifest = BSLC.unlines . map (\(k, v) -> breakAt72Bytes $ BSL.fromStrict $ k <> ": " <> v)
    overwriteRawManifestFields :: [(BS.ByteString, BS.ByteString)] -> [(BS.ByteString, BS.ByteString)] -> [(BS.ByteString, BS.ByteString)]
    overwriteRawManifestFields original overwrites' = fmap (\(k, v) -> (k, fromMaybe v $ Map.lookup k overwrites)) original
      where
        overwrites = Map.fromList overwrites'

resolveSourceLocation :: MultiIdeState -> PackageSourceLocation -> IO FilePath
resolveSourceLocation _ (PackageOnDisk path) = pure path
resolveSourceLocation miState (PackageInDar darPath) = do
  debugPrint miState "Looking for unpacked dar"
  archive <- toArchive <$> BSL.readFile darPath
  manifest <- either fail pure $ readDalfManifest archive
  -- Extracting package id from the dalf name, as it is cheap.
  -- TODO: Consider if this could be a problem
  let pkgId = extractPackageIdFromDalfPath $ mainDalfPath manifest
      pkgPath = unpackedDarPath miState pkgId

  pkgExists <- doesDirectoryExist pkgPath
  unless pkgExists $ unpackDar miState darPath

  pure pkgPath
