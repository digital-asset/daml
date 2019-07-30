-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Compiler.Dar
  ( buildDar
  , FromDalf(..)
  , breakAt72Chars
  ) where

import qualified Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import Data.List.Extra
import Data.Maybe
import qualified Data.Map.Strict as Map
import qualified Data.Set as S
import qualified Data.Text as T
import System.Directory
import System.FilePath

import Module (unitIdString)

import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.API
import Development.IDE.Types.Location
import qualified Development.IDE.Types.Logger as IdeLogger
import DA.Daml.Options.Types
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (encodeArchiveLazy)

------------------------------------------------------------------------------
{- | Builds a dar file.

A (fat) dar file is a zip file containing

* a dalf of a DAML library <name>.dalf
* a MANIFEST.MF file that describes the package
* all source files to that library
     - a dependency tree of imports
     - starting from the given top-level DAML 'file'
     - all these files _must_ reside in the same directory 'topdir'
     - the 'topdir' in the absolute path is replaced by 'name'
* all dalf dependencies
* additional data files under the data/ directory.

'topdir' is the path prefix of the top module that is _not_ part of the
qualified module name.
Example:  'file' = "/home/dude/work/solution-xy/daml/XY/Main/LibraryModules.daml"
contains "daml-1.2 module XY.Main.LibraryModules"
so 'topdir' is "/home/dude/work/solution-xy/daml"

The dar archive should stay independent of the dependency resolution tool. Therefore the pom file is
gernerated separately.

-}


-- | If true, we create the DAR from an existing .dalf file instead of compiling a *.daml file.
newtype FromDalf = FromDalf{unFromDalf :: Bool}

buildDar
  :: IdeState
  -> NormalizedFilePath
  -> Maybe [String] -- ^ exposed modules
  -> String -- ^ package name
  -> String -- ^ sdk version
  -> (LF.Package -> [(String, BS.ByteString)])
  -- We allow datafiles to depend on the package being produces to
  -- allow inference of things like exposedModules.
  -- Once we kill the old "package" command we could instead just
  -- pass "PackageConfigFields" to this function and construct the data
  -- files in here.
  -> FromDalf
  -> IO (Maybe BS.ByteString)
buildDar service file mbExposedModules pkgName sdkVersion buildDataFiles dalfInput = do
  let file' = fromNormalizedFilePath file
  liftIO $
    IdeLogger.logDebug (ideLogger service) $
    "Creating dar: " <> T.pack file'
  if unFromDalf dalfInput
    then liftIO $ Just <$> do
      bytes <- BSL.readFile file'
      createArchive
        bytes
        (toNormalizedFilePath $ takeDirectory file')
        []
        []
        []
        pkgName
        sdkVersion
    else runAction service $ runMaybeT $ do
      WhnfPackage pkg <- useE GeneratePackage file
      let pkgModuleNames = S.fromList $ map T.unpack $ LF.packageModuleNames pkg
      let missingExposed = S.fromList (fromMaybe [] mbExposedModules) S.\\ pkgModuleNames
      unless (S.null missingExposed) $
          -- FIXME: Should be producing a proper diagnostic
          error $
              "The following modules are declared in exposed-modules but are not part of the DALF: " <>
              show (S.toList missingExposed)
      let dalf = encodeArchiveLazy pkg
      -- get all dalf dependencies.
      dalfDependencies0 <- getDalfDependencies file
      let dalfDependencies =
              [ (T.pack $ unitIdString unitId, dalfPackageBytes pkg) | (unitId, pkg) <- Map.toList dalfDependencies0 ]
      -- get all file dependencies
      fileDependencies <- MaybeT $ getDependencies file
      liftIO $
        createArchive
          dalf
          (toNormalizedFilePath $ takeDirectory file')
          dalfDependencies
          (file:fileDependencies)
          (buildDataFiles pkg)
          pkgName
          sdkVersion

-- | Helper to bundle up all files into a DAR.
createArchive
  :: BSL.ByteString -- ^ DALF
  -> NormalizedFilePath -- ^ Module root used to locate interface and source files
  -> [(T.Text, BS.ByteString)] -- ^ DALF dependencies
  -> [NormalizedFilePath] -- ^ Module dependencies
  -> [(String, BS.ByteString)] -- ^ Data files
  -> String -- ^ Name of the main DALF
  -> String -- ^ SDK version
  -> IO BS.ByteString
createArchive dalf modRoot dalfDependencies fileDependencies dataFiles name sdkVersion = do
    -- Take all source file dependencies and produced interface files. Only the new package command
    -- produces interface files per default, hence we filter for existent files.
    ifaces <-
        fmap (map toNormalizedFilePath) $
        filterM doesFileExist $
        concat [[ifaceDir </> dep -<.> "hi", ifaceDir </> dep -<.> "hie"] | dep <- map fromNormalizedFilePath fileDependencies]

    -- Reads all module source files, and pairs paths (with changed prefix)
    -- with contents as BS. The path must be within the module root path, and
    -- is modified to have prefix <name> instead of the original root path.
    srcFiles <- forM fileDependencies $ \mPath -> do
      contents <- BSL.readFile $ fromNormalizedFilePath mPath
      return (name </> fromNormalizedFilePath (makeRelative' modRoot mPath), contents)

    ifaceFaceFiles <- forM ifaces $ \mPath -> do
      contents <- BSL.readFile $ fromNormalizedFilePath mPath
      let ifaceRoot = toNormalizedFilePath (ifaceDir </> fromNormalizedFilePath modRoot)
      return (name </> fromNormalizedFilePath (makeRelative' ifaceRoot mPath), contents)

    let dalfName = name <> ".dalf"
    let dependencies = [(T.unpack pkgName <> ".dalf", BSC.fromStrict bs)
                          | (pkgName, bs) <- dalfDependencies]
    let dataFiles' = [("data" </> n, BSC.fromStrict bs) | (n, bs) <- dataFiles]

    -- construct a zip file from all required files
    let allFiles = ("META-INF/MANIFEST.MF", manifestHeader dalfName $ dalfName:map fst dependencies)
                    : (dalfName, dalf)
                    : srcFiles
                    ++ ifaceFaceFiles
                    ++ dependencies
                    ++ dataFiles'

        mkEntry (filePath, content) = Zip.toEntry filePath 0 content
        zipArchive = foldr (Zip.addEntryToArchive . mkEntry) Zip.emptyArchive allFiles

    pure $ BSL.toStrict $ Zip.fromArchive zipArchive
      where
        manifestHeader :: FilePath -> [String] -> BSL.ByteString
        manifestHeader location dalfs = BSC.pack $ unlines
          [ "Manifest-Version: 1.0"
          , "Created-By: Digital Asset packager (DAML-GHC)"
          , "Sdk-Version: " <> sdkVersion
          , breakAt72Chars $ "Main-Dalf: " <> location
          , breakAt72Chars $ "Dalfs: " <> intercalate ", " dalfs
          , "Format: daml-lf"
          , "Encryption: non-encrypted"
          ]

breakAt72Chars :: String -> String
breakAt72Chars s = case splitAt 72 s of
  (s0, []) -> s0
  (s0, rest) -> s0 ++ "\n" ++ breakAt72Chars (" " ++ rest)

-- | Like `makeRelative` but also takes care of normalising filepaths so
--
-- > makeRelative' "./a" "a/b" == "b"
--
-- instead of
--
-- > makeRelative "./a" "a/b" == "a/b"
makeRelative' :: NormalizedFilePath -> NormalizedFilePath -> NormalizedFilePath
makeRelative' a b = toNormalizedFilePath $ makeRelative (fromNormalizedFilePath a) (fromNormalizedFilePath b)
