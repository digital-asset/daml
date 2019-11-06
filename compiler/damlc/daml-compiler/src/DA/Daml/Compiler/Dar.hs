-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Compiler.Dar
    ( buildDar
    , FromDalf(..)
    , breakAt72Bytes
    , PackageConfigFields(..)
    , pkgNameVersion
    , getSrcRoot
    , getDamlFiles
    , getDamlRootFiles
    , writeIfacesAndHie
    , mkConfFile
    ) where

import qualified "zip" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (encodeArchiveAndHash)
import DA.Daml.Options.Types
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import qualified Data.ByteString.Lazy.UTF8 as BSLUTF8
import Data.Conduit.Combinators (sourceFile, sourceLazy)
import Data.List.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Set as S
import qualified Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Shake
import Development.IDE.GHC.Compat
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import qualified Development.IDE.Types.Logger as IdeLogger
import SdkVersion
import System.Directory.Extra
import System.FilePath

import GHC
import MkIface
import Module
import HscTypes

------------------------------------------------------------------------------
{- | Builds a dar file.

A (fat) dar file is a zip file containing

* a dalf of a DAML library <name>.dalf
* a MANIFEST.MF file that describes the package
* all source files to that library
     - a dependency tree of imports
     - starting from the given top-level DAML 'file'
     - all these files _must_ reside in the same “source root” directory
     - the “source root” in the absolute path is replaced by 'name-hash'
* all dalf dependencies
* additional data files under the data/ directory.

“source root” corresponds to the import directory for a module,
i.e., the path prefix that is not part of the module name.
Example:  'file' = "/home/dude/work/solution-xy/daml/XY/Main/LibraryModules.daml"
contains "daml-1.2 module XY.Main.LibraryModules"
so “source root” is "/home/dude/work/solution-xy/daml"

The dar archive should stay independent of the dependency resolution tool. Therefore the pom file is
gernerated separately.

-}
-- | If true, we create the DAR from an existing .dalf file instead of compiling a *.daml file.
newtype FromDalf = FromDalf
    { unFromDalf :: Bool
    }

-- | daml.yaml config fields specific to packaging.
data PackageConfigFields = PackageConfigFields
    { pName :: String
    , pSrc :: String
    , pExposedModules :: Maybe [String]
    , pVersion :: Maybe String
    , pDependencies :: [String]
    , pSdkVersion :: String
    , cliOpts :: Maybe [String]
    }

buildDar ::
       IdeState
    -> PackageConfigFields
    -> NormalizedFilePath
    -> FromDalf
    -> IO (Maybe (Zip.ZipArchive ()))
buildDar service pkgConf@PackageConfigFields {..} ifDir dalfInput = do
    liftIO $
        IdeLogger.logDebug (ideLogger service) $
        "Creating dar: " <> T.pack pSrc
    if unFromDalf dalfInput
        then do
            bytes <- BSL.readFile pSrc
            -- in the dalfInput case we interpret pSrc as the filepath pointing to the dalf.
            pure $ Just $ createArchive pkgConf "" bytes [] (toNormalizedFilePath ".") [] [] []
        else runAction service $
             runMaybeT $ do
                 files <- getDamlFiles pSrc
                 pkgs <- usesE GeneratePackage files
                 let pkg = mergePkgs pkgs
                 let pkgModuleNames = map T.unpack $ LF.packageModuleNames pkg
                 let missingExposed =
                         S.fromList (fromMaybe [] pExposedModules) S.\\
                         S.fromList pkgModuleNames
                 unless (S.null missingExposed) $
                     -- FIXME: Should be producing a proper diagnostic
                     error $
                     "The following modules are declared in exposed-modules but are not part of the DALF: " <>
                     show (S.toList missingExposed)
                 let (dalf, pkgId) = encodeArchiveAndHash pkg
                 -- create the interface files
                 ifaces <- MaybeT $ writeIfacesAndHie ifDir files
                 -- get all dalf dependencies.
                 dalfDependencies0 <- getDalfDependencies files
                 let dalfDependencies =
                         [ (T.pack $ unitIdString unitId, LF.dalfPackageBytes pkg)
                         | (unitId, pkg) <- Map.toList dalfDependencies0
                         ]
                 let dataFiles = [mkConfFile pkgConf pkgModuleNames (T.unpack pkgId)]
                 srcRoot <- getSrcRoot pSrc
                 pure $
                     createArchive
                         pkgConf
                         (T.unpack pkgId)
                         dalf
                         dalfDependencies
                         srcRoot
                         files
                         dataFiles
                         ifaces

-- | Write interface files and hie files to the location specified by the given options.
writeIfacesAndHie ::
       NormalizedFilePath -> [NormalizedFilePath] -> Action (Maybe [NormalizedFilePath])
writeIfacesAndHie ifDir files =
    runMaybeT $ do
        tcms <- usesE TypeCheck files
        fmap concat $ forM (zip files tcms) $ \(file, tcm) -> do
            session <- lift $ hscEnv <$> use_ GhcSession file
            liftIO $ writeTcm session tcm
  where
    writeTcm session tcm =
        do
            let fp =
                    fromNormalizedFilePath ifDir </>
                    (ms_hspp_file $
                     pm_mod_summary $ tm_parsed_module $ tmrModule tcm)
            createDirectoryIfMissing True (takeDirectory fp)
            let ifaceFp = replaceExtension fp ".hi"
            let hieFp = replaceExtension fp ".hie"
            writeIfaceFile
                (hsc_dflags session)
                ifaceFp
                (hm_iface $ tmrModInfo tcm)
            hieFile <-
                liftIO $
                runHsc session $
                mkHieFile
                    (pm_mod_summary $ tm_parsed_module $ tmrModule tcm)
                    (fst $ tm_internals_ $ tmrModule tcm)
                    (fromJust $ tm_renamed_source $ tmrModule tcm)
            writeHieFile hieFp hieFile
            pure [toNormalizedFilePath ifaceFp, toNormalizedFilePath hieFp]

-- For backwards compatibility we allow both a file or a directory in "source".
-- For a file we use the import path as the src root.
getSrcRoot :: FilePath -> MaybeT Action NormalizedFilePath
getSrcRoot fileOrDir = do
  let fileOrDir' = toNormalizedFilePath fileOrDir
  isDir <- liftIO $ doesDirectoryExist fileOrDir
  if isDir
      then pure fileOrDir'
      else do
          pm <- useE GetParsedModule fileOrDir'
          Just root <- pure $ moduleImportPath fileOrDir' pm
          pure $ toNormalizedFilePath root

-- | Merge several packages into one.
mergePkgs :: [WhnfPackage] -> LF.Package
mergePkgs [] = error "No package build when building dar"
mergePkgs (WhnfPackage pkg0:pkgs) =
    foldl
        (\pkg1 (WhnfPackage pkg2) ->
             LF.Package
                 { LF.packageLfVersion = LF.packageLfVersion pkg2
                 , LF.packageModules = LF.packageModules pkg1 `NM.union` LF.packageModules pkg2
                 })
        pkg0
        pkgs

-- | Find all DAML files below a given source root. If the source root is a file we interpret it as
-- main and return that file and all dependencies.
getDamlFiles :: FilePath -> MaybeT Action [NormalizedFilePath]
getDamlFiles srcRoot = do
    isDir <- liftIO $ doesDirectoryExist srcRoot
    if isDir
        then liftIO $ damlFilesInDir srcRoot
        else do
            let normalizedSrcRoot = toNormalizedFilePath srcRoot
            deps <- MaybeT $ getDependencies normalizedSrcRoot
            pure (normalizedSrcRoot : deps)

-- | Return all daml files in the given directory.
damlFilesInDir :: FilePath -> IO [NormalizedFilePath]
damlFilesInDir srcRoot = do
    -- don't recurse into hidden directories (for example the .daml dir).
    fs <-
        listFilesInside
            (\fp ->
                 return $ fp == "." || (not $ isPrefixOf "." $ takeFileName fp))
            srcRoot
    pure $ map toNormalizedFilePath $ filter (".daml" `isExtensionOf`) fs

-- | Find all DAML files below a given source root. If the source root is a file we interpret it as
-- main and return only that file. This is different from getDamlFiles which also returns
-- all dependencies.
getDamlRootFiles :: FilePath -> IO [NormalizedFilePath]
getDamlRootFiles srcRoot = do
    isDir <- liftIO $ doesDirectoryExist srcRoot
    if isDir
        then liftIO $ damlFilesInDir srcRoot
        else pure [toNormalizedFilePath srcRoot]

fullPkgName :: String -> Maybe String -> String -> String
fullPkgName n mbV h =
    case mbV of
        Nothing -> n <> "-" <> h
        Just v -> n <> "-" <> v <> "-" <> h

pkgNameVersion :: String -> Maybe String -> String
pkgNameVersion n mbV =
    case mbV of
        Nothing -> n
        Just v -> n ++ "-" ++ v

mkConfFile ::
       PackageConfigFields -> [String] -> String -> (String, BS.ByteString)
mkConfFile PackageConfigFields {..} pkgModuleNames pkgId = (confName, bs)
  where
    confName = pkgNameVersion pName pVersion ++ ".conf"
    key = fullPkgName pName pVersion pkgId
    sanitizeBaseDeps "daml-stdlib" = damlStdlib
    sanitizeBaseDeps dep = dep
    bs =
        BSC.toStrict $
        BSC.pack $
        unlines $
            [ "name: " ++ pName
            , "id: " ++ pkgNameVersion pName pVersion
            , "key: " ++ pkgNameVersion pName pVersion
            ]
            ++ ["version: " ++ v | Just v <- [pVersion] ]
            ++
            [ "exposed: True"
            , "exposed-modules: " ++
              unwords (fromMaybe pkgModuleNames pExposedModules)
            , "import-dirs: ${pkgroot}" ++ "/" ++ key -- we really want '/' here
            , "library-dirs: ${pkgroot}" ++ "/" ++ key
            , "data-dir: ${pkgroot}" ++ "/" ++ key
            , "depends: " ++
              unwords
                  [ sanitizeBaseDeps $ dropExtension $ takeFileName dep
                  | dep <- pDependencies
                  ]
            ]

-- | Helper to bundle up all files into a DAR.
createArchive ::
       PackageConfigFields
    -> String
    -> BSL.ByteString -- ^ DALF
    -> [(T.Text, BS.ByteString)] -- ^ DALF dependencies
    -> NormalizedFilePath -- ^ Source root directory
    -> [NormalizedFilePath] -- ^ Module dependencies
    -> [(String, BS.ByteString)] -- ^ Data files
    -> [NormalizedFilePath] -- ^ Interface files
    -> Zip.ZipArchive ()
createArchive PackageConfigFields {..} pkgId dalf dalfDependencies srcRoot fileDependencies dataFiles ifaces
 = do
    -- Reads all module source files, and pairs paths (with changed prefix)
    -- with contents as BS. The path must be within the module root path, and
    -- is modified to have prefix <name-hash> instead of the original root path.
    forM_ fileDependencies $ \mPath -> do
        entry <- Zip.mkEntrySelector $ pkgName </> fromNormalizedFilePath (makeRelative' srcRoot mPath)
        Zip.sinkEntry Zip.Deflate (sourceFile $ fromNormalizedFilePath mPath) entry
    forM_ ifaces $ \mPath -> do
        let ifaceRoot =
                toNormalizedFilePath
                    (ifaceDir </> fromNormalizedFilePath srcRoot)
        entry <- Zip.mkEntrySelector $ pkgName </> fromNormalizedFilePath (makeRelative' ifaceRoot mPath)
        Zip.sinkEntry Zip.Deflate (sourceFile $ fromNormalizedFilePath mPath) entry
    let dalfName = pkgName </> pkgNameVersion pName pVersion <.> "dalf"
    let dependencies =
            [ (pkgName </> T.unpack depName <> ".dalf", BSL.fromStrict bs)
            | (depName, bs) <- dalfDependencies
            ]
    let dataFiles' =
            [ (pkgName </> "data" </> n, BSC.fromStrict bs)
            | (n, bs) <- dataFiles
            ]
    -- construct a zip file from all required files
    let allFiles =
            ( "META-INF/MANIFEST.MF"
            , manifestHeader dalfName (dalfName : map fst dependencies)) :
            (dalfName, dalf) :
            dependencies ++ dataFiles'
    forM_ allFiles $ \(file, content) -> do
        entry <- Zip.mkEntrySelector file
        Zip.sinkEntry Zip.Deflate (sourceLazy content) entry
  where
    pkgName = fullPkgName pName pVersion pkgId
    manifestHeader :: FilePath -> [String] -> BSL.ByteString
    manifestHeader location dalfs =
        BSC.unlines $
        map (breakAt72Bytes . BSLUTF8.fromString)
            [ "Manifest-Version: 1.0"
            , "Created-By: damlc"
            , "Sdk-Version: " <> pSdkVersion
            , "Main-Dalf: " <> toPosixFilePath location
            , "Dalfs: " <> intercalate ", " (map toPosixFilePath dalfs)
            , "Format: daml-lf"
            , "Encryption: non-encrypted"
            ]
    -- zip entries do have posix filepaths. hence the entries in the manifest also need to be posix
    -- files paths regardless of the operatin system.
    toPosixFilePath :: FilePath -> FilePath
    toPosixFilePath = replace "\\" "/"

-- | Break lines at 72 characters and indent following lines by one space. As of MANIFEST.md
-- specification.
breakAt72Bytes :: BSL.ByteString -> BSL.ByteString
breakAt72Bytes s =
    -- We break at 71 to give us one byte for \n (BSC.unlines will always use \n, never \r\n).
    case BSL.splitAt 71 s of
        (s0, rest)
            | BSL.null rest -> s0
            | otherwise -> s0 <> "\n" <> breakAt72Bytes (BSC.cons ' ' rest)

-- | Like `makeRelative` but also takes care of normalising filepaths so
--
-- > makeRelative' "./a" "a/b" == "b"
--
-- instead of
--
-- > makeRelative "./a" "a/b" == "a/b"
makeRelative' :: NormalizedFilePath -> NormalizedFilePath -> NormalizedFilePath
makeRelative' a b =
    toNormalizedFilePath $
    -- Note that NormalizedFilePath only takes care of normalizing slashes.
    -- Here we also want to normalise things like ./a to a
    makeRelative (normalise $ fromNormalizedFilePath a) (normalise $ fromNormalizedFilePath b)
