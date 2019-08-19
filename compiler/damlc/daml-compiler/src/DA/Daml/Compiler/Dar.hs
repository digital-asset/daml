-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Compiler.Dar
    ( buildDar
    , FromDalf(..)
    , breakAt72Chars
    , PackageConfigFields(..)
    , pkgNameVersion
    ) where

import qualified "zip" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (encodeArchiveAndHash)
import DA.Daml.Options.Types
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import Data.Conduit.Combinators (sourceFile, sourceLazy)
import Data.List.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import qualified Development.IDE.Types.Logger as IdeLogger
import Module
import SdkVersion
import System.FilePath

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
newtype FromDalf = FromDalf
    { unFromDalf :: Bool
    }

-- | daml.yaml config fields specific to packaging.
data PackageConfigFields = PackageConfigFields
    { pName :: String
    , pMain :: String
    , pExposedModules :: Maybe [String]
    , pVersion :: String
    , pDependencies :: [String]
    , pSdkVersion :: String
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
        "Creating dar: " <> T.pack pMain
    if unFromDalf dalfInput
        then do
            bytes <- BSL.readFile pMain
            pure $ Just $ createArchive pkgConf "" bytes [] (toNormalizedFilePath ".") [] [] []
        else runAction service $
             runMaybeT $ do
                 WhnfPackage pkg <- useE GeneratePackage file
                 parsedMain <- useE GetParsedModule file
                 let srcRoot =
                         toNormalizedFilePath $
                         fromMaybe (error "Cannot determine source root") $
                         moduleImportPaths parsedMain
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
                 ifaces <- MaybeT $ writeIfacesAndHie ifDir file
                 -- get all dalf dependencies.
                 dalfDependencies0 <- getDalfDependencies file
                 let dalfDependencies =
                         [ (T.pack $ unitIdString unitId, dalfPackageBytes pkg)
                         | (unitId, pkg) <- Map.toList dalfDependencies0
                         ]
                 -- get all file dependencies
                 fileDependencies <- MaybeT $ getDependencies file
                 let dataFiles =
                         [mkConfFile pkgConf pkgModuleNames (T.unpack pkgId)]
                 pure $
                     createArchive
                         pkgConf
                         (T.unpack pkgId)
                         dalf
                         dalfDependencies
                         srcRoot
                         (file : fileDependencies)
                         dataFiles
                         ifaces
  where
    file = toNormalizedFilePath pMain

fullPkgName :: String -> String -> String -> String
fullPkgName n v h = intercalate "-" [n, v, h]

pkgNameVersion :: String -> String -> String
pkgNameVersion n v = n ++ "-" ++ v

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
        unlines
            [ "name: " ++ pName
            , "id: " ++ pkgNameVersion pName pVersion
            , "key: " ++ pkgNameVersion pName pVersion
            , "version: " ++ pVersion
            , "exposed: True"
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
        BSC.pack $
        unlines
            [ "Manifest-Version: 1.0"
            , "Created-By: Digital Asset packager (DAML-GHC)"
            , "Sdk-Version: " <> pSdkVersion
            , breakAt72Chars $ "Main-Dalf: " <> toPosixFilePath location
            , breakAt72Chars $
              "Dalfs: " <> intercalate ", " (map toPosixFilePath dalfs)
            , "Format: daml-lf"
            , "Encryption: non-encrypted"
            ]
    -- zip entries do have posix filepaths. hence the entries in the manifest also need to be posix
    -- files paths regardless of the operatin system.
    toPosixFilePath :: FilePath -> FilePath
    toPosixFilePath = replace "\\" "/"

-- | Break lines at 72 characters and indent following lines by one space. As of MANIFEST.md
-- specification.
breakAt72Chars :: String -> String
breakAt72Chars s =
    case splitAt 72 s of
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
makeRelative' a b =
    toNormalizedFilePath $
    makeRelative (fromNormalizedFilePath a) (fromNormalizedFilePath b)
