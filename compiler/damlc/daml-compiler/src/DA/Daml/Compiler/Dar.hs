-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Compiler.Dar
    ( buildDar
    , FromDalf(..)
    , breakAt72Chars
    , PackageConfigFields(..)
    , pkgNameVersion
    ) where

import qualified Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Maybe
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (encodeArchiveAndHash)
import DA.Daml.Options.Types
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import Data.List.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Development.IDE.Core.API
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Types.Location
import qualified Development.IDE.Types.Logger as IdeLogger
import GHC
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
    -> IO (Maybe BSL.ByteString)
buildDar service pkgConf@PackageConfigFields {..} ifDir dalfInput = do
    liftIO $
        IdeLogger.logDebug (ideLogger service) $
        "Creating dar: " <> T.pack pMain
    if unFromDalf dalfInput
        then liftIO $
             Just <$> do
                 bytes <- BSL.readFile pMain
                 createArchive pkgConf "" bytes [] (toNormalizedFilePath ".") [] [] []
        else runAction service $
             runMaybeT $ do
                 WhnfPackage pkg <- useE GeneratePackage file
                 parsedMain <- useE GetParsedModule file
                 let srcRoot =
                         toNormalizedFilePath $
                         intercalate "/" $
                         dropSuffix
                             (splitOn
                                  "."
                                  (moduleNameString $
                                   moduleName $ ms_mod $ pm_mod_summary $ parsedMain))
                             (splitOn "/" $ dropExtension $ fromNormalizedFilePath file)
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
                 liftIO $
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
    confName = pName ++ ".conf"
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
            , "import-dirs: ${pkgroot}" </> key
            , "library-dirs: ${pkgroot}" </> key
            , "data-dir: ${pkgroot}" </> key
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
    -> IO BSL.ByteString
createArchive PackageConfigFields {..} pkgId dalf dalfDependencies srcRoot fileDependencies dataFiles ifaces
 = do
    -- Reads all module source files, and pairs paths (with changed prefix)
    -- with contents as BS. The path must be within the module root path, and
    -- is modified to have prefix <name-hash> instead of the original root path.
    srcFiles <-
        forM fileDependencies $ \mPath -> do
            contents <- BSL.readFile $ fromNormalizedFilePath mPath
            return
                ( pkgName </>
                  fromNormalizedFilePath (makeRelative' srcRoot mPath)
                , contents)
    ifaceFaceFiles <-
        forM ifaces $ \mPath -> do
            contents <- BSL.readFile $ fromNormalizedFilePath mPath
            let ifaceRoot =
                    toNormalizedFilePath
                        (ifaceDir </> fromNormalizedFilePath srcRoot)
            return
                ( pkgName </>
                  fromNormalizedFilePath (makeRelative' ifaceRoot mPath)
                , contents)
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
            srcFiles ++ ifaceFaceFiles ++ dependencies ++ dataFiles'
        mkEntry (filePath, content) = Zip.toEntry filePath 0 content
        zipArchive =
            foldr (Zip.addEntryToArchive . mkEntry) Zip.emptyArchive allFiles
    pure $ Zip.fromArchive zipArchive
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
