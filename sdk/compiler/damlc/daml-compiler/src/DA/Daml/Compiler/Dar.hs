-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Daml.Compiler.Dar
    ( createDarFile
    , buildDar
    , createArchive
    , FromDalf(..)
    , breakAt72Bytes
    , pkgNameVersion
    , getSrcRoot
    , getDamlFiles
    , getDamlRootFiles
    , writeIfacesAndHie
    , mkConfFile
    , expandSdkPackages
    , damlFilesInDir
    ) where

import qualified "zip" Codec.Archive.Zip as Zip
import Control.Applicative
import Control.Exception (assert)
import Control.Monad.Extra
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Maybe
import Control.Monad.Trans.Resource (ResourceT)
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (encodeArchiveAndHash)
import qualified DA.Daml.LF.Proto3.Archive as Archive
import DA.Daml.Compiler.ExtractDar (extractDar,ExtractedDar(..))
import DA.Daml.LF.TypeChecker.Error (Error(EUnsupportedFeature))
import DA.Daml.LF.TypeChecker.Upgrade as TypeChecker.Upgrade
import DA.Daml.Options (expandSdkPackages)
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Pretty (renderPretty)
import qualified DA.Service.Logger as Logger
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSC
import qualified Data.ByteString.Lazy.UTF8 as BSLUTF8
import Data.Conduit (ConduitT)
import Data.Conduit.Combinators (sourceFile, sourceLazy)
import Data.List.Extra
import qualified Data.Map.Strict as Map
import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Time
import Development.IDE.Core.API
import Development.IDE.Core.Service (getIdeOptions)
import Development.IDE.Core.RuleTypes.Daml
import Development.IDE.Core.Rules.Daml
import Development.IDE.Core.Shake
import Development.IDE.GHC.Compat
import Development.IDE.GHC.Util
import Development.IDE.Types.Location
import Development.IDE.Types.Options
import qualified Development.IDE.Types.Logger as IdeLogger
import System.Directory.Extra
import System.FilePath
import System.IO

import MkIface
import Module
import qualified Module as Ghc
import HscTypes
import qualified Data.SemVer as V
import DA.Daml.Project.Types (UnresolvedReleaseVersion(..))

import qualified "zip-archive" Codec.Archive.Zip as ZipArchive

-- | Create a DAR file by running a ZipArchive action.
createDarFile :: Logger.Handle IO -> FilePath -> Zip.ZipArchive () -> IO ()
createDarFile loggerH fp dar = do
    createDirectoryIfMissing True $ takeDirectory fp
    Zip.createArchive fp dar
    Logger.logInfo loggerH $ "Created " <> T.pack fp

------------------------------------------------------------------------------
{- | Builds a dar file.

A (fat) dar file is a zip file containing

* a dalf of a Daml library <name>.dalf
* a MANIFEST.MF file that describes the package
* all source files to that library
     - a dependency tree of imports
     - starting from the given top-level Daml 'file'
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

buildDar ::
       IdeState
    -> PackageConfigFields
    -> NormalizedFilePath
    -> FromDalf
    -> IO (Maybe (Zip.ZipArchive (), Maybe LF.PackageId))
buildDar service PackageConfigFields {..} ifDir dalfInput = do
    liftIO $
        IdeLogger.logDebug (ideLogger service) $
        "Creating dar: " <> T.pack pSrc
    if unFromDalf dalfInput
        then do
            bytes <- BSL.readFile pSrc
            -- in the dalfInput case we interpret pSrc as the filepath pointing to the dalf.
            -- Note that the package id is obviously wrong but this feature is not something we expose to users.
            pure $ Just
              ( createArchive pName pVersion pSdkVersion (LF.PackageId "") bytes [] (toNormalizedFilePath' ".") [] [] []
              , Nothing
              )
        -- We need runActionSync here to ensure that diagnostics are printed to the terminal.
        -- Otherwise runAction can return before the diagnostics have been printed and we might die
        -- without ever seeing diagnostics.
        else runActionSync service $
             runMaybeT $ do
                 files <- getDamlFiles pSrc
                 opts <- lift getIdeOptions
                 lfVersion <- lift getDamlLfVersion
                 mbUpgradedPackage <-
                   forM pUpgradedPackagePath $ \path ->
                     if lfVersion `LF.supports` LF.featurePackageUpgrades
                       then do
                         ExtractedDar{edMain} <- liftIO $ extractDar path
                         let bs = BSL.toStrict $ ZipArchive.fromEntry edMain
                         case Archive.decodeArchive Archive.DecodeAsMain bs of
                            Left _ -> error $ "Could not decode path " ++ path
                            Right (pid, package) -> return (pid, package)
                       else do
                         liftIO $
                           IdeLogger.logError (ideLogger service) $
                             renderPretty $ EUnsupportedFeature LF.featurePackageUpgrades
                         MaybeT (pure Nothing)
                 let pMeta = LF.PackageMetadata
                        { packageName = pName
                        , packageVersion = fromMaybe (LF.PackageVersion "0.0.0") pVersion
                        , upgradedPackageId = fst <$> mbUpgradedPackage
                        }
                 pkg <- case optShakeFiles opts of
                     Nothing -> mergePkgs pMeta lfVersion . map fst <$> usesE GeneratePackage files
                     Just _ -> generateSerializedPackage pName pVersion pMeta files

                 when pTypecheckUpgrades $
                     case mbUpgradedPackage of
                        Just (_, upgradedPackage) ->
                          MaybeT $ do
                            let upgradePair = Upgrading { past = upgradedPackage, present = pkg }
                            runDiagnosticCheck $ diagsToIdeResult (toNormalizedFilePath' pSrc) $ TypeChecker.Upgrade.checkUpgrade lfVersion upgradePair
                        _ -> pure ()
                 MaybeT $ finalPackageCheck (toNormalizedFilePath' pSrc) pkg

                 let pkgModuleNames = map (Ghc.mkModuleName . T.unpack) $ LF.packageModuleNames pkg

                 validateExposedModules pExposedModules pkgModuleNames

                 let (dalf,pkgId) = encodeArchiveAndHash pkg
                 -- For now, we don’t include ifaces and hie files in incremental mode.
                 -- The main reason for this is that writeIfacesAndHie is not yet ported to incremental mode
                 -- but it also makes creation of the archive slightly faster and those files are only required
                 -- for packaging. This definitely needs to be fixed before we can make incremental mode the default.
                 ifaces <-
                     MaybeT $ case optShakeFiles opts of
                         Nothing -> writeIfacesAndHie ifDir files
                         Just _ -> pure $ Just []
                 -- get all dalf dependencies.
                 dalfDependencies0 <- getDalfDependencies files
                 let dalfDependencies =
                         [ (T.pack $ unitIdString unitId, LF.dalfPackageBytes pkg, LF.dalfPackageId pkg)
                         | (unitId, pkg) <- Map.toList dalfDependencies0
                         ]
                 unstableDeps <- getUnstableDalfDependencies files
                 let confFile = mkConfFile pName pVersion (Map.keys unstableDeps) pExposedModules pkgModuleNames pkgId
                 let dataFiles = [confFile]
                 srcRoot <- getSrcRoot pSrc
                 pure
                   ( createArchive
                       pName pVersion pSdkVersion
                       pkgId
                       dalf
                       dalfDependencies
                       srcRoot
                       files
                       dataFiles
                       ifaces
                   , Just pkgId
                   )

validateExposedModules :: Maybe [ModuleName] -> [ModuleName] -> MaybeT Action ()
validateExposedModules mbExposedModules pkgModuleNames = do
    let missingExposed =
            S.fromList (fromMaybe [] mbExposedModules) S.\\
            S.fromList pkgModuleNames
    unless (S.null missingExposed) $ do
        -- FIXME: Should be producing a proper diagnostic
        liftIO $ hPutStrLn stderr $
            "The following modules are declared in exposed-modules but are not part of the DALF: " <>
            show (map Ghc.moduleNameString $ S.toList missingExposed)
        MaybeT (pure Nothing)
    whenJust mbExposedModules $ \exposedModules ->
        let hidden = pkgModuleNames \\ exposedModules
        in when (notNull hidden) $
           liftIO $ hPutStr stderr $ unlines
               [ "WARNING: The following modules are not part of exposed-modules: " <>
                 show (map Ghc.moduleNameString hidden)
               , "This can cause issues if those modules are referenced from a data-dependency."
               , "Suggestion: Remove the exposed-modules field from your daml.yaml file"
               , "to expose all modules."
               ]

-- | Write interface files and hie files to the location specified by the given options.
writeIfacesAndHie ::
       NormalizedFilePath -> [NormalizedFilePath] -> Action (Maybe [NormalizedFilePath])
writeIfacesAndHie ifDir files =
    runMaybeT $ do
        tcms <- usesE TypeCheck files
        fmap concat $ forM (zip files tcms) $ \(file, (tcm, _mapping)) -> do
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
            pure [toNormalizedFilePath' ifaceFp, toNormalizedFilePath' hieFp]

-- For backwards compatibility we allow both a file or a directory in "source".
-- For a file we use the import path as the src root.
getSrcRoot :: FilePath -> MaybeT Action NormalizedFilePath
getSrcRoot fileOrDir = do
  let fileOrDir' = toNormalizedFilePath' fileOrDir
  isDir <- liftIO $ doesDirectoryExist fileOrDir
  if isDir
      then pure fileOrDir'
      else do
          (pm, _mapping) <- useE GetParsedModule fileOrDir'
          Just root <- pure $ moduleImportPath fileOrDir' pm
          pure $ toNormalizedFilePath' root

-- | Merge several packages into one.
mergePkgs :: LF.PackageMetadata -> LF.Version -> [WhnfPackage] -> LF.Package
mergePkgs meta ver pkgs =
    foldl'
        (\pkg1 (WhnfPackage pkg2) -> assert (LF.packageLfVersion pkg1 == ver) $
             LF.Package
                 { LF.packageLfVersion = ver
                 , LF.packageModules = LF.packageModules pkg1 `NM.union` LF.packageModules pkg2
                 , LF.packageMetadata = LF.packageMetadata pkg1 <|> LF.packageMetadata pkg2
                 })
        LF.Package { LF.packageLfVersion = ver, LF.packageModules = NM.empty, LF.packageMetadata = Just meta }
        pkgs

-- | Find all Daml files below a given source root. If the source root is a file we interpret it as
-- main and return that file and all dependencies.
getDamlFiles :: FilePath -> MaybeT Action [NormalizedFilePath]
getDamlFiles srcRoot = do
    isDir <- liftIO $ doesDirectoryExist srcRoot
    if isDir
        then liftIO $ damlFilesInDir srcRoot
        else do
            let normalizedSrcRoot = toNormalizedFilePath' srcRoot
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
    pure $ map toNormalizedFilePath' $ filter (".daml" `isExtensionOf`) fs

-- | Find all Daml files below a given source root. If the source root is a file we interpret it as
-- main and return only that file. This is different from getDamlFiles which also returns
-- all dependencies.
getDamlRootFiles :: FilePath -> IO [NormalizedFilePath]
getDamlRootFiles srcRoot = do
    isDir <- liftIO $ doesDirectoryExist srcRoot
    if isDir
        then liftIO $ damlFilesInDir srcRoot
        else pure [toNormalizedFilePath' srcRoot]

mkConfFile
    :: LF.PackageName
    -> Maybe LF.PackageVersion
    -> [UnitId]
    -> Maybe [Ghc.ModuleName]
    -> [Ghc.ModuleName]
    -> LF.PackageId
    -> (FilePath, BS.ByteString)
mkConfFile pName pVersion pDependencies pExposedModules pkgModuleNames pkgId =
    (confName, confContent)
  where
    confName = unitIdString (pkgNameVersion pName pVersion) ++ ".conf"
    key = fullPkgName pName pVersion pkgId
    confContent =
        BSC.toStrict $
        BSC.pack $
        unlines $
            [ "name: " ++ T.unpack (LF.unPackageName pName)
            , "id: " ++ unitIdString (pkgNameVersion pName pVersion)
            ]
            ++ ["version: " ++ T.unpack v | Just (LF.PackageVersion v) <- [pVersion] ]
            ++
            [ "exposed: True"
            , "exposed-modules: " ++
              (unwords . map Ghc.moduleNameString . fromMaybe pkgModuleNames) pExposedModules
            , "import-dirs: ${pkgroot}" ++ "/" ++ key -- we really want '/' here
            , "library-dirs: ${pkgroot}" ++ "/" ++ key
            , "data-dir: ${pkgroot}" ++ "/" ++ key
            , "depends: " ++ unwords (map unitIdString pDependencies)
            ]

sinkEntryDeterministic
    :: Zip.CompressionMethod
    -> ConduitT () BS.ByteString (ResourceT IO) ()
    -> Zip.EntrySelector
    -> Zip.ZipArchive ()
sinkEntryDeterministic compression sink sel = do
    Zip.sinkEntry compression sink sel
    Zip.setModTime fixedTime sel
  -- The ZIP file format uses the MS-DOS timestamp format
  -- (I didn’t even knew that existed) which starts at
  -- 1980 rather than 1970.
  where fixedTime = UTCTime (fromGregorian 1980 1 1) 0

-- | Helper to bundle up all files into a DAR.
createArchive
    :: LF.PackageName
    -> Maybe LF.PackageVersion
    -> UnresolvedReleaseVersion
    -> LF.PackageId
    -> BSL.ByteString -- ^ DALF
    -> [(T.Text, BS.ByteString, LF.PackageId)] -- ^ DALF dependencies
    -> NormalizedFilePath -- ^ Source root directory
    -> [NormalizedFilePath] -- ^ Module dependencies
    -> [(String, BS.ByteString)] -- ^ Data files
    -> [NormalizedFilePath] -- ^ Interface files
    -> Zip.ZipArchive ()
createArchive pName pVersion pSdkVersion pkgId dalf dalfDependencies srcRoot fileDependencies dataFiles ifaces
 = do
    -- Reads all module source files, and pairs paths (with changed prefix)
    -- with contents as BS. The path must be within the module root path, and
    -- is modified to have prefix <name-hash> instead of the original root path.
    forM_ fileDependencies $ \mPath -> do
        entry <- Zip.mkEntrySelector $ pkgName </> fromNormalizedFilePath (makeRelative' srcRoot mPath)
        sinkEntryDeterministic Zip.Deflate (sourceFile $ fromNormalizedFilePath mPath) entry
    forM_ ifaces $ \mPath -> do
        let ifaceRoot =
                toNormalizedFilePath'
                    (ifaceDir </> fromNormalizedFilePath srcRoot)
        entry <- Zip.mkEntrySelector $ pkgName </> fromNormalizedFilePath (makeRelative' ifaceRoot mPath)
        sinkEntryDeterministic Zip.Deflate (sourceFile $ fromNormalizedFilePath mPath) entry
    let dalfName = pkgName </> fullPkgName pName pVersion pkgId <.> "dalf"
    let dependencies =
            [ (pkgName </> T.unpack depName <> "-" <> (T.unpack $ LF.unPackageId depPkgId) <> ".dalf", BSL.fromStrict bs)
            | (depName, bs, depPkgId) <- dalfDependencies
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
        sinkEntryDeterministic Zip.Deflate (sourceLazy content) entry
  where
    pkgName = fullPkgName pName pVersion pkgId
    manifestHeader :: FilePath -> [String] -> BSL.ByteString
    manifestHeader location dalfs =
        BSC.unlines $
        map (breakAt72Bytes . BSLUTF8.fromString)
            [ "Manifest-Version: 1.0"
            , "Created-By: damlc"
            , "Name: " <> unitIdString (pkgNameVersion pName pVersion)
            , "Sdk-Version: " <> V.toString (unwrapUnresolvedReleaseVersion pSdkVersion)
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
    toNormalizedFilePath' $
    -- Note that NormalizedFilePath only takes care of normalizing slashes.
    -- Here we also want to normalise things like ./a to a
    makeRelative (normalise $ fromNormalizedFilePath a) (normalise $ fromNormalizedFilePath b)
