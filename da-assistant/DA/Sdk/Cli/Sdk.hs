-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DataKinds #-}

module DA.Sdk.Cli.Sdk
    ( Error
    , SdkError
    , SdkUninstallError
    , prettyError
    , readSdkMetadata
    , installPackage
    , installLatestSdk

    , installSdkM
    , sdkUse
    , sdkList
    , sdkUpgrade
    , sdkUninstall

    , getSdkMetadata
    , getSdkPackageVersionDirPath

    , confirm
    ) where

import           DA.Sdk.Prelude             hiding (group)
import           Control.Lens               (over, _Left)
import           Control.Monad.Trans.Except (ExceptT (..), runExceptT, throwE, withExceptT)
import           Control.Monad.Catch        (MonadMask)
import           Data.Either                (isLeft)
import           Data.Either.Combinators    (whenRight)
import qualified Data.Map.Strict            as MS
import qualified Data.Yaml                  as Yaml
import qualified Data.Set                   as S
import qualified Turtle
import qualified DA.Sdk.Cli.Package         as Package
import qualified DA.Sdk.Cli.Repository      as Repo
import qualified DA.Sdk.Cli.Changelog       as Changelog
import           DA.Sdk.Cli.Conf
import qualified DA.Sdk.Cli.Locations       as L
import qualified DA.Sdk.Cli.Locations.Turtle as LT
import           DA.Sdk.Cli.Monad.Locations as Loc
import qualified DA.Sdk.Cli.Metadata        as Metadata
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Cli.Monad.MockIO
import           DA.Sdk.Cli.Monad.FileSystem as FS
import qualified DA.Sdk.Cli.OS              as OS
import qualified DA.Sdk.Cli.SdkVersion      as SdkVersion
import           DA.Sdk.Version             (SemVersion (..), showSemVersion)
import qualified DA.Sdk.Pretty              as P
import qualified DA.Sdk.Cli.Command.Types   as CT
import           DA.Sdk.Cli.System          (linkOrCopy', removeDaHomeDir)
import           DA.Sdk.Cli.Monad.Repository
import           DA.Sdk.Cli.Monad.UserInteraction
import           DA.Sdk.Cli.Binaries
import           Data.Bifunctor (first)
import           Control.Monad.Trans.Class (lift)
import           Data.Text as T hiding (group)
import           Control.Monad.Logger       (MonadLogger)

--------------------------------------------------------------------------------
-- Types
--------------------------------------------------------------------------------

data Error p
    = MetadataFileNotFound L.FilePathOfSdkPackageVersionMetafile
    | PackageError (Package.InstallPackageFailed SemVersion p)
    | VersionParseError
    | YamlError Yaml.ParseException
    | YamlEncodeError EncodeYamlFileFailed
    | RepoError Repo.Error
    | BinDirPopError BinDirPopulationError
    | GetInstalledSdkVsnsError FS.LsFailed
    deriving (Show)

type SdkError = Error 'L.SdkPackage

data SdkUninstallError
    = ConfirmationError ReadInputLineFailed
    | CannotGetInstalledSdkVsns FS.LsFailed

--------------------------------------------------------------------------------
-- SDK
--------------------------------------------------------------------------------

-- | The coordinates for the SDK release artifact
sdkArtifact :: Repo.Package
sdkArtifact = Repo.Package "sdk" ["com", "digitalasset"]

-- | Install a SDK package
installPackage ::
       (MockIO m, MonadFS m)
    => Repo.Handle m
    -> Bool
    -> L.FilePathOfSdkTmpDir
    -> L.FilePathOfPackagesDir
    -> Repo.Repository
    -> Text
    -> Metadata.Group
    -> Metadata.Package
    -> m (Either (Error p) (Package.InstallResult p))
installPackage handle versioned tempDir packagesPath pkgRepo packageName group package =
    runExceptT $ do
      let packagePath = getPackagePath packagesPath packageName versioned version
      errOrOk <- ExceptT . fmap Right $ Package.installPackage handle pkgRepo (downloadablePackage Nothing) tempDir packagePath
      case (errOrOk, packaging) of
        (Left err, Metadata.PackagingTarGz _) | Package.RepoError _ <- Package.ipfError err ->
            -- Retry with classifier
            ExceptT
              $ fmap (over _Left PackageError)
              $ Package.installPackage handle pkgRepo (downloadablePackage classifier) tempDir packagePath
        (Left err, _) -> throwE $ PackageError err
        (Right p, _)  -> return p

  where
    version = Metadata._gVersion group
    packaging = Metadata._pPackaging package
    classifier =
        case OS.currentOS of
            OS.Linux   -> Just "linux"
            OS.MacOS   -> Just "osx"
            OS.Windows -> Just "win"
            _          -> error "OS not supported"
    downloadablePackage mbClassifier =
        Repo.DownloadablePackage
        { Repo._dpClassifier = mbClassifier
        , Repo._dpPackaging = packaging
        , Repo._dpPackage =
            Repo.Package
            { Repo._pId = packageName
            , Repo._pGroup = Metadata._gGroupId group
            }
        , Repo._dpVersion = version
        }

-- | Attempt to install a specific SDK release and the mandatory packages
-- in the release.
-- isExperimental means installing the SDK "meta" package (the one containing sdk.yaml
-- and NOTHING else) from the experimental repo.
installSdk :: (MonadUserOutput m, MockIO m, MonadFS m) => Repo.Handle m -> Repo.Repository -> SemVersion -> L.FilePathOfSdkTmpDir -> L.FilePathOfPackagesDir -> m (Either SdkError ())
installSdk handle metaPkgRepo sdkVersion tempDir packagesPath = runExceptT $ do
    let sdkDownloadablePackage =
          Repo.DownloadablePackage
          { Repo._dpClassifier = Nothing
          , Repo._dpPackaging  = Metadata.PackagingTarGz Metadata.TarGz
          , Repo._dpPackage = sdkArtifact
          , Repo._dpVersion = sdkVersion
          }
    let targetPath = L.addPathSuffix (textToPath (showSemVersion sdkVersion)) $ L.sdkPackagePath packagesPath
    installPath <-
        ExceptT $ over _Left PackageError
                <$> Package.installPackage handle metaPkgRepo sdkDownloadablePackage tempDir targetPath
    metadata <- ExceptT . readSdkMetadata $ Package.getPath installPath
    case installPath of
      Package.AlreadyInstalled _p ->
        displayNoLine $ "Already installed: " <> showSemVersion sdkVersion <> ", checking packages:"
      Package.Success _iPath -> do
        displayNoLine "Installing packages:"

    -- Parse the newly installed SDK metadata and install mandatory component
    -- since there can be a partial state where the sdk version is installed
    -- but some dependencies are missing, we always check and install the packages
    forM_ (MS.toList $ Metadata._mGroups metadata) $ \(_name, group) -> do
        let version = Metadata._gVersion group
        forM_ (MS.toList $ Metadata._gPackages group) $ \(artifactId, package) ->
          unless (Metadata._pDoc package) $ do
            res <- ExceptT $ installPackage handle True tempDir packagesPath Repo.daRepository artifactId group package
            case res of
              Package.AlreadyInstalled _ ->
                return ()
              _ ->
                displayNoLine $ " " <> artifactId <> "-" <> showSemVersion version
    displayNewLine

    -- Activate the SDK
    mockIO_ $ activateSdk packagesPath metadata

activateSdk :: L.FilePathOfPackagesDir -> Metadata.Metadata -> IO ()
activateSdk packagesPath metadata = do
    displayNoLine "Activating packages:"
    phase "daml-extension" activateExtension
    displayNewLine
  where
    phase name act = do
        displayNoLine $ " " <> name
        act

    activateExtension = do
      -- Lookup daml extension package
      let mbPackage = Metadata.findPackage metadata "daml-extension"

      -- Replace the symlink in .vscode
      homePath <- Turtle.home
      let damlExtTargetPath = homePath </> ".vscode" </> "extensions" </> "da-vscode-daml-extension"
      case mbPackage of
        Nothing      ->
          displayStr "No daml-extension included, not activating it"
        Just (group, _package) -> do
          let version = Metadata._gVersion group
              path = L.packageVersionDirPath packagesPath (textToPath "daml-extension") version
          installed <- LT.testdir path
          unless installed $
            error "daml-extension specified in release, but not installed"
          vscodeExtenstionPath <- Loc.getVscodeExtensionPath
          whenRight vscodeExtenstionPath $ \f -> do void $ FS.rm' f
          Turtle.mktree damlExtTargetPath
          status <- linkOrCopy' True path damlExtTargetPath
          when (isLeft status) $
            error "activating daml-extension failed: could not create symlink"

installLatestSdk :: (MonadUserOutput m, MockIO m, MonadMask m, MonadFS m)
                 => Repo.Handle m
                 -> L.FilePathOfSdkTmpDir
                 -> L.FilePathOfPackagesDir
                 -> m (Either SdkError SemVersion)
installLatestSdk handle tempDir packagesPath = runExceptT $ do
    latestVersion <- withExceptT RepoError . ExceptT $ Repo.hLatestSdkVersion handle Repo.defaultTagsFilter
    ExceptT $ installSdk handle Repo.daRepository latestVersion tempDir packagesPath
    return latestVersion

getSdkMetadata :: (Loc.MonadLocations m, MonadFS m) => SemVersion -> m (Either SdkError Metadata.Metadata)
getSdkMetadata version = do
    path <- getSdkPackageVersionDirPath version
    readSdkMetadata path

readSdkMetadata :: MonadFS m => L.FilePathOfSdkPackageVersionDir -> m (Either SdkError Metadata.Metadata)
readSdkMetadata path =
    first mapErr <$> Metadata.readSdkMetadata path
  where
    mapErr err =
      case err of
        Metadata.MetadataFileNotFound f -> MetadataFileNotFound f
        Metadata.YamlError e -> YamlError e

--------------------------------------------------------------------------------
-- SDK Commands
--------------------------------------------------------------------------------

installSdkM :: (MonadRepo m, MonadUserOutput m, MockIO m,
                MonadFS m, Loc.MonadLocations m)
               => Conf -> Repo.Repository -> SemVersion -> m (Either SdkError ())
installSdkM conf sdkMetaPkgRepo sdkVersion = do
    handle <- makeHandle conf
    daTempDir <- Loc.getSdkTempDirPath
    packagesDir <- getPackagesPath
    installSdk handle sdkMetaPkgRepo sdkVersion daTempDir packagesDir

-- | Extract the tags used for filtering of sdk versions from the environment.
askTagsFilter :: Monad m => Conf -> m Repo.TagsFilter
askTagsFilter conf = do
    pure $ Repo.TagsFilter
        { Repo._includedTags = S.fromList $ confSDKIncludedTags conf
        , Repo._excludedTags = S.fromList $ confSDKExcludedTags conf
        }

-- | upgrade: Upgrade the current SDK version to the latest version
sdkUpgrade :: (MockIO m, MonadLocations m, MonadLogger m, MonadRepo m, MonadUserOutput m, MonadFS m)
           => Maybe Project -> Maybe SemVersion -> Conf -> m (Either SdkError ())
sdkUpgrade mbProj defaultSdkVsn conf = runExceptT $ do
    -- Resolve the latest SDK version
    display "Checking for latest version... "
    handle <- lift $ makeHandle conf
    tagsFilter <- askTagsFilter conf
    latest <- withExceptT RepoError $ ExceptT $ Repo.hLatestSdkVersion handle tagsFilter
    display $ showSemVersion latest
    if Just latest == defaultSdkVsn then
      display $ "Already default version: " <> showSemVersion latest
    else do
      -- Install and activate the new version
      ExceptT $ sdkUse conf Repo.daRepository latest

    -- Show the changelog entry if possible
    errOrChange <- Changelog.readChange mbProj (Just latest)
    case errOrChange of
        Left _ -> pure ()
        Right change ->
            displayPretty change

-- | list: List installed SDK versions
-- Example:
--   0.1.2
--   0.2.0 (active)
sdkList :: CliM (Either SdkError ())
sdkList = runExceptT $ do
    defaultVersion <- lift SdkVersion.getDefaultSdkVersion
    activeVersion <- lift SdkVersion.getActiveSdkVersion
    installedUnsorted <- withExceptT GetInstalledSdkVsnsError $ ExceptT SdkVersion.getInstalledSdkVersions
    let installed = sort installedUnsorted
    displayStr "Installed SDK releases:"
    forM_ installed $ \v -> let version = Just v in do
            let status | version == defaultVersion && version == activeVersion = "(active, default)"
                        | version == defaultVersion = "(default)"
                        | version == activeVersion = "(active)"
                        | otherwise = ""
            display $ "  " <> showSemVersion v <> " " <> status

-- | use: Set the currently active SDK version, downloading it if necessary.
sdkUse :: (MockIO m, MonadUserOutput m, MonadRepo m, MonadLogger m, MonadFS m, MonadLocations m)
       => Conf -> Repo.Repository -> SemVersion -> m (Either SdkError ())
sdkUse conf sdkMetaPkgRepo version = runExceptT $ do
      -- Make sure the version is installed
      ExceptT $ installSdkM conf sdkMetaPkgRepo version
      -- Activate the new version by setting the default SDK version in the
      -- default configuration.
      -- FIXME(JM): Move this out from here
      display $ "Activating SDK version " <> showSemVersion version <> "..."
      configPath <- Loc.getDefaultConfigFilePath
      errorOrProps <- decodeYamlFile configPath
      let props' = either (const []) fromProps errorOrProps ++ [PropSDKDefaultVersion (Just version)]
      _ <- withExceptT YamlEncodeError $ ExceptT $ encodeYamlFile configPath (Props props')

      daHomeDir <- Loc.getDaHomePath
      daBinDir <- Loc.getDaBinDirPath
      withExceptT BinDirPopError $ ExceptT $
        populateBinWithPackagesBinaries (L.packagesPath daHomeDir) daBinDir version

confirm :: (MonadUserInput m, MonadUserOutput m) => Bool -> Text -> m (Either ReadInputLineFailed Bool)
confirm isScript question = runExceptT $ do
  if isScript then do
    display "This command needs confirmation and cannot be ran in script mode."
    return False
  else do
    display question
    answer <- ExceptT readInputLine
    return $ isAffirmative answer
  where
    isAffirmative answer = T.toLower answer `elem` ["y", "yes"]

confirm' :: (MonadUserInput m, MonadUserOutput m) => Bool -> Text -> m (Either SdkUninstallError Bool)
confirm' isScript question = first ConfirmationError <$> confirm isScript question

-- | uninstall: Remove a version or the complete SDK.
sdkUninstall :: (MockIO m, MonadFS m, MonadLocations m, MonadUserOutput m, MonadUserInput m)
             => Maybe Project -> Maybe SemVersion -> CT.SdkUninstallAction
             -> Bool -> m (Either SdkUninstallError ())
sdkUninstall _ _ CT.UninstallAll isScript = runExceptT $ do
    isConfirmed <- ExceptT $ confirm' isScript "Do you really want to remove DAML SDK? [y/N]"
    when isConfirmed $ do
        daHomePath <- Loc.getDaHomePath
        haveSymLink <- testfile' "/usr/local/bin/da"
        isConfirmed2 <- ExceptT $ confirm' isScript ("Do you really want to remove the DAML SDK home directory "
                                                    <> "and all of its contents? [y/N] " <> L.pathOfToText daHomePath)
        when isConfirmed2 $ do
            display $ "Deleting " <> L.pathOfToText daHomePath
            daBin <- getDaBinPath
            mockIO_ $ removeDaHomeDir daHomePath daBin
            vscodeExtenstionPath <- Loc.getVscodeExtensionPath
            case vscodeExtenstionPath of
                Right path -> void $ FS.rm' path
                Left _ -> mockIO_ $ display "Could not determine vscode extension path "
            when haveSymLink $ do
                isConfirmed3 <- ExceptT $ confirm' isScript "Do you want to remove the symlink /usr/local/bin/da? [y/N] "
                when isConfirmed3 $ do
                    display "removing symlink /usr/local/bin/da" -- Note that this can only happen on non-Win systems: rm -rf is OK.
                    mockIO_ $ void $ Turtle.shell ("rm -f " <> "/usr/local/bin/da") Turtle.empty
sdkUninstall _ _ (CT.Uninstall []) _isScript =
    Right <$> display "Please specify versions to uninstall or 'all' to remove everything."
sdkUninstall mbProj defaultSdkVsn (CT.Uninstall versions) isScript =
    runExceptT $ forM_ versions removeVersion
  where
      removeVersion :: (MonadUserInput m, MonadUserOutput m, MonadLocations m, MonadFS m, MockIO m)
                    => SemVersion -> ExceptT SdkUninstallError m ()
      removeVersion v = do
          active <- SdkVersion.getActiveSdkVersion' mbProj defaultSdkVsn
          installed <- withExceptT CannotGetInstalledSdkVsns $ ExceptT SdkVersion.getInstalledSdkVersions
          let isInstalled = v `elem` installed
          if isInstalled then
            if active == Just v then
              display $ "Version " <> showSemVersion v <> " is active, cannot delete it."
            else
              if defaultSdkVsn == Just v then
                 display $ "Version " <> showSemVersion v <> " is set as default, please change it first using 'da use' before attempting removal"
              else do
                isConfirmed <- ExceptT $ confirm' isScript ("Do you really want to remove DAML SDK version " <> showSemVersion v <> "? [y/N]")
                when isConfirmed $ do
                   pkgDir <- Loc.getSdkPackageTemplateDirPath v
                   _ <- mockIO_ $ void $ Turtle.shell ("rm -rf " <> L.pathOfToText pkgDir) Turtle.empty
                   display $ "Deleted version " <> showSemVersion v
          else
            display $ "Version " <> showSemVersion v <>  " is not installed"

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

prettyError :: Error p -> P.Doc ann
prettyError er = case er of
    MetadataFileNotFound path -> P.sep
        [ P.t $ L.pathOfToText path
        , P.reflow "is not a valid sdk folder ('sdk.yaml') missing."
        ]
    YamlError pe              ->
        P.pretty $ Yaml.prettyPrintParseException pe
    PackageError packageError ->
        P.pretty packageError
    RepoError repoErr ->
        P.pretty repoErr
    VersionParseError -> "Failed to parse SDK version"
    BinDirPopError binDirPopErr ->
        P.pretty binDirPopErr
    GetInstalledSdkVsnsError (FS.LsFailed f _) ->
        P.t $ "Unable to get installed SDK versions: directory cannot be listed: "
                <> pathToText f
    YamlEncodeError (EncodeYamlFileFailed f _) ->
        P.t $ "Failed to encode YAML file: " <> pathToText f

instance P.Pretty (Error p) where
    pretty = prettyError

instance P.Pretty SdkUninstallError where
    pretty (ConfirmationError (ReadInputLineFailed _)) =
        P.t "Action confirmation error, an error happened while trying to read user error."
    pretty (CannotGetInstalledSdkVsns (FS.LsFailed _ _)) =
        P.t "Cannot get installed SDK versions, unable to list the package directory."