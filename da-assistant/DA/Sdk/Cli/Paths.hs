-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Paths
    ( showPath
    , findPath
    , runPackageMain
    , findExecutable
    , findPackagePath
    , extractPackagePath
    , extractExecutablePath
    , PackagePathQuery(..)
    , PackageExecutable(..)
    , PackageType(..)
    , PathError
    ) where

import           Control.Monad             (forM_)
import           DA.Sdk.Cli.Command.Types
import qualified DA.Sdk.Cli.Monad.Locations as Loc
import           DA.Sdk.Cli.Monad.FileSystem as FS
import qualified DA.Sdk.Cli.Locations      as L
import qualified DA.Sdk.Cli.Metadata       as Metadata
import           DA.Sdk.Cli.Monad
import qualified DA.Sdk.Cli.Sdk            as Sdk
import qualified DA.Sdk.Cli.SdkVersion     as SdkVersion
import           DA.Sdk.Prelude
import qualified DA.Sdk.Pretty             as P
import qualified DA.Sdk.Version            as V
import qualified Data.Map.Strict           as MS
import qualified Data.Text.Extended        as T
import qualified Data.Text                 as DT
import           Data.Typeable
import           DA.Sdk.Cli.System         (executeFile)
import           Control.Exception
import           DA.Sdk.Cli.Monad.UserInteraction
import           Control.Exception.Safe    (throwString, MonadThrow)
import           DA.Sdk.Cli.Conf.Types     (Project(..))
import           DA.Sdk.Version            (SemVersion (..))
import           System.Environment
--------------------------------------------------------------------------------
-- Types
--------------------------------------------------------------------------------

data PackageExecutable
    = PEBinary FilePath FilePath
      -- | @PEBinary packagePath relativeExecutablePath@
      -- Package contains a binary executable.
    | PEJar FilePath FilePath
      -- | @PEJar packagePath relativeExecutablePath@
      -- Package contains a java archive that can be executed with
      -- java -jar.
    deriving (Show)

data PackageType
    = PTExecutable PackageExecutable
    | PTPackage FilePath
      -- | @PTPackage packagePath@
      -- Package does not specify a main entry point.
    deriving (Show)

data PathError
    = PathErrorMissingExecutableInPackage FilePath
    | PathErrorPackageCouldNotBeFound FilePath
    deriving (Show, Typeable)

-- Make PathError an instance of Exception so that it can be thrown in MonadThrow
instance Exception PathError

showPath :: (Loc.MonadLocations m, MonadFS m, MonadUserOutput m, MonadThrow m) =>
       Maybe Text
    -> PackagePathQuery
    -- ^ Show path to the executable.
    -> Maybe Project -> Maybe SemVersion
    -> m ()
showPath mbPathName packagePathQuery proj defaultSdkVsn = case mbPathName of
    Just pathName ->
        findPath' proj defaultSdkVsn pathName packagePathQuery >>= \case
            p -> display $ pathToText p
    Nothing -> allPaths proj defaultSdkVsn

-- | Find the executable in a package.
findExecutable :: Text -> CliM PackageExecutable
findExecutable packageName = do
    packageType <- findPackageType packageName
    case packageType of
        PTPackage p -> throwM $ PathErrorMissingExecutableInPackage p
        PTExecutable e -> pure e

findExecutable' :: (Loc.MonadLocations m, MonadFS m, MonadThrow m) => Text -> Maybe Project -> Maybe SemVersion -> m PackageExecutable
findExecutable' packageName proj defaultSdkVsn = do
    packageType <- findPackageType' packageName proj defaultSdkVsn
    case packageType of
        PTPackage p -> throwM $ PathErrorMissingExecutableInPackage p
        PTExecutable e -> pure e

-- | Find the path to the package.
findPackagePath :: Text -> CliM FilePath
findPackagePath packageName = do
    packageType <- findPackageType packageName
    pure $ extractPackagePath packageType

findPackagePath' :: (Loc.MonadLocations m, MonadFS m, MonadThrow m) => Text -> Maybe Project -> Maybe SemVersion -> m FilePath
findPackagePath' packageName proj defaultSdkVsn = do
    packageType <- findPackageType' packageName proj defaultSdkVsn
    pure $ extractPackagePath packageType

-- | Depending on the 'PackagePathQuery' find either the path to the package
-- or the executable in the package.
findPath :: Text -> PackagePathQuery -> CliM FilePath
findPath packageName = \case
    PPQExecutable -> extractExecutablePath <$> findExecutable packageName
    PPQPackage -> findPackagePath packageName

findPath' :: (Loc.MonadLocations m, MonadFS m, MonadThrow m) => Maybe Project -> Maybe SemVersion -> Text -> PackagePathQuery -> m FilePath
findPath' proj defaultSdkVsn packageName = \case
    PPQExecutable -> extractExecutablePath <$> findExecutable' packageName proj defaultSdkVsn
    PPQPackage -> findPackagePath' packageName proj defaultSdkVsn

-- | Ask for the metadata of the currently active sdk version.
getSdkMetadata :: CliM (Either Sdk.SdkError Metadata.Metadata)
getSdkMetadata = do
    mbActiveVersion <- SdkVersion.getActiveSdkVersion
    sdkVersion <- SdkVersion.requireSdkVersion mbActiveVersion
    Sdk.getSdkMetadata sdkVersion

getSdkMetadata' :: (Loc.MonadLocations m, MonadFS m) => Maybe Project -> Maybe SemVersion -> m (Either Sdk.SdkError Metadata.Metadata)
getSdkMetadata' proj defaultSdkVsn = do
    mbActiveVersion <- SdkVersion.getActiveSdkVersion' proj defaultSdkVsn
    sdkVersion <- SdkVersion.requireSdkVersion' mbActiveVersion
    Sdk.getSdkMetadata sdkVersion

-- | Ask the metadata for the 'PackageType' of a package.
findPackageType :: Text -> CliM PackageType
findPackageType pathName = do
    errOrMetadata <- getSdkMetadata
    metadata <- either (throwString . show . P.renderPlain . P.pretty) return errOrMetadata
    packagesPath <- Loc.getPackagesPath
    let mbPackage = Metadata.findPackage metadata pathName
    case mbPackage of
        Just (grp, package) ->
            pure
          $ makePath packagesPath pathName (Metadata._gVersion grp) package
        Nothing -> throwM $ PathErrorPackageCouldNotBeFound $ textToPath pathName

findPackageType' :: (Loc.MonadLocations m, MonadFS m, MonadThrow m) => Text -> Maybe Project -> Maybe SemVersion -> m PackageType
findPackageType' pathName proj defaultSdkVsn = do
    errOrMetadata <- getSdkMetadata' proj defaultSdkVsn
    metadata <- either (throwString . show . P.renderPlain . P.pretty) return errOrMetadata
    packagesPath <- Loc.getPackagesPath
    let mbPackage = Metadata.findPackage metadata pathName
    case mbPackage of
        Just (grp, package) ->
            pure
          $ makePath packagesPath pathName (Metadata._gVersion grp) package
        Nothing -> throwM $ PathErrorPackageCouldNotBeFound $ textToPath pathName

allPackages :: (Loc.MonadLocations m, MonadFS m, MonadThrow m) => Maybe Project -> Maybe SemVersion -> m [(Text, PackageType)]
allPackages proj defaultSdkVsn = do
    errOrMetadata <- getSdkMetadata' proj defaultSdkVsn
    metadata <- either (throwString . show . P.renderPlain . P.pretty) return errOrMetadata
    packagesPath <- Loc.getPackagesPath
    return
      [ ( packageName
        , makePath
              packagesPath
              packageName
              (Metadata._gVersion packageGroup)
              package
        )
      | (_, packageGroup) <-
          MS.toList $ Metadata._mGroups metadata
      , (packageName, package) <-
          MS.toList $ Metadata._gPackages packageGroup
      ]

allPaths :: (Loc.MonadLocations m, MonadFS m, MonadUserOutput m, MonadThrow m) => Maybe Project -> Maybe SemVersion -> m ()
allPaths proj defaultSdkVsn = do
    packages <- allPackages proj defaultSdkVsn
    forM_ packages $ \(packageName, p) ->
        display $ packageName <> ": " <> pathToText (extractPackagePath p)

envVarWithSdkVersion :: String -> IO [(String, String)]
envVarWithSdkVersion version = do
    oldEnv <- getEnvironment
    return $ ("DAML_SDK_VERSION", version) : filter (\(a,_) -> a /= "DAML_SDK_VERSION") oldEnv


runPackageMain :: (Loc.MonadLocations m, MonadFS m, MonadIO m, MonadUserOutput m, MonadThrow m) => Maybe (Text, [Text]) -> Maybe Project -> Maybe SemVersion -> m ()
runPackageMain mbRunArgs proj defaultSdkVsn = case mbRunArgs of
    Just (pathName, args) -> do
        activeVersion <- SdkVersion.getActiveSdkVersion' proj defaultSdkVsn
        envM <- liftIO $ mapM (envVarWithSdkVersion . DT.unpack . V.showSemVersion) activeVersion
        executable <- findExecutable' pathName proj defaultSdkVsn
        case executable of
            PEBinary packagePath execPath ->
                liftIO $ executeFile
                    (pathToString $ packagePath </> execPath)
                    (map T.unpack args)
                    envM
            PEJar packagePath execPath ->
                liftIO $ executeFile
                    "java"
                    ("-jar" : pathToString (packagePath </> execPath) : map T.unpack args)
                    Nothing -- inherit environment

    Nothing -> do
        display "Missing: Package name and arguments\n"
        display "Runnable packages: "
        packages <- allPackages proj defaultSdkVsn
        forM_ packages $ \(packageName, ppt) ->
            case ppt of
                PTExecutable _ -> display $ "  " <> packageName
                PTPackage _ -> pure ()

--------------------------------------------------------------------------------
-- Package Paths
--------------------------------------------------------------------------------

makePath ::
       L.FilePathOfPackagesDir
    -> Text
    -> V.SemVersion
    -> Metadata.Package
    -> PackageType
makePath packagesPath packageName version package =
    let vsnTxt = V.showSemVersion version
        path   = (L.unwrapFilePathOf packagesPath
                    </> textToPath packageName
                    </> textToPath vsnTxt)  -- TODO make this a typesafe path
    in case Metadata._pMain package of
         Just mainPath
           | Metadata._pPackaging package == Metadata.PackagingJar ->
               PTExecutable $ PEJar path mainPath
           | otherwise ->
               PTExecutable $ PEBinary path mainPath
         Nothing | packageName == "sandbox" ->
               let jar = textToPath ("sandbox" <> "-" <> vsnTxt <> ".jar")
               in  PTExecutable $ PEJar path jar
         Nothing ->
           PTPackage path


-- | Extract the package path from a 'PackageType'.
extractPackagePath :: PackageType -> FilePath
extractPackagePath = \case
    PTExecutable (PEBinary p _) -> p
    PTExecutable (PEJar p _) -> p
    PTPackage p -> p

extractExecutablePath :: PackageExecutable -> FilePath
extractExecutablePath = \case
    PEBinary packagePath relativeExecutablePath ->
        packagePath </> relativeExecutablePath
    PEJar packagePath relativeExecutablePath ->
        packagePath </> relativeExecutablePath

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance P.Pretty PathError where
    pretty = \case
        PathErrorMissingExecutableInPackage path -> P.sep
            [ P.s "Couldn't find an executable in the package"
            , P.p path
            ]
        PathErrorPackageCouldNotBeFound path -> P.sep
            [ P.s "Package"
            , P.p path
            , P.s "could not be found"
            ]
