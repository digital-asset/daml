-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Project
    ( Error
    , showError
    , createProjectFrom
    , installTemplate
    , getProject
    , requireProject
    , requireProject'

    , cleanupInstalledTemplate
    , runActivateScript

    , getOverlappingFiles

    -- Exported to enable testing
    , createTemplateBasedProjectInternal
    ) where

import qualified Control.Foldl                 as Foldl
import           Control.Monad.Error.Class     (throwError)
import           Control.Monad.Except          (ExceptT(..), runExceptT)
import           Control.Monad.Trans.Class     (lift)
import qualified DA.Sdk.Cli.Conf               as Conf
import qualified DA.Sdk.Cli.Monad.Locations    as Locations
import qualified DA.Sdk.Cli.Locations          as Loc
import qualified DA.Sdk.Cli.Locations.Turtle   as LocTurtle

import           DA.Sdk.Cli.Monad              (CliM, asks, envConf, envProject, logError)
import qualified DA.Sdk.Cli.SdkVersion         as SdkVersion
import           DA.Sdk.Prelude
import           DA.Sdk.Version                as V
import qualified DA.Sdk.Cli.Repository         as Repo
import           DA.Sdk.Cli.Template.Consts
import qualified DA.Sdk.Cli.Conf               as Cf
import qualified DA.Sdk.Pretty                 as P
import           Data.Bifunctor                (first)
import qualified Data.Text.Extended            as T
import qualified Data.Yaml                     as Yaml
import           System.Exit                   (exitFailure)
import           System.Environment            (getEnvironment)
import           DA.Sdk.Cli.System             (runShellScript)
import           Turtle                        (cd, cptree, fold, ls, lstree, mktree,
                                                pwd, rmtree, testdir, testfile, testpath)
import qualified DA.Sdk.Shell.Tar              as Tar
import qualified Filesystem.Path.CurrentOS     as F
import           DA.Sdk.Cli.Monad.UserInteraction
import DA.Sdk.Cli.Monad.MockIO
import qualified DA.Sdk.Cli.Monad.FileSystem as FS
import DA.Sdk.Data.Maybe.Extra
import Control.Monad.Trans.Maybe
import Control.Monad.Logger (MonadLogger)

data Error
    = EFolderNotEmpty FilePath
    | EIsFile FilePath
    | ESetupNeeded
    | EFailedToReadConfigSkeleton FilePath T.Text
    deriving (Show, Eq)

showError :: Error -> Text
showError =
    \case
        EFolderNotEmpty filePath ->
            "Folder is not empty: " <> pathToText filePath
        EIsFile filePath ->
            "There is already a file with this name: " <> pathToText filePath
        ESetupNeeded ->
            "Error occurred. setup needed"
        EFailedToReadConfigSkeleton filePath exception ->
            "Failed to read template config skeleton from " <> pathToText filePath <> ": " <> exception

installTemplate
    :: Repo.NameSpace
    -> Repo.TemplateName
    -> Repo.TemplateType
    -> FilePath
    -> CliM ()
installTemplate ns tn tp path = do
    if tp == Repo.Addon then do
        proj <- requireProject
        let projPath = Cf.projectPath proj
        when (absolute path || projPath `isPrefixOfPath` path)
                $ logExitErr ("Target path (" <> pathToText path <> ") is" <>
                              " not under project path (" <> pathToText projPath <> ")")
    else do
        alreadyExists <- testpath path
        when alreadyExists $ logExitErr ("Target path already exists: " <> pathToText path)
    daTempDir   <- Cf.getSdkTempDirPath
    daBinDir    <- Locations.getDaBinDirPath
    repoUrls    <- asks (Cf.confRepositoryURLs . envConf)
    credentials <- asks (Cf.confCredentials . envConf)
    sdkVsn      <- SdkVersion.getActiveSdkVersion >>= SdkVersion.requireSdkVersion
    handle      <- Repo.makeBintrayTemplateRepoHandle repoUrls credentials
    errOrOk     <- liftIO $ LocTurtle.withTempDirectory daTempDir
        "da-sdk-templ-install" $ \td -> do
            let tempDir    = stringToPath td
                extractDir = tempDir </> textToPath "extract"
            errOrU <- createTemplateBasedProjectInternal handle ns tn tp sdkVsn tempDir
            case errOrU of
                Left err -> return $ Left ("Failed to install template: "
                                         <> (P.renderPlain $ P.pretty err))
                Right fp -> do
                            mktree extractDir
                            result <- Tar.unTarGzip
                                          Tar.defaultUnTarGzipOptions
                                          fp
                                          extractDir
                            case result of
                              Left err -> return $ Left $ T.pack err
                              Right () -> do
                                -- Check overlapping files for Addon templates
                                mbOverlapErr <- if tp == Repo.Addon then do
                                    overlappingFiles <- getOverlappingFiles extractDir path
                                    if null overlappingFiles then
                                        return Nothing
                                    else
                                        let overlaps = T.intercalate ", " $ map pathToText overlappingFiles
                                            err = "Refusing to add template due to existing files: " <> overlaps
                                        in return $ Just err
                                else
                                    return Nothing
                                case mbOverlapErr of
                                    Just err ->
                                        return $ Left err
                                    Nothing -> do
                                        mktree path
                                        cptree extractDir path
                                        when (ns == Cf.sdkNameSpace) $ do
                                            cleanupInstalledTemplate path
                                            runActivateScript daBinDir extractDir path
                                        return $ Right ()
    either logError return errOrOk
  where
    isPrefixOfPath p fp = pathToString p `isPrefixOf` pathToString fp
    logExitErr err  = logError err >> liftIO exitFailure

createTemplateBasedProjectInternal
    :: Repo.TemplateRepoHandle
    -> Repo.NameSpace
    -> Repo.TemplateName
    -> Repo.TemplateType
    -> SemVersion
    -> FilePath
    -> IO (Either Repo.Error FilePath)
createTemplateBasedProjectInternal handle ns tn tType sdkVsn targetDir = runExceptT $ do
    mbInfos <- mapM (\v -> Repo.getInfo handle ns tn v) possibleVsns
    case findFirstNonNothingInfoWithVersion possibleVsns mbInfos of
      Just (Just tmplt, rl') -> tryGetting rl' tmplt
      _                      -> throwError noSuchTemplt
  where
    findFirstNonNothingInfoWithVersion possVsns mbInfs = find (isJust . fst) (zip mbInfs possVsns)
    isOfRightType t = (/=) Nothing $ find ((==) (Repo.Tag "type" $ T.show tType)) (Repo.templateTags t)
    (exactVsn, rl) = Repo.toExactVsnAndRelLine sdkVsn
    possibleVsns   = [exactVsn, rl]
    noSuchTemplt   = Repo.ErrorNoSuchTemplate tn rl tType
    tryGetting rl1 tmplt = do
        when (not $ isOfRightType tmplt) $ throwError noSuchTemplt
        lastRevMb <- Repo.getLatestRevision handle ns tn rl1
        case lastRevMb of
          Just lastR -> Repo.downloadTemplate handle ns tn rl1 lastR targetDir
          Nothing    -> throwError noSuchTemplt

createProjectFrom :: FilePath -> FilePath -> CliM ()
createProjectFrom templatePath toPath = do
    mbSdkVersion <- SdkVersion.getDefaultSdkVersion
    sdkVersion <- SdkVersion.requireSdkVersion mbSdkVersion
    daBinDir <- Locations.getDaBinDirPath
    liftIO $ do
        result <- runExceptT $ do
            dir <- directoryPath toPath
            let
                name = case pathToText $ basename dir of
                    "" -> pathToText $ dirname dir
                    d -> d
            display $ "Creating a project called " <> name
            display $ "in " <> pathToText dir <> "."

            templateExists <- testdir templatePath
            when templateExists $ cptree templatePath dir

            -- FIXME(JM): Temporary fix. Refactor this code to use
            -- Template.templateAdd.
            templateScriptsExists <- testdir (dir </> "_template")
            when templateScriptsExists $ rmtree (dir </> "_template")

            createProjectConfigFile name dir sdkVersion
        case result of
            Left e   -> display $ showError e
            Right () -> return ()
    liftIO $ runActivateScript daBinDir templatePath toPath

createProjectConfigFile :: Text -> FilePath -> V.SemVersion -> ExceptT Error IO ()
createProjectConfigFile name dir sdkVersion = do
    let props =
            Conf.Props
                [ Conf.PropProjectName name
                , Conf.PropProjectSDKVersion sdkVersion
                ]
        projectConfigFile  = dir </> "da.yaml"
        configSkeletonFile = dir </> "da-skeleton.yaml"

    templateProps <- readConfigSkeletonProps configSkeletonFile
    lift $ Yaml.encodeFile (encodeString projectConfigFile) (Conf.joinProps props templateProps)

readConfigSkeletonProps :: FilePath -> ExceptT Error IO Conf.Props
readConfigSkeletonProps configSkeletonFile =
    testfile configSkeletonFile >>= \case
        True  -> decodeConfigSkeleton configSkeletonFile
        False -> return defaultSkeletonProps
  where
    defaultSkeletonProps =
        Conf.Props
            [ Conf.PropDAMLSource "daml/Main.daml"
            , Conf.PropDAMLScenario (Just "Example.example")
            , Conf.PropProjectParties ["Alice", "Bob"]
            ]

decodeConfigSkeleton :: FilePath -> ExceptT Error IO Conf.Props
decodeConfigSkeleton configSkeletonFile = ExceptT $ do
    ioResult <- Yaml.decodeFileEither (pathToString configSkeletonFile)
    rmtree configSkeletonFile
    return $ first packError ioResult
  where
    packError exception = EFailedToReadConfigSkeleton configSkeletonFile (T.show exception)


getProject :: CliM (Maybe Conf.Project)
getProject = asks envProject

requireProject :: CliM Conf.Project
requireProject = getProject >>= \case
    Nothing -> do
        logError "This command requires a project. Switch to your project directory and try again."
        liftIO exitFailure
    Just proj ->
        return proj

data NotInProjectDir = NotInProjectDir deriving (Eq, Show)

requireProject' :: MonadLogger m => Maybe Cf.Project -> m (Either NotInProjectDir Conf.Project)
requireProject' mbProject =
  case mbProject of
    Nothing -> do
        logError "This command requires a project. Switch to your project directory and try again."
        return $ Left NotInProjectDir
    Just proj ->
        return $ Right proj

getOverlappingFiles :: (FS.MonadFS m, MockIO m) => FilePath -> FilePath -> m [FilePath]
getOverlappingFiles path1 path2 = do
    files <- mockDirectoryContents' $ sort <$> Turtle.fold (Turtle.lstree path1) Foldl.list
    forMaybeM files $ \path -> runMaybeT $ do
        Just suffix <- return $ F.stripPrefix (path1 </> "") path
        True <- lift $ FS.testfile' (path2 </> suffix)
        return suffix

-- | Return the directory to be used for the new project.
-- If it does exist and is either a file or non empty
-- return an error.
directoryPath :: FilePath -> ExceptT Error IO FilePath
directoryPath path = do
    projectDirectory <-
        if relative path then do
            cwd <- pwd
            pure $ collapse $ cwd </> path
        else
            pure path
    pathExists <- testpath projectDirectory
    isDirectory <- testdir projectDirectory
    if pathExists then
        if isDirectory then do
             dirContents <- fold (ls projectDirectory) Foldl.length
             if dirContents > 0 then
                 throwError $ EFolderNotEmpty projectDirectory
             else
                 pure projectDirectory
        else
            throwError $ EIsFile projectDirectory
    else
        pure projectDirectory

runActivateScript :: Loc.FilePathOfDaBinaryDir -> FilePath -> FilePath -> IO ()
runActivateScript daBinDir path toPath = do
    cwd <- pwd
    -- Switch to the project root
    cd toPath

    -- Run the activation script (from package path) if it exists.
    let activatePath = path </> templateScriptsDirectory </> "activate"
    activateExists <- testfile activatePath
    when activateExists $ do

        -- Add the path to 'da' binary to the PATH
        env <- getEnvironment
        let daBinDirStr = Loc.pathOfToString daBinDir
            envPath = fromMaybe "/bin" (lookup "PATH" env)
            env' =   ("PATH", daBinDirStr <> ":" <> envPath)
                    : filter (\(k,_) -> k /= "PATH") env

        -- Execute the activation script
        runShellScript (pathToString activatePath) [] (Just env')
    cd cwd

cleanupInstalledTemplate :: FilePath -> IO ()
cleanupInstalledTemplate path = do
    cwd <- pwd
    -- Switch to the project root
    cd path
    -- Remove the SDK internal files from the project
    templateScriptsExists <- testdir templateScriptsDirectory
    when templateScriptsExists $ rmtree templateScriptsDirectory
    cd cwd
