-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Commands for managing SDK templates
module DA.Sdk.Cli.Template
    ( templateAdd
    , templatePublish
    , listUsableTemplates
    , listUsableTemplateTuples
    , getTemplateInfo
    , checkNamespaceExists
    , createProject

    , builtInAddonTemplateNames
    , builtInProjectTemplateNames

    -- Internal logic exported for testing
    , templatePublishInternal
    , listUsableTemplatesInternal

    , listRepos

    , getReleaseTemplates
    , getBuiltinTemplates
    , templateScriptsDirectory

    , TemplateInfo(..)
    ) where

import           Control.Lens                hiding ((<.>))
import           DA.Sdk.Cli.Conf             (confCredentials, confRepositoryURLs,
                                              getSdkTempDirPath, projectPath, confIsScript)
import qualified DA.Sdk.Cli.Monad.Locations  as Loc
import qualified DA.Sdk.Cli.Locations        as L
import qualified DA.Sdk.Cli.Locations.Turtle as LT
import qualified DA.Sdk.Cli.Metadata         as Metadata
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Cli.Project
import           DA.Sdk.Cli.Template.Consts
import qualified DA.Sdk.Cli.Repository       as Repo
import qualified DA.Sdk.Cli.Repository.Types as Ty
import           DA.Sdk.Cli.Sdk
import qualified DA.Sdk.Cli.SdkVersion       as SdkVersion
import           DA.Sdk.Cli.Template.Types
import           DA.Sdk.Cli.Command.Types    (TemplateListFormat(..))
import           DA.Sdk.Prelude              hiding (group)
import qualified DA.Sdk.Pretty               as P
import qualified DA.Sdk.Shell.Tar            as Tar
import           DA.Sdk.Version              (SemVersion (..), showSemVersion)
import qualified Data.ByteString             as BS
import qualified Control.Foldl               as Foldl
import qualified Data.Text.Extended          as T
import           System.Exit                 (exitFailure)
import qualified Turtle
import           Turtle                      ((<.>), (</>))
import           Control.Monad.Trans.Except  (runExceptT)
import           DA.Sdk.Cli.Monad.UserInteraction
import Control.Exception.Safe (throwString)

--------------------------------------------------------------------------------
-- Commands
--------------------------------------------------------------------------------

-- | Publish a template with the given attributes to the given location
templatePublish :: Ty.NameSpace
                -> Ty.TemplateName
                -> Ty.ReleaseLine
                -> CliM ()
templatePublish ns tn@(Ty.TemplateName tnTxt) rl = do
    templatePath <- Turtle.pwd
    askForApproval templatePath $ do
        repoUrls    <- asks (confRepositoryURLs . envConf)
        credentials <- asks (confCredentials . envConf)
        handle      <- Repo.makeBintrayTemplateRepoHandle repoUrls credentials
        let projName        = Turtle.dirname templatePath
            tarName         = projName <.> "tar.gz"
        tempLocation <- getSdkTempDirPath
        let tempTarLocation = L.unwrapFilePathOf tempLocation </> tarName
        result <- liftIO $ Tar.tarGzip templatePath tempTarLocation
        case result of
          Left t   ->
            logError ("Unable to create the tar.gz archive: " <> T.pack t)
          Right () -> do
            content <- liftIO $ BS.readFile $ encodeString tempTarLocation
            tType   <- getTemplateType templatePath
            errOrOk <- liftIO $ templatePublishInternal handle ns tn rl content tType
            case errOrOk of
              Left err -> logError ("Publishing template unsuccessful: " <>
                                        (P.renderPlain $ P.pretty err))
              Right () -> display ("Template " <> tnTxt <> " (release line "
                                    <> T.show rl <>
                                 ") successfully published.")
  where
    askForApproval projP codeToBeConfirmed = do
        isScript <- asks $ confIsScript . envConf
        filesToUpload <- Turtle.fold (Turtle.lstree projP) Foldl.list
        display "The following files will be uploaded as part of the template:"
        forM_ filesToUpload $ \fname -> display ("  " <> (T.pack $ encodeString fname))
        errOrIsConfirmed <- confirm isScript "Do you approve publishing? [y/N]"
        isConfirmed <- either (throwString . show) return errOrIsConfirmed
        when isConfirmed codeToBeConfirmed

templatePublishInternal
    :: Ty.TemplateRepoHandle
    -> Ty.NameSpace
    -> Ty.TemplateName
    -> Ty.ReleaseLine
    -> BS.ByteString
    -> Ty.TemplateType
    -> IO (Either Ty.Error ())
templatePublishInternal h ns tn rl bytes tType = runExceptT $ do
        nextR <- nextRev <$> Ty.getLatestRevision h ns tn rl
        Ty.uploadTemplate h ns tn rl nextR bytes
        Ty.tagTemplate h ns tn rl (tags tType)
  where
    tags tT = [Ty.Tag "template" "true", Ty.Tag "release-line" (T.show rl),
               Ty.Tag "type" (T.show tT)]
    nextRev = maybe (Ty.Revision 1) (\(Ty.Revision rvs) -> Ty.Revision (rvs+1))

createProject :: Maybe Text -> FilePath -> CliM Bool
createProject mbPackageNameName targetPath = do
    mbSdkVersion <- SdkVersion.getDefaultSdkVersion
    sdkVersion <- SdkVersion.requireSdkVersion mbSdkVersion

    -- let tmplt = sdkPath </> "templates" </> (textToPath $ fromMaybe "getting-started" mbBuiltinTName)
    let packageName = fromMaybe "quickstart-java" mbPackageNameName
    templates <- (<>) <$> getBuiltinTemplates onlyAddon sdkVersion <*> getReleaseTemplates onlyAddon sdkVersion
    let mbTemplatePath = preview (traverse . filtered (\(TemplateInfo n _ _ _ _) -> n == packageName) . tiPath) templates
    case mbTemplatePath of
      Just (Just templatePath) ->
        createProjectFrom templatePath targetPath >> return True
      _ ->
        return False
  where
    onlyAddon = Just Ty.Project

-- | Add a template to the current project
templateAdd :: Text -> Maybe FilePath -> CliM Bool
templateAdd packageName mbTargetPath = do
    proj <- requireProject
    sdkVersion <- SdkVersion.getActiveSdkVersion >>= SdkVersion.requireSdkVersion
    templates <- (<>) <$> getBuiltinTemplates onlyAddon sdkVersion <*> getReleaseTemplates onlyAddon sdkVersion
    let mbTemplatePath = preview (traverse . filtered (\(TemplateInfo n _ _ _ _) -> n == packageName) . tiPath) templates
    case mbTemplatePath of
      Just (Just templatePath) ->
        copyTemplateToProject (fromMaybe (projectPath proj) mbTargetPath) templatePath
      _ ->
        return False
  where
    onlyAddon = Just Ty.Addon
    copyTemplateToProject toPath path = do
        daBinDir <- Loc.getDaBinDirPath
        liftIO $ do
            overlappingFiles <- getOverlappingFiles path toPath
            if null overlappingFiles
            then do
                Turtle.cptree path toPath
                display $ "Template '" <> packageName <> "' added to project."
                cleanupInstalledTemplate toPath
                runActivateScript daBinDir path toPath
                return True
            else do
                  display "ERROR: Refusing to add template due to existing files: "
                  display $ T.intercalate ", "
                                 $ map pathToText overlappingFiles
                  exitFailure

listRepos :: Ty.Subject -> CliM ()
listRepos subject = do
    repositoryURLs <- asks (confRepositoryURLs . envConf)
    credentials <- asks (confCredentials . envConf)
    handle <- Repo.makeManagementHandle repositoryURLs credentials
    errOrOk <- Ty._mhListRepos handle subject
    case errOrOk of
        Left err -> logError ("Listing repositories unsuccessful: " <>
                            (P.renderPlain $ P.pretty err))
        Right rs -> forM_ rs $ \(Ty.Repository r) -> display r

checkNamespaceExists :: Ty.NameSpace -> CliM Bool
checkNamespaceExists ns = do
    repoUrls  <- asks (confRepositoryURLs . envConf)
    credentials <- asks (confCredentials . envConf)
    handle      <- Repo.makeBintrayTemplateRepoHandle repoUrls credentials
    nssOrErr    <- liftIO $ runExceptT $ Ty.listNameSpaces handle
    case nssOrErr of
      Left err  -> logError (P.renderPlain $ P.pretty err) >> return False
      Right nss -> return (ns `elem` nss)

listUsableTemplates :: TemplateListFormat -> Maybe Ty.TemplateType -> CliM ()
listUsableTemplates = \case
    TemplateListAsTable  -> listUsableTemplateTable
    TemplateListAsTuples -> listUsableTemplateTuples

listUsableTemplateTable :: Maybe Ty.TemplateType -> CliM ()
listUsableTemplateTable mty = listUsableTemplates' mty resPrintAll errPrint
  where
    resPrintAll l = display $ P.table $ map fmt l
    fmt (Left ti) = [tiName ti, "Built-in template for version: " <> tiVersion ti]
    fmt (Right t) = [Ty.templateName t, Ty.templateDesc t]
    errPrint mbTp' sdkVsn = display ("There are no " <> tpText mbTp' <>
                                   " templates for SDK version " <>
                                   showSemVersion sdkVsn)
    tpText mbTp' = maybe "" T.show mbTp'

listUsableTemplateTuples :: Maybe Ty.TemplateType -> CliM ()
listUsableTemplateTuples mty = listUsableTemplates' mty resPrintAll errPrint
  where
    resPrintAll = mapM_ resPrint
    resPrint (Left ti) = display (tiName ti <> " [built-in]")
    resPrint (Right r) = let Ty.NameSpace n = Ty.templateNameSpace r
        in display (n <> "/" <> Ty.templateName r <> "/" <> Ty.templateRelLine r)
    errPrint _ _ = return ()

listUsableTemplates'
    :: Maybe Ty.TemplateType
    -> ([Either TemplateInfo Ty.Template] -> CliM ())
    -> (Maybe Ty.TemplateType -> SemVersion -> CliM ())
    -> CliM ()
listUsableTemplates' mbTp resPrint errPrint = do
    sdkVsn <- SdkVersion.getActiveSdkVersion >>= SdkVersion.requireSdkVersion
    builtIn <- getBuiltinTemplates mbTp sdkVsn
    released <- getReleaseTemplates mbTp sdkVsn
    let ts = map Left (builtIn ++ released)
    if null ts
      then errPrint mbTp sdkVsn
      else resPrint ts

listUsableTemplatesInternal -- this function does not succeed without bintray credentials.
    :: Ty.TemplateRepoHandle
    -> Maybe Ty.TemplateType
    -> [Ty.NameSpace]
    -> SemVersion
    -> IO (Either Ty.Error [Ty.Template])
listUsableTemplatesInternal handle mbTemplType nss sdkVsn = runExceptT $ do
    templates <- mapM (\ns -> Ty.searchTemplates handle ns tags) nss
    return $ concat templates
  where
    (exactVsn, relLine) = Ty.toExactVsnAndRelLineTagTxt sdkVsn
    tags = [Ty.Tag "template" "true",
            -- Note that both release line and exactly matching
            -- version need to be taken care of
            Ty.Tag "release-line" relLine,
            Ty.Tag "release-line" exactVsn] ++
            maybe []
                   (\tt -> [Ty.Tag "type" (T.show tt)])
                   mbTemplType

getTemplateInfo :: Ty.NameSpace -> Ty.TemplateName -> Ty.ReleaseLine -> CliM ()
getTemplateInfo ns tn rl = do
    repoUrls    <- asks (confRepositoryURLs . envConf)
    credentials <- asks (confCredentials . envConf)
    handle      <- Repo.makeBintrayTemplateRepoHandle repoUrls credentials
    errOrInfo   <- liftIO $ runExceptT $ Ty.getInfo handle ns tn rl
    case errOrInfo of
      Right (Just t) -> display $ format t
      Right Nothing  -> logError "No such template."
      Left  err      -> logError ("Error when getting template info: " <>
                                  (P.renderPlain $ P.pretty err))
  where
    format t = "Template:\t"     <> Ty.templateName t <> "\n" <>
               "Description:\t"  <> Ty.templateDesc t <> "\n" <>
               "Revisions:\t"    <> (formatRevs $ Ty.templateRevs t) <> "\n" <>
               "Tags:\t\t"       <> (formatTags $ Ty.templateTags t) <> "\n" <>
               "Namespace:\t"    <> nSpace t <> "\n" <>
               "Release line:\t" <> Ty.templateRelLine t
    formatRevs = T.intercalate ", " . map T.show
    formatTags = T.intercalate ", " . map formatTag
    formatTag (Ty.Tag n v) = n <> "=" <> v
    nSpace t = let (Ty.NameSpace ns') = Ty.templateNameSpace t in ns'

getTemplateType :: FilePath -> CliM Ty.TemplateType
getTemplateType templatePath = do
    isProjTemplate <- Turtle.testfile $ templatePath </> "da.yaml"
    if isProjTemplate
    then return Ty.Project
    else return Ty.Addon
--------------------------------------------------------------------------------
-- Internals
--------------------------------------------------------------------------------

-- | Retrieve the list of built-in templates in a SDK release
getBuiltinTemplates :: Maybe Ty.TemplateType -> SemVersion -> CliM [TemplateInfo]
getBuiltinTemplates mbTp sdkVersion = do
    templatesDir <- Loc.getSdkPackageTemplateDirPath sdkVersion
    entries <- LT.ls templatesDir
    let prog entry = do
          isDir <- Turtle.testdir entry
          guard isDir
          let tName = pathToText $ Turtle.basename entry
          pure (TemplateInfo tName (Just entry)
                (showSemVersion sdkVersion { _svPreRelease = Just "SDK" })
                "Built-in template"
                (if tName `elem` builtInProjectTemplateNames then Ty.Project else Ty.Addon))
    liftIO
        $ fmap (maybe id (\tp -> filter (\t -> tp == tiType t)) mbTp)
        $ mapM prog entries

-- | Retrieve the list of templates in a SDK release
getReleaseTemplates :: Maybe Ty.TemplateType -> SemVersion -> CliM [TemplateInfo]
getReleaseTemplates mbTp sdkVersion = do
    errOrMetadata <- getSdkMetadata sdkVersion
    metadata <- either (throwString . show . P.renderPlain . P.pretty) return errOrMetadata
    packagesPath <- Loc.getPackagesPath
    let packages = Metadata.allPackages metadata
    pure $ ifoldMap
             (\name (group, package) ->
                 let version = Metadata._gVersion group
                 in if Metadata._pTemplate package
                    then maybe
                            id
                            (\tp -> filter (\t -> tp == tiType t))
                            mbTp
                            [TemplateInfo
                                name
                                (Just $ L.unwrapFilePathOf packagesPath </> textToPath name </> textToPath (showSemVersion version)) -- TODO improve
                                (showSemVersion version)
                                ("Template released with SDK " <> showSemVersion sdkVersion)
                                (if name `elem` builtInProjectTemplateNames then Ty.Project else Ty.Addon)
                            ]
                    else [])
             packages
