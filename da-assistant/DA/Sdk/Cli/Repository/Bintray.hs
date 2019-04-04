-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE TypeOperators     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -Wno-missing-signatures #-} -- because of servant-generated client functions

module DA.Sdk.Cli.Repository.Bintray
    ( PackageVersion(..)
    , daSubject
    , daRepository
    , daExperimentalRepository
    , makeBintrayHandle

    , makeBintrayManagementHandle
    , makeBintrayRequestHandle

    , makeBintrayTemplateRepoHandleWithRequestLayer

    , sdkAssistantPackage
    , sdkAssistantAltPackage

      -- Tagging
    , versionMatchesTags

      -- Exported to enable testing
    , listPublishedTemplates
    , listRepos
    , downloadPackagePath
    , latestToVersioned
    ) where

import           Control.Lens                      (over, _Left)
import           Control.Monad.Trans.Except        ( ExceptT (..), runExceptT, throwE
                                                   , withExceptT
                                                   )
import           Control.Monad.Extra               (firstJustM)
import qualified DA.Sdk.Cli.Conf                   as Conf
import qualified DA.Sdk.Cli.Metadata               as Metadata
import           DA.Sdk.Cli.OS                     (OS (..))
import           DA.Sdk.Cli.Conf.Types             (RepositoryURLs(..))
import qualified DA.Sdk.Cli.Repository.Types       as Ty
import           DA.Sdk.Cli.Repository.Bintray.API
import           DA.Sdk.Prelude
import qualified DA.Sdk.Version                    as V
import           Data.Aeson                        as Aeson
import           Data.Aeson.Types                  (typeMismatch)
import qualified Data.ByteString                   as BS
import qualified Data.Text.Extended                as T
import           Text.Read                         (readMaybe)
import qualified Data.Text.Encoding                as T.Encoding
import qualified Data.Set                          as Set
import qualified Pipes.HTTP                        as HTTP
import           Turtle                            hiding (Handle, err, find, sortBy)
import qualified DA.Sdk.Version                    as Version
import           Data.Proxy
import qualified Network.HTTP.Client.TLS           as Client
import           Servant.API
import           Servant.Client                    hiding (manager)
import           Network.HTTP.Types.Status
import           Control.Monad.Error.Class         hiding (Error)
import qualified Data.Map                          as MP
import           DA.Sdk.Cli.Command                (groupByAttrName)
import           Control.Arrow                     ((&&&))
import qualified Data.List.Split                   as Spl
import qualified Data.List                         as L

newtype PackageVersion v = PackageVersion
    { packageVersionVersion :: v
    }

--------------------------------------------------------------------------------
-- Handle
--------------------------------------------------------------------------------

makeBintrayHandle ::
       MonadIO m
    => Ty.RequestHandle
    -> RepositoryURLs
    -> Ty.Credentials
    -> m (Ty.Handle m)
makeBintrayHandle reqH repositoryURLs credentials = do
    let apiBaseUrl         = repositoryAPIURL repositoryURLs
        bintrayDownloadUrl = repositoryDownloadURL repositoryURLs
    manager <- liftIO $ HTTP.newManager HTTP.tlsManagerSettings
    pure $ Ty.Handle
        { Ty.hLatestSdkVersion =
            lookupLatestSdkVersion apiBaseUrl credentials manager
        , Ty.hDownloadPackage =
            downloadPackage bintrayDownloadUrl credentials manager
        , Ty.hLatestPackageVersion = \package ->
            lookupLatestPackageVersion reqH credentials package
        , Ty.hDownloadSdkAssistant = \package ->
            downloadSdkAssistant bintrayDownloadUrl credentials manager package
        , Ty.hCheckCredentials = checkCredentials reqH credentials
        , Ty.hGetPackageTags = mapErrorM . getPackageTags apiBaseUrl credentials manager
        }

makeBintrayManagementHandle ::
       MonadIO m
    => Ty.RequestHandle
    -> BaseUrl
    -> Ty.Credentials
    -> m Ty.ManagementHandle
makeBintrayManagementHandle reqH apiBaseUrl credentials =
    pure $ Ty.ManagementHandle
        { Ty._mhListReleases = listReleases apiBaseUrl credentials
        , Ty._mhGetReleaseTags = \version ->
            mapErrorM $ getReleaseTags apiBaseUrl credentials version
        , Ty._mhSetReleaseTags = \version tags ->
            mapErrorM $ setReleaseTags apiBaseUrl credentials version tags
        , Ty._mhAddReleaseTags = \version newTags ->
            mapErrorM $ addReleaseTags apiBaseUrl credentials version newTags
        , Ty._mhRemoveReleaseTags = \version tagsToRemove ->
            mapErrorM $ removeReleaseTags apiBaseUrl credentials version tagsToRemove
        , Ty._mhListPublishedTemplates = \subject repo ->
                listPublishedTemplates reqH subject repo credentials
        , Ty._mhListRepos = \subject -> listRepos reqH subject credentials
        }

makeBintrayRequestHandle :: MonadIO m => BaseUrl -> ClientEnv -> m Ty.RequestHandle
makeBintrayRequestHandle dlUrl clientEnv@(ClientEnv mgr _apiBaseUrl _) = do
    pure $ Ty.RequestHandle
        { Ty.hReqCreatePackage = \sub repo desc auth ->
            toEVoid (createPackageReq sub repo desc auth)
        , Ty.hReqSetAttributes = \sub repo pkg attrs auth ->
            toEVoid (updatePackageAttributesReq sub repo pkg attrs auth)
        , Ty.hReqUploadPackage = \sub repo pkg v fname content publish auth ->
            toEVoid (uploadReleaseReq sub repo pkg v fname publish content auth)
        , Ty.hReqSearchPkgsWithAtts = \sub repo attrs auth ->
            toE id (attributeSearchReq sub repo attrs (Just Ty.Yes) auth)
        , Ty.hListRepos = \sub auth ->
            toE id (getRepositoriesReq sub auth)
        , Ty.hSearchPkgVersionFiles = \sub repo pkg v incl auth ->
            toE id (getVersionFilesReq sub repo pkg v incl auth)
        , Ty.hGetPackageInfoReq = \sub repo pkg auth ->
            toE id (getPackageReq sub repo pkg (Just Ty.Yes) auth)
        , Ty.hGetLatestVersionReq = \sub repo pkg auth ->
            toE id (getLatestVersionReq sub repo pkg auth)
        , Ty.hDownloadPackageReq = \sub repo (Ty.PackageName p) vsn targetDir auth -> do
            let downloadablePkg    = Ty.DownloadablePackage Nothing (Metadata.PackagingTarGz Metadata.TarGz)
                                            (Ty.Package p []) vsn -- package group is empty []
                targetName         = downloadPackagePath downloadablePkg
                targetPath         = pathToString (targetDir </> (filename $ textToPath $ T.intercalate "/" targetName))
            bs <- toEdl id (downloadReq sub repo targetName auth)
            liftIO $ BS.writeFile targetPath bs
            return $ stringToPath targetPath
        }
  where
    -- toE :: ClientM r -> ExceptT Ty.Error IO ()
    toEVoid   = toE void
    toE = toE' clientEnv
    toEdl = toE' (ClientEnv mgr dlUrl Nothing)
    toE' cE tr op = do
        errOrRes <- liftIO $ runClientM op cE
        either throwE return $ tr $ mapError errOrRes

makeBintrayTemplateRepoHandleWithRequestLayer ::
       MonadIO m
    => Ty.Credentials
    -> Ty.Subject
    -> Ty.RequestHandle
    -> m Ty.TemplateRepoHandle
makeBintrayTemplateRepoHandleWithRequestLayer creds subject reqH =
    return $ Ty.TemplateRepoHandle
    { Ty.uploadTemplate = \ns tname rl rvs@(Ty.Revision r) file -> do
        let v        = T.show r
            pTxt     = pkgNameTxt tname rl
            filePath = [pTxt, v, pTxt <> "-" <> v <> ".tar.gz"]
            pkgDesc  = Ty.PackageDescriptor pTxt ("Template package: " <> pTxt)
        -- if package is already created, we should not fail
        _ <- catchErrCode 409 () $ Ty.hReqCreatePackage reqH subject (toRepo ns)
                                        pkgDesc creds'
        -- uploaded packages auto-published by publish=1 (Just Yes)
        Ty.hReqUploadPackage reqH subject (toRepo ns) (pkgName tname rl)
          (toGenVsn rvs) filePath file (Just Ty.Yes) creds'
    , Ty.downloadTemplate = \ns tname rl rvs targetDir ->
        Ty.hDownloadPackageReq reqH subject (toRepo ns) (pkgName tname rl)
          (toGenVsn rvs) targetDir creds'
    , Ty.getLatestRevision = \ns tname rl ->
        catchErrCode 404 Nothing $ fmap toRvsn2 $
         Ty.hGetLatestVersionReq reqH subject
         (toRepo ns) (pkgName tname rl) creds'
    , Ty.tagTemplate = \ns tname rl tags ->
        Ty.hReqSetAttributes reqH subject (toRepo ns)
                (pkgName tname rl) (toAttrs tags) creds'
    , Ty.searchTemplates = \ns tags -> do
        fmap (toTemplates ns) $ Ty.hReqSearchPkgsWithAtts reqH subject
            (toRepo ns) (toSearchAttrs tags) creds'
    , Ty.getInfo = \ns tname rl ->
        catchErrCode 404 Nothing $ fmap (Just . toTemplate ns) $
          Ty.hGetPackageInfoReq reqH subject
          (toRepo ns) (pkgName tname rl) creds'
    , Ty.listNameSpaces =
        fmap toNameSpaces $ Ty.hListRepos reqH subject creds'
    }
  where
    creds' = credentialsToBasicAuthData creds
    toRepo (Ty.NameSpace ns) = Ty.Repository ns
    pkgNameTxt (Ty.TemplateName tn) rl =
        T.intercalate "-" [tn, T.show rl]
    pkgName tn rl = Ty.PackageName $ pkgNameTxt tn rl
    toAttrs             = map toAttr
    toAttr (Ty.Tag k v) = Ty.Attribute k $ Ty.AVText [v]
    toSearchAttrs       = map Ty.SearchAttribute .
                            groupByAttrName .
                                map (\(Ty.Tag k v) -> (k, v))
    fromAttrs mp        = map fromAttr $ MP.toList mp
    fromAttr (k, v)     = Ty.Tag k (fromMaybe "" $ headMay v)
    toTemplates ns      = map (\sAttr -> let Ty.PackageName nameTxt = Ty.infoPkgName sAttr
                                             (tn, rl) = toTNameRl nameTxt
                                         in Ty.Template (T.pack tn) (Ty.infoDescription sAttr)
                                             (catMaybes $ toRvsn1 <$> Ty.infoVersions sAttr)
                                             (fromAttrs $ Ty.infoAttributes sAttr)
                                             ns (T.pack rl))
    toTemplate ns btPkg = let Ty.PackageName nameTxt = Ty._bpPackageName btPkg
                              (tn, rl) = toTNameRl nameTxt
                          in Ty.Template (T.pack tn) (Ty._bpDescription btPkg)
                                (catMaybes $ toRvsn1 <$> Ty._bpVersions btPkg)
                                (fromAttrs $ Ty._bpAttributes btPkg)
                                ns (T.pack rl)
    toTNameRl pkText    = brkOnLast '-' pkText
    brkOnLast c t = (L.intercalate [c] . init &&& last) $ Spl.splitOn [c] (T.unpack t)
    toRvsn1 (Ty.GenericVersion vTxt)                    = fromVersionTxt vTxt
    toRvsn2 (Ty.LatestVersion (Ty.GenericVersion vTxt)) = fromVersionTxt vTxt
    fromVersionTxt t = Ty.Revision <$> (readMaybe $ T.unpack t)
    toGenVsn (Ty.Revision r) = Ty.GenericVersion $ T.show r
    toNameSpaces = map toNameSpace
    toNameSpace (Ty.Repository n) = Ty.NameSpace n
    catchErrCode code ret = flip catchError
                -- If package was already created then there is no error.
                (\case err@(Ty.ErrorRequest (Ty.ServantError (FailureResponse rsp))) ->
                                if (statusCode $ responseStatusCode rsp) == code
                                then return ret
                                else throwError err
                       other -> throwError other)

mapErrorM ::
        MonadIO m
    => m (Either ServantError c)
    -> m (Either Ty.Error c)
mapErrorM = fmap mapError

mapError :: Either ServantError c -> Either Ty.Error c
mapError = over _Left (Ty.ErrorRequest . Ty.ServantError)

--------------------------------------------------------------------------------
-- Version
--------------------------------------------------------------------------------

lookupLatestPackageVersion ::
       (MonadIO m, V.Versioned v)
    => Ty.RequestHandle
    -> Ty.Credentials
    -> Ty.Package
    -> m (Either Ty.Error v)
lookupLatestPackageVersion handle creds (Ty.Package pId _group) = liftIO $ runExceptT $ do
    vsn <- Ty.hGetLatestVersionReq handle daSubject daRepository (Ty.PackageName pId)
            (credentialsToBasicAuthData creds)
    ExceptT $ pure $ latestToVersioned vsn


latestToVersioned :: (V.Versioned v) => Ty.LatestVersion -> Either Ty.Error v
latestToVersioned (Ty.LatestVersion (Ty.GenericVersion v)) =
    case eitherDecodeStrict' (T.Encoding.encodeUtf8 v') of
        Left er   -> Left $ Ty.ErrorJSONParse $ T.pack er
        Right ver -> Right ver
  where
    v' = if (not $ T.null v) && (T.head v /= '"')
        then "\"" <> v <> "\"" -- only "\"string\"" is a valid JSON string
        else v

-- | The latest available SDK version changes depending on the included and
-- excluded tags someone has set.
lookupLatestSdkVersion ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> HTTP.Manager
    -> Ty.TagsFilter
    -> m (Either Ty.Error Version.SemVersion)
lookupLatestSdkVersion apiBaseUrl creds manager tagsFilter =
    runExceptT $ do
        package <- withExceptT (Ty.ErrorRequest . Ty.ServantError) $ ExceptT $ liftIO $ runClientM
            (getPackageReq
                daSubject
                daRepository
                sdkPackageName
                (Just Ty.Yes)
                (credentialsToBasicAuthData creds))
            (ClientEnv manager apiBaseUrl Nothing)
        -- Versions sorted newest to oldest.
        versions <- sortBy (flip compare) <$> traverse parseSemVersion (Ty._bpVersions package)
        mbVersion <- firstJustM isValidVersion versions
        maybe (throwE Ty.ErrorNoVersionFound) pure mbVersion
  where
    parseSemVersion (Ty.GenericVersion v) = maybe (throwE $ Ty.ErrorVersionParse v) pure $ Version.parseSemVersion v
    isValidVersion ::
           MonadIO m
        => Version.SemVersion
        -> ExceptT Ty.Error m (Maybe Version.SemVersion)
    isValidVersion version = do
        let getReleaseTags' = getReleaseTags apiBaseUrl creds (Ty.semToGenericVersion version)
        tags <- withExceptT (Ty.ErrorRequest . Ty.ServantError) $ ExceptT getReleaseTags'
        pure $
            if versionMatchesTags tagsFilter tags
            then
                Just version
            else
                Nothing

-- | A version matches your tags if either
-- - Your included tags contain the wildcard tag.
-- - The version on the server matches at least one tag in your included list,
--   but none in your excluded list.
versionMatchesTags :: Ty.TagsFilter -> Set.Set Text -> Bool
versionMatchesTags (Ty.TagsFilter includedTags excludedTags) tags =
    matchesAtLeastOneOfIncluded && matchesNoneOfExcluded
  where
    containsWildcard = Set.member Conf.wildcardTag

    containsTags providedTags tagsToMatch =
        not $ Set.null $ Set.intersection providedTags tagsToMatch

    matches providedTags tagsToMatch =
        containsWildcard providedTags || containsTags providedTags tagsToMatch

    matchesAtLeastOneOfIncluded = matches includedTags tags

    matchesNoneOfExcluded = not $ matches excludedTags tags

--------------------------------------------------------------------------------
-- Publish
--------------------------------------------------------------------------------

isTemplateAttribute :: Ty.Attribute
isTemplateAttribute = Ty.Attribute "template" (Ty.AVText ["true"])

listPublishedTemplates ::
    MonadIO m
    => Ty.RequestHandle
    -> Ty.Subject
    -> Ty.Repository
    -> Ty.Credentials
    -> m (Either Ty.Error [(Ty.PackageName, Text, Ty.GenericVersion)])
listPublishedTemplates reqH subject repository credentials =
                liftIO $ runExceptT listPubTemp
  where
    listPubTemp = do
        pkgInfos <- Ty.hReqSearchPkgsWithAtts reqH subject repository
                        [Ty.SearchAttribute isTemplateAttribute]
                        (credentialsToBasicAuthData credentials)
        return $ concatMap (\pkgInfo ->
                            map (Ty.infoPkgName pkgInfo,
                                 Ty.infoDescription pkgInfo, )
                                (Ty.infoVersions pkgInfo))
                           pkgInfos

listRepos ::
    MonadIO m
    => Ty.RequestHandle
    -> Ty.Subject
    -> Ty.Credentials
    -> m (Either Ty.Error [Ty.Repository])
listRepos reqH subject credentials = liftIO $ runExceptT $
        Ty.hListRepos reqH subject (credentialsToBasicAuthData credentials)

--------------------------------------------------------------------------------
-- Download
--------------------------------------------------------------------------------

downloadPackagePath ::
       V.Versioned v
    => Ty.DownloadablePackage v
    -> [Text]
downloadPackagePath dp@(Ty.DownloadablePackage _ _ (Ty.Package aid agroup) version) =
    (if null agroup then id else (agroup ++)) [aid, V.formatVersion version, downloadPackageFileName dp]

downloadPackageFileName ::
        V.Versioned v
    => Ty.DownloadablePackage v
    -> Text
downloadPackageFileName (Ty.DownloadablePackage mbClassifier packaging (Ty.Package aid _) version) =
    format (s % "-" % s % s % s) aid formattedVersion classifier fileEnding
  where
    classifier       = maybe "" ("-" <>) mbClassifier
    fileEnding       = Metadata.packagingToFileEnding packaging
    formattedVersion = V.formatVersion version

-- | Download a package from bintray.
downloadPackage ::
       (MonadIO m, V.Versioned v)
    => BaseUrl
    -> Ty.Credentials
    -> HTTP.Manager
    -> Ty.Repository
    -> Ty.DownloadablePackage v
    -> FilePath
    -> m (Either Ty.Error FilePath)
downloadPackage bintrayDownloadUrl creds mgr repo downloadablePackage downloadDir = do
    let targetName = downloadPackagePath downloadablePackage
        targetFile = textToPath $ last targetName
        targetPath = pathToString (downloadDir </> targetFile)
        clEnv      = ClientEnv mgr bintrayDownloadUrl Nothing
        auth       = credentialsToBasicAuthData creds
    errOrBs <- liftIO $ runClientM (downloadReq daSubject repo targetName auth) clEnv
    case errOrBs of
      Left err -> do
        let targetNameStr = T.unpack $ T.intercalate "/" targetName
            ctx           = targetNameStr <> " (" <> showBaseUrl bintrayDownloadUrl <> ")"
        return $ Left $ Ty.ErrorDownload $ Ty.ServantErrorWithContext err ctx
      Right bs -> do
        liftIO $ BS.writeFile targetPath bs
        return $ Right $ stringToPath targetPath

downloadSdkAssistant ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> HTTP.Manager
    -> Ty.Package
    -> OS
    -> V.BuildVersion
    -> FilePath
    -> m (Either Ty.Error FilePath)
downloadSdkAssistant bintrayDownloadUrl credentials manager package os version downloadDir = liftIO $ runExceptT $ do
    classifier <- case os of
        Linux             -> pure "linux"
        MacOS             -> pure "osx"
        Windows           -> pure "win"
        Unknown unknownOs -> throwE (Ty.ErrorUnknownOS unknownOs)
    ExceptT $ downloadPackage bintrayDownloadUrl credentials manager daRepository (downloadablePackage classifier) downloadDir
  where
    downloadablePackage classifier = Ty.DownloadablePackage
        { Ty._dpClassifier = Just classifier
        , Ty._dpPackaging = Metadata.PackagingExtension "run"
        , Ty._dpPackage = package
        , Ty._dpVersion = version
        }


checkCredentials ::
       MonadIO m
    => Ty.RequestHandle
    -> Ty.Credentials
    -> m (Either Ty.Error ())
checkCredentials handle creds = liftIO $ do
    errOrPkgs <- runExceptT $ Ty.hReqSearchPkgsWithAtts handle daSubject daRepository fakeAttrL
                                        (credentialsToBasicAuthData creds)
    return $ void $ over _Left (const $ Ty.ErrorRequest Ty.RequestErrorUnauthorized) errOrPkgs
  where
    fakeAttrL = [Ty.SearchAttribute (Ty.Attribute "check-creds-attr" (Ty.AVText ["check-creds"]))]

--------------------------------------------------------------------------------
-- Special Packages
--------------------------------------------------------------------------------

sdkAssistantPackage :: Ty.Package
sdkAssistantPackage = Ty.Package
    { Ty._pId = "da-cli"
    , Ty._pGroup = ["com", "digitalasset"]
    }

sdkAssistantAltPackage :: Text -> Ty.Package
sdkAssistantAltPackage channel = Ty.Package
    { Ty._pId = "da-cli-" <> channel
    , Ty._pGroup = ["com", "digitalasset"]
    }

--------------------------------------------------------------------------------
-- Management
--------------------------------------------------------------------------------

daSubject :: Ty.Subject
daSubject = Ty.Subject "digitalassetsdk"

daRepository :: Ty.Repository
daRepository = Ty.Repository "DigitalAssetSDK"

-- | This repo is used for storage of experimental releases
-- published by DA employees.
daExperimentalRepository :: Ty.Repository
daExperimentalRepository = Ty.Repository "DigitalAssetSDK-experimental"

sdkPackageName :: Ty.PackageName
sdkPackageName = Ty.PackageName "sdk"

-- | The name of the attribute where the release tags will be saved in.
tagAttributeName :: Text
tagAttributeName = "tags"

-- | Find the attribute where the tags are saved in and return
-- the saved tags as a 'Set.Set'.
attributesToTags :: [Ty.Attribute] -> Set.Set Text
attributesToTags =
    maybe
        Set.empty
        (Set.fromList . unAVText . Ty._aValue) . find ((== tagAttributeName) . Ty._aName)
  where
    unAVText :: Ty.AttributeValue -> [Text]
    unAVText = \case
        Ty.AVText ts -> ts

-- | Convert our internal credentials representation to one
-- servant can use for basic auth.
credentialsToBasicAuthData :: Ty.Credentials -> Maybe BasicAuthData
credentialsToBasicAuthData Ty.Credentials{..} = do
    Ty.BintrayCredentials{..} <- bintrayCredentials
    Just $ BasicAuthData (T.Encoding.encodeUtf8 bintrayUsername)
                         (T.Encoding.encodeUtf8 bintrayKey)

bintrayManagementAPIWithAuth :: Proxy BintrayManagementAPIWithAuth
bintrayManagementAPIWithAuth = Proxy

bintrayManagementAPINoAuth :: Proxy BintrayManagementAPINoAuth
bintrayManagementAPINoAuth = Proxy

(getReleaseAttributesReqWithAuth
    :<|> getPackageAttributesReqWithAuth
    :<|> updateReleaseAttributesReqWithAuth
    :<|> updatePackageAttributesReqWithAuth
    :<|> attributeSearchReqWithAuth
    :<|> getPackageReqWithAuth
    :<|> createPackageReqWithAuth
    :<|> uploadReleaseReqWithAuth
    :<|> getRepositoriesReqWithAuth
    :<|> getVersionFilesReqWithAuth
    :<|> getLatestVersionReqWithAuth) =
    client bintrayManagementAPIWithAuth

(getReleaseAttributesReqNoAuth
    :<|> getPackageAttributesReqNoAuth
    :<|> updateReleaseAttributesReqNoAuth
    :<|> updatePackageAttributesReqNoAuth
    :<|> attributeSearchReqNoAuth
    :<|> getPackageReqNoAuth
    :<|> createPackageReqNoAuth
    :<|> uploadReleaseReqNoAuth
    :<|> getRepositoriesReqNoAuth
    :<|> getVersionFilesReqNoAuth
    :<|> getLatestVersionReqNoAuth) =
    client bintrayManagementAPINoAuth

getReleaseAttributesReq :: Ty.Subject
                        -> Ty.Repository
                        -> Ty.PackageName
                        -> Ty.GenericVersion
                        -> Maybe BasicAuthData
                        -> ClientM [Ty.Attribute]
getReleaseAttributesReq subj repo pkg vers = \case
    Nothing -> getReleaseAttributesReqNoAuth subj repo pkg vers
    Just auth -> getReleaseAttributesReqWithAuth auth subj repo pkg vers

getPackageAttributesReq :: Ty.Subject
                        -> Ty.Repository
                        -> Ty.PackageName
                        -> Maybe BasicAuthData
                        -> ClientM [Ty.Attribute]
getPackageAttributesReq subj repo pkg = \case
    Nothing -> getPackageAttributesReqNoAuth subj repo pkg
    Just auth -> getPackageAttributesReqWithAuth auth subj repo pkg

updateReleaseAttributesReq :: Ty.Subject
                           -> Ty.Repository
                           -> Ty.PackageName
                           -> Ty.GenericVersion
                           -> [Ty.Attribute]
                           -> Maybe BasicAuthData
                           -> ClientM [Ty.Attribute]
updateReleaseAttributesReq subj repo pkg vers attrs = \case
    Nothing -> updateReleaseAttributesReqNoAuth subj repo pkg vers attrs
    Just auth -> updateReleaseAttributesReqWithAuth auth subj repo pkg vers attrs

updatePackageAttributesReq :: Ty.Subject
                           -> Ty.Repository
                           -> Ty.PackageName
                           -> [Ty.Attribute]
                           -> Maybe BasicAuthData
                           -> ClientM NoContent
updatePackageAttributesReq subj repo pkg attrs = \case
    Nothing -> updatePackageAttributesReqNoAuth subj repo pkg attrs
    Just auth -> updatePackageAttributesReqWithAuth auth subj repo pkg attrs

attributeSearchReq :: Ty.Subject
                   -> Ty.Repository
                   -> [Ty.SearchAttribute]
                   -> Maybe Ty.BitFlag
                   -> Maybe BasicAuthData
                   -> ClientM [Ty.PackageInfo]
attributeSearchReq subj repo attrs bitflag = \case
    Nothing -> attributeSearchReqNoAuth subj repo attrs bitflag
    Just auth -> attributeSearchReqWithAuth auth subj repo attrs bitflag

getPackageReq :: Ty.Subject
              -> Ty.Repository
              -> Ty.PackageName
              -> Maybe Ty.BitFlag
              -> Maybe BasicAuthData
              -> ClientM Ty.BintrayPackage
getPackageReq subj repo pkg bitflag = \case
    Nothing -> getPackageReqNoAuth subj repo pkg bitflag
    Just auth -> getPackageReqWithAuth auth subj repo pkg bitflag

createPackageReq :: Ty.Subject
                 -> Ty.Repository
                 -> Ty.PackageDescriptor
                 -> Maybe BasicAuthData
                 -> ClientM NoContent
createPackageReq subj repo pkgdesc = \case
    Nothing -> createPackageReqNoAuth subj repo pkgdesc
    Just auth -> createPackageReqWithAuth auth subj repo pkgdesc

uploadReleaseReq :: Ty.Subject
                 -> Ty.Repository
                 -> Ty.PackageName
                 -> Ty.GenericVersion
                 -> [Text]
                 -> Maybe Ty.BitFlag
                 -> BS.ByteString
                 -> Maybe BasicAuthData
                 -> ClientM NoContent
uploadReleaseReq subj repo pkg vers a b c = \case
    Nothing -> uploadReleaseReqNoAuth subj repo pkg vers a b c
    Just auth -> uploadReleaseReqWithAuth auth subj repo pkg vers a b c

getRepositoriesReq :: Ty.Subject
                   -> Maybe BasicAuthData
                   -> ClientM [Ty.Repository]
getRepositoriesReq subj = \case
    Nothing -> getRepositoriesReqNoAuth subj
    Just auth -> getRepositoriesReqWithAuth auth subj

getVersionFilesReq :: Ty.Subject
                   -> Ty.Repository
                   -> Ty.PackageName
                   -> Ty.GenericVersion
                   -> Maybe Ty.BitFlag
                   -> Maybe BasicAuthData
                   -> ClientM [Ty.PkgFileInfo]
getVersionFilesReq subj repo pkg vers bitflag = \case
    Nothing -> getVersionFilesReqNoAuth subj repo pkg vers bitflag
    Just auth -> getVersionFilesReqWithAuth auth subj repo pkg vers bitflag

getLatestVersionReq :: Ty.Subject
                    -> Ty.Repository
                    -> Ty.PackageName
                    -> Maybe BasicAuthData
                    -> ClientM Ty.LatestVersion
getLatestVersionReq subj repo pkg = \case
    Nothing -> getLatestVersionReqNoAuth subj repo pkg
    Just auth -> getLatestVersionReqWithAuth auth subj repo pkg

bintrayDownloadAPIWithAuth :: Proxy BintrayDownloadAPIWithAuth
bintrayDownloadAPIWithAuth = Proxy

bintrayDownloadAPINoAuth :: Proxy BintrayDownloadAPINoAuth
bintrayDownloadAPINoAuth = Proxy

downloadReqNoAuth = client bintrayDownloadAPINoAuth
downloadReqWithAuth = client bintrayDownloadAPIWithAuth

downloadReq ::
       Ty.Subject
    -> Ty.Repository
    -> [Text]
    -> Maybe BasicAuthData
    -> ClientM BS.ByteString

downloadReq subj repo m = \case
    Nothing -> downloadReqNoAuth subj repo m
    Just auth -> downloadReqWithAuth auth subj repo m


getReleaseTags ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> Ty.GenericVersion
    -> m (Either ServantError (Set.Set Text))
getReleaseTags apiBaseUrl credentials version = do
    manager <- liftIO Client.newTlsManager
    attributes <- liftIO $ runClientM
        (getReleaseAttributesReq
            daSubject
            daRepository
            sdkPackageName
            version
            (credentialsToBasicAuthData credentials))
        (ClientEnv manager apiBaseUrl Nothing)
    pure $ attributesToTags <$> attributes

getPackageTags ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> HTTP.Manager
    -> Ty.PackageName
    -> m (Either ServantError (Set.Set Text))
getPackageTags apiBaseUrl credentials manager package = do
    -- manager <- liftIO Client.newTlsManager
    attributes <- liftIO $ runClientM
        (getPackageAttributesReq
            daSubject
            daRepository
            package
            (credentialsToBasicAuthData credentials))
        (ClientEnv manager apiBaseUrl Nothing)
    pure $ attributesToTags <$> attributes

setReleaseTags ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> Ty.GenericVersion
    -> Set.Set Text
    -> m (Either ServantError (Set.Set Text))
setReleaseTags apiBaseUrl credentials version tags = do
    manager <- liftIO Client.newTlsManager
    attributes <- liftIO $ runClientM
        (updateReleaseAttributesReq
            daSubject
            daRepository
            sdkPackageName
            version
            [Ty.Attribute tagAttributeName (Ty.AVText $ Set.toList tags)]
            (credentialsToBasicAuthData credentials))
        (ClientEnv manager apiBaseUrl Nothing)
    pure $ attributesToTags <$> attributes

addReleaseTags ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> Ty.GenericVersion
    -> Set.Set Text
    -> m (Either ServantError (Set.Set Text))
addReleaseTags apiBaseUrl credentials version newTags = do
    errOrReleaseTags <- getReleaseTags apiBaseUrl credentials version
    case errOrReleaseTags of
        Left err -> pure $ Left err
        Right releaseTags ->
            setReleaseTags apiBaseUrl credentials version
          $ Set.union releaseTags newTags

removeReleaseTags ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> Ty.GenericVersion
    -> Set.Set Text
    -> m (Either ServantError (Set.Set Text))
removeReleaseTags apiBaseUrl credentials version removeTags = do
    errOrReleaseTags <- getReleaseTags apiBaseUrl credentials version
    case errOrReleaseTags of
        Left err -> pure $ Left err
        Right releaseTags ->
            setReleaseTags apiBaseUrl credentials version
          $ Set.difference releaseTags removeTags

listReleases ::
       MonadIO m
    => BaseUrl
    -> Ty.Credentials
    -> m (Either Ty.Error [Ty.Release (Set.Set Text)])
listReleases apiBaseUrl credentials = do
    manager <- liftIO Client.newTlsManager
    runExceptT $ do
        packages <- withExceptT (Ty.ErrorRequest . Ty.ServantError) $ ExceptT $ liftIO $ runClientM
            (getPackageReq
                daSubject
                daRepository
                sdkPackageName
                (Just Ty.Yes)
                (credentialsToBasicAuthData credentials))
            (ClientEnv manager apiBaseUrl Nothing)
        forM (Ty._bpVersions packages) $ \version@(Ty.GenericVersion vTxt) -> do
            semVersion <- maybe (throwE (Ty.ErrorVersionParse vTxt)) pure $ Version.parseSemVersion vTxt
            releaseTags <- withExceptT (Ty.ErrorRequest . Ty.ServantError)
                         $ ExceptT $ getReleaseTags apiBaseUrl credentials version
            pure $ Ty.Release
                { Ty._releaseVersion = semVersion
                , Ty._releaseTags = releaseTags
                }

--------------------------------------------------------------------------------
-- Instances
--------------------------------------------------------------------------------

instance FromJSON v => FromJSON (PackageVersion v) where
    parseJSON (Object v) = PackageVersion <$> v .: "name"
    parseJSON invalid    = typeMismatch "PackageVersion" invalid
