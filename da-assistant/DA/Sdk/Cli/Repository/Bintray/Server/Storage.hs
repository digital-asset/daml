-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TemplateHaskell     #-}

module DA.Sdk.Cli.Repository.Bintray.Server.Storage
    ( createTables
    , initPackageMetadata
    , getAttributes
    , updateAttributes
    , attributeSearch
    , getPackage
    , createPackage
    , getRepositories
    , getLatestVersion
    , persistConfig
    , getPackageDir
    , uploadContent
    , downloadContent
    , getVersionFiles

    -- utilities
    , noVersion
    , getServerPath
    ) where

import qualified Algorithms.NaturalSort                     as NaturalSort
import           Control.Exception.Lifted                   (bracket)
import           Control.Monad.Except                       (throwError)
import           Control.Monad.Reader                       (MonadReader, ask, asks)
import           Control.Monad.Logger                       (logDebug, logError)
import qualified Data.ByteString                            as BS
import qualified Data.ByteString.Lazy                       as BSL
import           Data.Foldable                              (for_, traverse_, maximumBy)
import           Data.Function                              (on)
import qualified Data.List.NonEmpty                         as NE
import           Data.List.NonEmpty                         (NonEmpty(..))
import           Data.Map                                   (Map)
import qualified Data.Text.Extended                         as Text
import qualified Data.Text.Encoding                         as TE
import           Database.SQLite.Simple
import           DA.Sdk.Prelude
import qualified DA.Sdk.Cli.Repository.Types                as Ty
import           DA.Sdk.Cli.Repository.Bintray.Server.Types
import           Data.Hashable                              (hash)
import qualified Filesystem.Path                            as FS
import           Servant.Server.Internal.ServantErr         (err404, errBody)
import qualified Turtle

withConnection' :: (Connection -> ServerM a) -> ServerM a
withConnection' f = do
    sqliteUriFilename <- Text.unpack <$> asks serverSQLiteFile
    bracket (liftIO $ open sqliteUriFilename) (liftIO . close) f

withTransaction' :: (Connection -> IO a) -> ServerM a
withTransaction' f = withConnection' $ \conn -> liftIO $ withTransaction conn (f conn)

createTables :: ServerConf -> IO ()
createTables conf = withConnection (Text.unpack $ serverSQLiteFile conf) $ \c -> do
    traverse_ (execute_ c) [ dropAttrValueTable
                           , dropAttrTable
                           , dropPackageTable
                           , dropConfigTableStatement
                           , dropPkgToPathMappingTableStatement
                           , dropPkgVsnToFileMappingTableStatement
                           , createPackageTable
                           , createSubjectRepoNameVersionIdx
                           , createAttrTable
                           , createAttrNameIdx
                           , createAttrValueTable
                           , createAttrValueIdx
                           , createConfigTableStatement
                           , createPkgToPathMappingTableStatement
                           , createPkgVsnToFileMappingTableStatement
                           ]
  where
    dropAttrValueTable = "DROP TABLE IF EXISTS attribute_value"
    dropAttrTable = "DROP TABLE IF EXISTS attribute"
    dropPackageTable = "DROP TABLE IF EXISTS package"
    createPackageTable = "CREATE TABLE package (\n\
    \  package_id  INTEGER PRIMARY KEY,\n\
    \  subject     TEXT NOT NULL,\n\
    \  repo        TEXT NOT NULL,\n\
    \  name        TEXT NOT NULL,\n\
    \  description TEXT,\n\
    \  version     TEXT,\n\
    \  CONSTRAINT sub_repo_name_ver_c UNIQUE (subject, repo, name, version)\n\
    \)"
    dropConfigTableStatement = "DROP TABLE IF EXISTS server_config"
    dropPkgToPathMappingTableStatement = "DROP TABLE IF EXISTS pkg_to_path"
    dropPkgVsnToFileMappingTableStatement = "DROP TABLE IF EXISTS pkg_vsn_to_file"
    createSubjectRepoNameVersionIdx =
        "CREATE INDEX sub_repo_name_ver_idx ON package (subject, repo, name, version)"
    createAttrTable = "CREATE TABLE attribute (\n\
    \  attribute_id INTEGER PRIMARY KEY,\n\
    \  package_id   INTEGER NOT NULL,\n\
    \  name         TEXT NOT NULL,\n\
    \  CONSTRAINT package_id_name_c UNIQUE (package_id, name),\n\
    \  FOREIGN KEY (package_id) REFERENCES package (package_id)\n\
    \)"
    createAttrNameIdx = "CREATE INDEX name_idx ON attribute (name)"
    createAttrValueTable = "CREATE TABLE attribute_value (\n\
    \  attribute_id INTEGER NOT NULL,\n\
    \  value TEXT NOT NULL,\n\
    \  PRIMARY KEY (attribute_id, value),\n\
    \  FOREIGN KEY (attribute_id) REFERENCES attribute(attribute_id)\n\
    \)"
    createAttrValueIdx = "CREATE INDEX attr_value_idx ON attribute_value (attribute_id, value)"
    createPkgToPathMappingTableStatement = "CREATE TABLE pkg_to_path (\n\
    \  subject          TEXT NOT NULL,\n\
    \  repo             TEXT NOT NULL,\n\
    \  server_file_path TEXT NOT NULL,\n\
    \  local_file_path  TEXT NOT NULL,\n\
    \  PRIMARY KEY (subject, repo, server_file_path)\n\
    \)"
    createPkgVsnToFileMappingTableStatement = "CREATE TABLE pkg_vsn_to_file (\n\
    \  package_id        INTEGER NOT NULL,\n\
    \  server_file_path  TEXT NOT NULL,\n\
    \  PRIMARY KEY (package_id, server_file_path)\n\
    \)"
    createConfigTableStatement = "CREATE TABLE server_config (\n\
    \    package_directory      TEXT\n\
    \)"


persistConfig :: ServerConf -> [ExternalFilePath] -> IO ()
persistConfig conf serverPathToRealMapping =
    withConnection (Text.unpack $ serverSQLiteFile conf) $ \conn -> do
        executeNamed conn confStmt [ ":pkgDir" := serverPkgsDir conf ]
        for_ serverPathToRealMapping $ \ExternalFilePath {..} ->
            executeNamed conn pkgMapStmt [ ":subject"          := efpSubject
                                         , ":repo"             := efpRepo
                                         , ":server_file_path" := efpServerFilePath
                                         , ":local_file_path"  := pathToString efpLocalFilePath
                                         ]
  where
    confStmt = "INSERT OR REPLACE INTO server_config (package_directory) VALUES (:pkgDir)"
    pkgMapStmt = "INSERT OR REPLACE INTO pkg_to_path (subject, repo, server_file_path, local_file_path)\n\
                 \ VALUES (:subject, :repo, :server_file_path, :local_file_path)"

initPackageMetadata :: ServerConf
                    -> [ExternalFilePath]
                    -> IO ()
initPackageMetadata conf serverPathToRealMapping =
    withConnection (Text.unpack $ serverSQLiteFile conf) $ \conn ->
        for_ serverPathToRealMapping $ \ExternalFilePath {..} -> do
            let Ty.PackageName packageNameTxt = efpPackageName
            createPackage' conn efpSubject efpRepo Ty.PackageDescriptor { descPkgName = packageNameTxt, descDescription = "" }
            createVersion' conn efpSubject efpRepo efpPackageName efpVersion
            updateAttributes' conn efpSubject efpRepo efpPackageName Nothing [Ty.Attribute "tags" (Ty.AVText ["visible-external"])]
            updateAttributes' conn efpSubject efpRepo efpPackageName (Just efpVersion) [Ty.Attribute "tags" (Ty.AVText ["visible-external"])]
            mapVersionFile conf efpSubject efpRepo efpPackageName efpVersion (textToPath efpServerFilePath)

defaultPackageDir :: Text
defaultPackageDir = "/tmp"

getPackageDir :: ServerM Text
getPackageDir = do
    getOnlyOne <$> withConnection' (\conn -> liftIO $ query_ conn statement)
  where
    getOnlyOne [[v]] = v
    getOnlyOne _     = defaultPackageDir
    statement = "SELECT package_directory FROM server_config"

getAttributes ::  Ty.Subject
              -> Ty.Repository
              -> Ty.PackageName
              -> Maybe Ty.GenericVersion
              -> ServerM [Ty.Attribute]
getAttributes subject repo name maybeVersion = do
    $logDebug ("getAttributes: " <> Text.show subject <> " " <>
                Text.show repo <> " " <> Text.show name <> " " <>
                Text.show maybeVersion)
    withConnection' $ getAttributes' subject repo name maybeVersion

getAttributes' :: (MonadIO m, MonadReader ServerConf m)
               => Ty.Subject
               -> Ty.Repository
               -> Ty.PackageName
               -> Maybe Ty.GenericVersion
               -> Connection
               -> m [Ty.Attribute]
getAttributes' subject repo name maybeVersion conn = do
        attrs <- liftIO $ queryNamed conn statement bindings'
        return $ fmap listToAttribute $ NE.groupBy ((==) `on` fst) $ sortBy (compare `on` fst) attrs
  where
    listToAttribute (h :| t) = Ty.Attribute (fst h) $ Ty.AVText (snd h : fmap snd t)
    statement = "SELECT a.name, av.value\n\
                \FROM package AS p JOIN attribute AS a ON p.package_id = a.package_id \
                \                  JOIN attribute_value AS av ON a.attribute_id = av.attribute_id\n\
                \WHERE p.subject = :subject AND p.repo = :repo AND p.name = :name AND p.version " <>
                maybe "IS NULL" (const "= :version") maybeVersion
    bindings = [ ":subject" := subject, ":repo" := repo, ":name" := name ]
    bindings' = maybe bindings (\v -> (":version" := v):bindings) maybeVersion

getAttributesMap' :: (MonadIO m, MonadReader ServerConf m)
                  => Ty.Subject
                  -> Ty.Repository
                  -> Ty.PackageName
                  -> Maybe Ty.GenericVersion
                  -> Connection
                  -> m (Map Text [Text])
getAttributesMap' subject repo name maybeVersion conn =
    Ty.attributesToMap <$> getAttributes' subject repo name maybeVersion conn

updateAttributes :: Ty.Subject
                 -> Ty.Repository
                 -> Ty.PackageName
                 -> Maybe Ty.GenericVersion
                 -> [Ty.Attribute]
                 -> ServerM () -- should be [Ty.Attribute] but we don't need it
updateAttributes subject repo name maybeVersion attributes = do
    $logDebug ("updateAttributes: " <> Text.show subject <> " " <>
                Text.show repo <> " " <> Text.show name <> " " <>
                Text.show maybeVersion <> " " <> Text.show attributes)
    withTransaction' $ \c -> updateAttributes' c subject repo name maybeVersion attributes

updateAttributes' :: Connection
                  -> Ty.Subject
                  -> Ty.Repository
                  -> Ty.PackageName
                  -> Maybe Ty.GenericVersion
                  -> [Ty.Attribute]
                  -> IO () -- should be [Ty.Attribute] but we don't need it
updateAttributes' conn subject repo name maybeVersion attributes = for_ attributes (go conn)
  where
    go conn' (Ty.Attribute attrName (Ty.AVText attrValues)) = do
        let packageId = hash (subject, repo, name, maybeVersion)
        let attributeId = hash (packageId, attrName)
        executeNamed conn' insertAttributeStatement [ ":attribute_id" := attributeId
                                                   , ":package_id"   := packageId
                                                   , ":name"         := attrName ]
        executeNamed conn' deleteAttributeValuesStatement [ ":attribute_id" := attributeId ]
        for_ attrValues $ \attrValue -> do
            executeNamed conn' insertAttributeValueStatement
                [ ":attribute_id" := attributeId
                , ":value"        := attrValue ]

    insertAttributeStatement =
        "INSERT OR IGNORE INTO attribute (attribute_id, package_id, name) \
        \VALUES (:attribute_id, :package_id, :name)"
    deleteAttributeValuesStatement =
        "DELETE FROM attribute_value WHERE attribute_id = :attribute_id"
    insertAttributeValueStatement =
        "INSERT INTO attribute_value (attribute_id, value) \
        \VALUES (:attribute_id, :value)"

-- NOTE: we just need to be able to search for packages, no releases
attributeSearch :: Ty.Subject
                -> Ty.Repository
                -> [Ty.SearchAttribute]
                -- -> Ty.BitFlag
                -> ServerM [Ty.PackageInfo]
attributeSearch subject repo searchAttrs = do
    $logDebug ("attributeSearch: " <> Text.show subject <> " " <>
                Text.show repo <> " " <> Text.show searchAttrs)
    withConnection' $ attributeSearch' subject repo searchAttrs

attributeSearch' :: Ty.Subject
                 -> Ty.Repository
                 -> [Ty.SearchAttribute]
                 -> Connection
                 -- -> Ty.BitFlag
                 -> ServerM [Ty.PackageInfo]
attributeSearch' subject repo searchAttrs conn = do
    -- 1. select all the packageIds of packages which have matching attributes
    packageIds <- liftIO $ query' matchingAllAttributesStatement matchingAllAttributesBindings :: ServerM [Only Int]
    -- 2. for each packageId, we get name, versions, description and attributes
    found <- for packageIds $ \(Only packageId) -> do
        nameDescMay <- headMay <$> liftIO (queryNamed' packageNameAndDescrStatement [ ":package_id" := packageId ]) :: ServerM (Maybe (Ty.PackageName, Text))
        case nameDescMay of
            Just (name, descr) -> do
                versions <- getVersions' subject repo name conn
                attributes <- getAttributesMap' subject repo name Nothing conn
                pure [Ty.PackageInfo {
                      Ty.infoPkgName     = name
                    , Ty.infoVersions    = versions
                    , Ty.infoDescription = descr
                    , Ty.infoAttributes  = attributes
                    }]
            Nothing -> pure []
    pure $ concat found
  where
    query' q na = query conn q na
    queryNamed' q na = queryNamed conn q na
    packageNameAndDescrStatement = "SELECT name, description FROM package WHERE package_id = :package_id AND version IS NULL"

    matchingAllAttributesBindings = searchAttrs >>= \(Ty.SearchAttribute (Ty.Attribute n (Ty.AVText vs))) -> n : vs

    attrWhereClause (Ty.SearchAttribute (Ty.Attribute _ (Ty.AVText vs))) idx =
        "(SELECT a.package_id AS package_id\n\
        \FROM attribute AS a JOIN attribute_value AS av ON a.attribute_id = av.attribute_id\n\
        \WHERE a.name = ? AND av.value IN (" <> intercalate "," (replicate (length vs) "?") <> ")) AS t_" <> show idx
    whereClause =
        let p i = "t_" <> show i <> ".package_id"
        in snd $ foldl1 (\(i1, s1) (i2, s2) -> (i2, unwords [s1, "JOIN", s2, "ON", p i1, "=", p i2]))
                        (fmap (\(idx, attr) -> (idx, attrWhereClause attr idx)) $ zip [0::Int ..] searchAttrs)

    matchingAllAttributesStatement = Query $ Text.pack $
        case searchAttrs of
            [] -> "SELECT package_id FROM attribute"
            _  -> "SELECT t_0.package_id AS package_id FROM " <> whereClause

getVersions' :: Ty.Subject
             -> Ty.Repository
             -> Ty.PackageName
             -> Connection
             -> ServerM [Ty.GenericVersion]
getVersions' subject repository packageName conn =
    fmap fromOnly <$> liftIO (queryNamed conn versionsStatement versionBindings)
  where
    versionBindings = [ ":subject" := subject, ":repo" := repository, ":name" := packageName ]
    versionsStatement = "SELECT version FROM package WHERE subject = :subject AND repo = :repo AND name = :name AND version IS NOT NULL"

getPackage :: Ty.Subject
           -> Ty.Repository
           -> Ty.PackageName
           -> ServerM (Maybe Ty.BintrayPackage)
getPackage subject repo name =
    withConnection' $ \conn -> do
        $logDebug ("getPackage: " <> Text.show subject <> " " <>
                    Text.show repo <> " " <> Text.show name)
        attributes <- getAttributesMap' subject repo name Nothing conn
        versions <- getVersions' subject repo name conn
        descMay <- liftIO $ fmap fromOnly . headMay <$> queryNamed conn packageNameAndDescrStatement packageNameAndDescrBindings
        case descMay of
            Just desc ->
                pure $ Just Ty.BintrayPackage {
                      Ty._bpPackageName = name
                    , Ty._bpRepository  = repo
                    , Ty._bpOwner       = subject
                    , Ty._bpVersions    = versions
                    , Ty._bpDescription = desc
                    , Ty._bpAttributes  = attributes
                }
            Nothing -> do
                $logDebug $ "No package found for " <> Text.show subject <> " " <>
                    Text.show repo <> " " <> Text.show name
                pure Nothing
  where
    packageNameAndDescrStatement = "SELECT description FROM package WHERE \n\
        \subject = :subject AND repo = :repo AND name = :name AND version IS NULL"
    packageNameAndDescrBindings = [ ":subject" := subject, ":repo" := repo, ":name" := name ]


getMappedPathForServerPath :: Ty.Subject -> Ty.Repository -> [Text] -> ServerM (Maybe FilePath)
getMappedPathForServerPath subject@(Ty.Subject subjectTxt) repo@(Ty.Repository repoTxt) path = do
    let pathTxt = Text.intercalate "/" path
    $logDebug ("searching mapped path for subject: " <> subjectTxt <> " repo: " <> repoTxt <> " path: " <> pathTxt)
    rows <- withConnection' $ \conn -> liftIO $
        queryNamed conn statement [ ":subject"           := subject
                                  , ":repo"              := repo
                                  , ":server_file_path"  := pathTxt
                                  ] :: ServerM [Only Text]
    return $ fmap (textToPath . fromOnly) $ headMay rows
  where
    statement = "SELECT local_file_path FROM pkg_to_path WHERE subject = :subject AND repo = :repo AND server_file_path = :server_file_path"

noVersion :: Maybe Ty.GenericVersion
noVersion = Nothing

createPackage :: Ty.Subject
              -> Ty.Repository
              -> Ty.PackageDescriptor
              -> ServerM ()
createPackage subject repo p = do
    $logDebug ("createPackage: " <> Text.show subject <> " " <> Text.show repo <> " " <>
                Text.show p)
    withConnection' $ \conn -> liftIO $ createPackage' conn subject repo p

createPackage' :: Connection
               -> Ty.Subject
               -> Ty.Repository
               -> Ty.PackageDescriptor
               -> IO ()
createPackage' conn subject repo Ty.PackageDescriptor {..} =
    create conn subject repo (Ty.PackageName descPkgName) (Just descDescription) noVersion

createVersion :: Ty.Subject
              -> Ty.Repository
              -> Ty.PackageName
              -> Ty.GenericVersion
              -> ServerM ()
createVersion subject repo name version = do
    $logDebug ("createVersion: " <> Text.show subject <> " " <> Text.show repo <> " " <>
               Text.show name <> " " <> Text.show version)
    withConnection' $ \conn -> liftIO $ createVersion' conn subject repo name version

createVersion' :: Connection
               -> Ty.Subject
               -> Ty.Repository
               -> Ty.PackageName
               -> Ty.GenericVersion
               -> IO ()
createVersion' conn subject repo name version =
    create conn subject repo name Nothing (Just version)

create :: Connection
       -> Ty.Subject
       -> Ty.Repository
       -> Ty.PackageName
       -> Maybe Text
       -> Maybe Ty.GenericVersion
       -> IO ()
create conn subject repo name descrMay versionMay =
    executeNamed conn statement bindings
  where
    packageId = hash (subject, repo, name, versionMay)
    statement = "INSERT OR IGNORE INTO package (package_id, subject, repo, name, description, version)\n\
               \ VALUES (:package_id, :subject, :repo, :name, :description, :version)"
    bindings = [ ":package_id" := packageId, ":subject" := subject, ":repo" := repo
               , ":name" := name, ":description" := fromMaybe "" descrMay
               , ":version" := versionMay ]

getRepositories :: Ty.Subject
                -> ServerM [Ty.Repository]
getRepositories subject = do
    $logDebug ("getRepositories: " <> Text.show subject)
    withConnection' queryRepo
  where
    queryRepo :: Connection -> ServerM [Ty.Repository]
    queryRepo conn = fmap fromOnly <$> liftIO (queryNamed conn statement bindings)
    statement = "SELECT DISTINCT repo FROM package WHERE subject = :subject"
    bindings = [ ":subject" := subject ]

getLatestVersion :: Ty.Subject
                 -> Ty.Repository
                 -> Ty.PackageName
                 -> ServerM Ty.LatestVersion
getLatestVersion subject repo name = do
    $logDebug ("getLatestVersion: " <> Text.show subject <> " " <>
                Text.show repo <> " " <> Text.show name)
    versions <- withConnection' $ getVersions' subject repo name
    $logDebug ("getLatestVersion: " <> Text.show versions)
    case versions of
      [] -> throwError $ err404 { errBody = "No versions of this artefact present." }
      _  -> do
        let latest = maximumBy (NaturalSort.compare `on` fromGV) versions
        $logDebug ("getLatestVersion: " <> Text.show latest)
        pure $ Ty.LatestVersion latest
  where
    fromGV (Ty.GenericVersion vTxt) = vTxt

uploadContent :: Ty.Subject
              -> Ty.Repository
              -> Ty.PackageName
              -> Ty.GenericVersion
              -> [Text]
              -> BS.ByteString
              -- -> Maybe Ty.BitFlag
              -> ServerM ()
uploadContent subject@(Ty.Subject subjectTxt)
              repo@(Ty.Repository repoTxt)
              name@(Ty.PackageName nameTxt)
              version@(Ty.GenericVersion versionTxt)
              relPath
              bytes = do
    $logDebug ("uploadContent: " <> subjectTxt <> " " <> repoTxt <> " " <>
                nameTxt <> " " <> versionTxt <> " " <> Text.show relPath)
    pkgsDir <- asks serverPkgsDir
    -- nameTxt, versionTxt are NOT part of the content file's path (which equals to the download URLs path)
    -- e.g.: https://digitalassetsdk.bintray.com/templates-test/language-binding-csharp-0.7.x/1/language-binding-csharp-0.7.x-1.tar.gz
    -- Name and version there come from the relative path component of the upload and NOT from nameTxt and versionTxt.
    let contentFileRel = FS.concat $ fmap textToPath relPath
        contentFile    = FS.concat $ fmap textToPath $ [pkgsDir, subjectTxt, repoTxt] ++ relPath
        contentDir     = FS.parent contentFile
    liftIO $ Turtle.mktree contentDir
    liftIO $ BS.writeFile (pathToString contentFile) bytes
    $logDebug ("Writing uploaded file: " <> (Text.pack $ pathToString contentFile))
    createVersion subject repo name version
    conf <- ask
    liftIO $ mapVersionFile conf subject repo name version contentFileRel

mapVersionFile :: ServerConf
               -> Ty.Subject
               -> Ty.Repository
               -> Ty.PackageName
               -> Ty.GenericVersion
               -> FS.FilePath
               -> IO ()
mapVersionFile conf subject repo name version filepath =
    withConnection (Text.unpack $ serverSQLiteFile conf)
        $ \conn -> executeNamed conn mapFileStm bindingsMapFileStm
  where
    versionMay = Just version
    mapFileStm = "INSERT OR IGNORE INTO pkg_vsn_to_file (package_id, server_file_path)\n\
               \ VALUES (:package_id, :server_file_path)"
    bindingsMapFileStm =
               [ ":package_id"       := hash (subject, repo, name, versionMay)
               , ":server_file_path" := pathToString filepath ]

getServerPath :: Text
              -> Ty.Subject
              -> Ty.Repository
              -> [Text]
              -> FS.FilePath
getServerPath pkgsDir
              (Ty.Subject subjectTxt)
              (Ty.Repository repoTxt)
              relPath= FS.concat $ fmap textToPath $ [pkgsDir, subjectTxt, repoTxt] ++ relPath

-- Bintray API has a different signature for downloadContent from uploadContent
-- (see https://bintray.com/docs/api/#_download_content)
downloadContent :: Ty.Subject
                -> Ty.Repository
                -> [Text]
                -> ServerM BS.ByteString
downloadContent subject@(Ty.Subject subjectTxt)
                repo@(Ty.Repository repoTxt)
                relPath = do
    pkgsDir           <- asks serverPkgsDir
    let nonMappedPath =  getServerPath pkgsDir subject repo relPath
    mappedPathMay     <- getMappedPathForServerPath subject repo relPath
    logMappedOrNot mappedPathMay nonMappedPath
    let localFilePath = fromMaybe nonMappedPath mappedPathMay
    doesFileExist <- Turtle.testfile localFilePath
    if doesFileExist
    then do
        $logDebug $ "file '" <> pathToText localFilePath <> "' exists, I'm returning it's content"
        liftIO $ BS.readFile $ pathToString localFilePath
    else do
        $logError $ "file '" <> pathToText localFilePath <> "' doesn\'t exists (content requested " <> contentTxt <> ")"
        let inputPath = "/" <> subjectTxt <> "/" <> repoTxt <> "/" <> Text.intercalate "/" relPath
        let errBody = BSL.fromStrict $ TE.encodeUtf8 ("The requested file (" <> inputPath <> ") does not exist.")
        throwError $ err404 { errBody = errBody }
  where
    relPathTxt = Text.intercalate "/" relPath
    contentTxt = "subject: " <> subjectTxt <> " repo: " <> repoTxt <> " path: " <> relPathTxt
    logMappedOrNot Nothing nonMappedPath =
        $logDebug $ contentTxt <> " is not a mapped path and resolves as " <> pathToText nonMappedPath
    logMappedOrNot (Just mappedPath) _   =
        $logDebug $ contentTxt <> " is a mapped path and it is mapped to " <> pathToText mappedPath

-- Get Version Files
-- GET /packages/:subject/:repo/:package/versions/:version/files[?include_unpublished=0/1]
getVersionFiles :: Ty.Subject
                -> Ty.Repository
                -> Ty.PackageName
                -> Ty.GenericVersion
                -- -> Maybe Ty.BitFlag
                -> ServerM [Ty.PkgFileInfo]
getVersionFiles subject@(Ty.Subject subjectTxt)
                repo@(Ty.Repository repoTxt)
                name@(Ty.PackageName nameTxt)
                version@(Ty.GenericVersion versionTxt) = do
    $logDebug ("getVersionFiles: " <> subjectTxt <> " " <> repoTxt <> " " <> nameTxt <> " " <> versionTxt)
    filePaths <- queryRepo
    return $ Ty.PkgFileInfo <$> filePaths
  where
    queryRepo :: ServerM [Text]
    queryRepo = do
        rows <- withConnection' $ \conn -> liftIO (queryNamed conn statement bindings)
        return $ catMaybes $ headMay <$> rows
    statement = "SELECT DISTINCT server_file_path FROM pkg_vsn_to_file WHERE package_id = :package_id"
    bindings  = [ ":package_id" := hash (subject, repo, name, Just version) ]
