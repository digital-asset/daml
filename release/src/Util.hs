-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Util (
    runFastLoggingT,

    Artifact(..),
    ArtifactLocation(..),
    ArtifactFiles(..),
    BazelLocations(..),
    BazelTarget(..),
    PomData(..),

    artifactFiles,
    buildTargets,
    copyToReleaseDir,
    getBazelLocations,
    isScalaJar,
    isDeployJar,
    loggedProcess_,
    mavenArtifactCoords,
    resolvePomData,
    splitBazelTarget,

    mainArtifactPath,
    pomFilePath,
    sourceJarPath,
    javadocJarPath,

    osName
  ) where


import Control.Applicative
import qualified Control.Concurrent.Async.Lifted.Safe as Async
import qualified Control.Exception.Safe as E
import Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import Data.Aeson
import           Data.Char (isSpace)
import           Data.Conduit ((.|))
import qualified Data.Conduit as C
import qualified Data.Conduit.Process as Proc
import qualified Data.Conduit.Text as CT
import           Data.SemVer (Version)
import qualified Data.SemVer as SemVer
import qualified System.Process
import Data.Maybe
import           Data.Text (Text, unpack)
import qualified Data.Text as T
import           Path
import           Path.IO
import           System.Console.ANSI
                   (Color(..), SGR(SetColor, Reset), ConsoleLayer(Foreground),
                    ColorIntensity(..), setSGRCode)
import qualified System.Log.FastLogger as FastLogger
import System.Info.Extra
import qualified Text.XML as XML
import qualified Text.XML.Cursor as XML

import Types

newtype BazelTarget = BazelTarget { getBazelTarget :: Text }
    deriving (FromJSON, Show)

data ReleaseType
    = Jar JarType
    deriving (Eq, Show)

data JarType
    = Lib
      -- ^ A java library jar, with source and javadoc jars.
    | Deploy
      -- ^ Deploy jar, e.g. a fat jar containing transitive deps.
    | Proto
      -- ^ A java protobuf library (*-speed.jar).
    | Scala
      -- ^ A scala library jar, with source and scaladoc jars.
    | JarJar
      -- ^ A shaded jar built with jarjar. This is similar to a deploy jar
      -- but the names of the targets are slightly different.
    deriving (Eq, Show)

instance FromJSON ReleaseType where
    parseJSON = withText "ReleaseType" $ \t ->
        case t of
            "jar-lib" -> pure $ Jar Lib
            "jar-deploy" -> pure $ Jar Deploy
            "jar-proto" -> pure $ Jar Proto
            "jar-scala" -> pure $ Jar Scala
            "jar-jarjar" -> pure $ Jar JarJar
            _ -> fail ("Could not parse release type: " <> unpack t)

data Artifact c = Artifact
    { artTarget :: !BazelTarget
    , artReleaseType :: !ReleaseType
    , artJavadocJar :: !(Maybe (Path Rel File))
    , artSourceJar :: !(Maybe (Path Rel File))
    -- artJavadocJar and artSourceJar can be used to specify the path to
    -- custom Bazel targets javadoc and source jars. The corresponding Bazel target
    -- is assumed to be in the same package as artTarget and should have the same
    -- name as the output file. E.g., if artTarget points to //foobar:mycustomx,
    -- artSourceJar can be set to mycustomx-src.jar and we will build the bazel target
    -- //foobar:mycustomx-src.jar.
    , artMetadata :: !c
    } deriving Show

instance FromJSON (Artifact (Maybe ArtifactLocation)) where
    parseJSON = withObject "Artifact" $ \o -> Artifact
        <$> o .: "target"
        <*> o .: "type"
        <*> o .:? "javadoc-jar"
        <*> o .:? "src-jar"
        <*> o .:? "location"

data ArtifactLocation = ArtifactLocation
    { coordGroupId :: GroupId
    , coordArtifactId :: ArtifactId
    } deriving Show

instance FromJSON ArtifactLocation where
    parseJSON = withObject "ArtifactLocation" $ \o -> do
        groupId <- o .: "groupId"
        artifactId <- o .: "artifactId"
        pure $ ArtifactLocation (T.split (== '.') groupId) artifactId

-- | This maps a target declared in artifacts.yaml to the individual Bazel targets
-- that need to be built for a release.
buildTargets :: IncludeDocs -> Artifact (Maybe ArtifactLocation) -> [BazelTarget]
buildTargets (IncludeDocs includeDocs) art@Artifact{..} =
    case artReleaseType of
        Jar jarTy ->
            let pomTar = BazelTarget (getBazelTarget artTarget <> "_pom")
                jarTarget | jarTy == Deploy = BazelTarget (getBazelTarget artTarget <> "_deploy.jar")
                          | otherwise = artTarget
                (directory, _) = splitBazelTarget artTarget
            in [jarTarget, pomTar] <>
               map (\t -> BazelTarget ("//" <> directory <> ":" <> t))
               (catMaybes $
                    [ sourceJarName art
                    , scalaSourceJarName art
                    , deploySourceJarName art
                    -- java_proto_library produces the sources as a side-effect, but is not a proper implicit target
                    -- therefore we cannot add it here as a "required target" to build as with the others
                    -- , protoSourceJarName art
                    , T.pack . toFilePath <$> artSourceJar
                    , javadocDeployJarName art
                    , javadocProtoJarName art
                    ] <>
                    concat [
                       [ javadocJarName art
                       , scaladocJarName art
                       , T.pack . toFilePath <$> artJavadocJar
                       ]
                       | includeDocs
                    ])

data PomData = PomData
  { pomGroupId :: GroupId
  , pomArtifactId :: ArtifactId
  , pomVersion :: Version
  } deriving (Eq, Show)

readPomData :: FilePath -> IO PomData
readPomData f = do
    doc <- XML.readFile XML.def f
    let c = XML.fromDocument doc
    let elName name = XML.Name name (Just "http://maven.apache.org/POM/4.0.0") Nothing
    [artifactId] <- pure $ c XML.$/ (XML.element (elName "artifactId") XML.&/ XML.content)
    [groupId] <- pure $ c XML.$/ (XML.element (elName "groupId") XML.&/ XML.content)
    [Right version] <- pure $ map SemVer.fromText $ c XML.$/ (XML.element (elName "version") XML.&/ XML.content)
    pure $ PomData
        { pomGroupId = T.split (== '.') groupId
        , pomArtifactId = artifactId
        , pomVersion = version
        }

resolvePomData :: BazelLocations -> Version -> Artifact (Maybe ArtifactLocation) -> IO (Artifact PomData)
resolvePomData BazelLocations{..} version art =
    case artMetadata art of
        Just ArtifactLocation{..} -> pure art
            { artMetadata = PomData
                { pomGroupId = coordGroupId
                , pomArtifactId = coordArtifactId
                , pomVersion = version
                }
            }
        Nothing -> do
            let (dir, name) = splitBazelTarget $ artTarget art
            dir <- parseRelDir (unpack dir)
            name <- parseRelFile (unpack name <> "_pom.xml")
            dat <- readPomData $ unpack $ pathToText $ bazelBin </> dir </> name
            pure art { artMetadata = dat }

data BazelLocations = BazelLocations
    { bazelBin :: !(Path Abs Dir)
    } deriving Show

getBazelLocations :: IO BazelLocations
getBazelLocations = do
    bazelBin <- parseAbsDir . T.unpack . T.strip . T.pack =<< System.Process.readProcess "bazel" ["info", "bazel-bin"] ""
    pure BazelLocations{..}

splitBazelTarget :: BazelTarget -> (Text, Text)
splitBazelTarget (BazelTarget t) =
    case T.split (== ':') <$> T.stripPrefix "//" t of
        Just [a, b] -> (a, b)
        _ -> error ("Malformed bazel target: " <> show t)

mainExt :: ReleaseType -> Text
mainExt Jar{} = "jar"

mainFileName :: ReleaseType -> Text -> Text
mainFileName (Jar jarTy) name = case jarTy of
    Lib -> "lib" <> name <> ".jar"
    Deploy -> name <> "_deploy.jar"
    Proto -> "lib" <> T.replace "_java" "" name <> "-speed.jar"
    Scala -> name <> ".jar"
    JarJar -> name <> ".jar"

sourceJarName :: Artifact a -> Maybe Text
sourceJarName Artifact{..}
  | Jar Lib <- artReleaseType = Just $ "lib" <> snd (splitBazelTarget artTarget) <> "-src.jar"
  | otherwise = Nothing

scalaSourceJarName :: Artifact a -> Maybe Text
scalaSourceJarName Artifact{..}
  | artReleaseType `elem` [Jar Scala, Jar JarJar] = Just $ snd (splitBazelTarget artTarget) <> "_src.jar"
  | otherwise = Nothing

deploySourceJarName :: Artifact a -> Maybe Text
deploySourceJarName Artifact{..}
  | Jar Deploy <- artReleaseType = Just $ snd (splitBazelTarget artTarget) <> "_src.jar"
  | otherwise = Nothing

protoSourceJarName :: Artifact a -> Maybe Text
protoSourceJarName Artifact{..}
  | Jar Proto <- artReleaseType = Just $ T.replace "_java" "" (snd (splitBazelTarget artTarget)) <> "-speed-src.jar"
  | otherwise = Nothing

customSourceJarName :: Artifact a -> Maybe Text
customSourceJarName Artifact{..} = T.pack . toFilePath <$> artSourceJar

scaladocJarName :: Artifact a -> Maybe Text
scaladocJarName Artifact{..}
   | artReleaseType `elem` [Jar Scala, Jar JarJar] = Just $ snd (splitBazelTarget artTarget) <> "_scaladoc.jar"
   | otherwise = Nothing

javadocDeployJarName :: Artifact a -> Maybe Text
javadocDeployJarName Artifact{..}
  | Jar Deploy <- artReleaseType = Just $ snd (splitBazelTarget artTarget) <> "_javadoc.jar"
  | otherwise = Nothing

javadocProtoJarName :: Artifact a -> Maybe Text
javadocProtoJarName Artifact{..}
  | Jar Proto <- artReleaseType = Just $ snd (splitBazelTarget artTarget) <> "_javadoc.jar"
  | otherwise = Nothing

javadocJarName :: Artifact a -> Maybe Text
javadocJarName Artifact{..}
  | Jar Lib <- artReleaseType = Just $ snd (splitBazelTarget artTarget) <> "_javadoc.jar"
  | otherwise = Nothing

customJavadocJarName :: Artifact a -> Maybe Text
customJavadocJarName Artifact{..} = T.pack . toFilePath <$> artJavadocJar

data ArtifactFiles f = ArtifactFiles
  { artifactMain :: f
  , artifactPom :: f
  , artifactSources :: Maybe f
  , artifactJavadoc :: Maybe f
  } deriving (Functor, Foldable)

-- | Given an artifact, produce a list of pairs of an input file and the corresponding output file.
artifactFiles :: E.MonadThrow m => IncludeDocs -> Artifact PomData -> m (ArtifactFiles (Path Rel File, Path Rel File))
artifactFiles (IncludeDocs includeDocs) artifact@Artifact{..} = do
    let PomData{..} = artMetadata
    outDir <- parseRelDir $ unpack $
        T.intercalate "/" pomGroupId #"/"# pomArtifactId #"/"# SemVer.toText pomVersion #"/"
    let (directory, name) = splitBazelTarget artTarget
    directory <- parseRelDir $ unpack directory

    mainArtifactIn <- mainArtifactPath name artifact
    mainArtifactOut <- parseRelFile (unpack (pomArtifactId #"-"# SemVer.toText pomVersion # "." # mainExt artReleaseType))

    pomFileIn <- pomFilePath name
    pomFileOut <- releasePomPath artMetadata

    mbSourceJarIn <- sourceJarPath artifact
    sourceJarOut <- releaseSourceJarPath artMetadata

    mbJavadocJarIn <- javadocJarPath artifact
    javadocJarOut <- releaseDocJarPath artMetadata

    pure $ ArtifactFiles
      { artifactMain = (directory </> mainArtifactIn, outDir </> mainArtifactOut)
      , artifactPom = (directory </> pomFileIn, outDir </> pomFileOut)
      , artifactSources = fmap (\sourceJarIn -> (directory </> sourceJarIn, outDir </> sourceJarOut)) mbSourceJarIn
      , artifactJavadoc = guard includeDocs *> fmap (\javadocJarIn -> (directory </> javadocJarIn, outDir </> javadocJarOut)) mbJavadocJarIn
      }
        -- ^ Note that the Scaladoc is specified with the "javadoc" classifier.

mainArtifactPath :: E.MonadThrow m => Text -> Artifact a -> m (Path Rel File)
mainArtifactPath name artifact = parseRelFile $ unpack $ mainFileName (artReleaseType artifact) name

pomFilePath :: E.MonadThrow m => Text -> m (Path Rel File)
pomFilePath name = parseRelFile $ unpack $ name <> "_pom.xml"

sourceJarPath :: E.MonadThrow m => Artifact a -> m (Maybe (Path Rel File))
sourceJarPath artifact =
    traverse
        (parseRelFile . unpack)
        (customSourceJarName artifact <|> sourceJarName artifact <|> scalaSourceJarName artifact <|> deploySourceJarName artifact <|> protoSourceJarName artifact)

javadocJarPath :: E.MonadThrow m => Artifact a -> m (Maybe (Path Rel File))
javadocJarPath artifact =
    traverse
        (parseRelFile . unpack)
        (customJavadocJarName artifact <|> javadocJarName artifact <|> scaladocJarName artifact <|> javadocDeployJarName artifact <|> javadocProtoJarName artifact)

-- | The file path to the source jar for the given artifact in the release directory.
releaseSourceJarPath :: E.MonadThrow m => PomData -> m (Path Rel File)
releaseSourceJarPath PomData{..} =
    parseRelFile (unpack (pomArtifactId # "-" # SemVer.toText pomVersion # "-sources.jar"))

-- | The file path to the javadoc jar for the given artifact in the release directory.
releaseDocJarPath :: E.MonadThrow m => PomData -> m (Path Rel File)
releaseDocJarPath PomData{..} =
    parseRelFile (unpack (pomArtifactId # "-" # SemVer.toText pomVersion # "-javadoc.jar"))

-- | The file path to the pom file for the given artifact in the release directory.
releasePomPath :: E.MonadThrow m => PomData -> m (Path Rel File)
releasePomPath PomData{..} =
    parseRelFile (unpack (pomArtifactId # "-" # SemVer.toText pomVersion # ".pom"))

-- | Given an artifact, produce a list of pairs of an input file and the Maven coordinates.
-- This corresponds to the files uploaded to Maven Central.
mavenArtifactCoords :: E.MonadThrow m => IncludeDocs -> Artifact PomData -> m [(MavenCoords, Path Rel File)]
mavenArtifactCoords (IncludeDocs includeDocs) Artifact{..} = do
    let PomData{..} = artMetadata
    outDir <- parseRelDir $ unpack $
        T.intercalate "/" pomGroupId #"/"# pomArtifactId #"/"# SemVer.toText pomVersion #"/"

    mainArtifactFile <- parseRelFile (unpack (pomArtifactId #"-"# SemVer.toText pomVersion # "." # mainExt artReleaseType))
    pomFile <- releasePomPath artMetadata
    sourcesFile <- releaseSourceJarPath artMetadata
    javadocFile <- releaseDocJarPath artMetadata

    let mavenCoords classifier artifactType =
           MavenCoords { groupId = pomGroupId, artifactId = pomArtifactId, version = pomVersion, classifier, artifactType }
    pure $
         [ (mavenCoords Nothing $ mainExt artReleaseType, outDir </> mainArtifactFile)
         , (mavenCoords Nothing "pom",  outDir </> pomFile)
         , (mavenCoords (Just "sources") "jar", outDir </> sourcesFile)
         ] <>
         [ (mavenCoords (Just "javadoc") "jar", outDir </> javadocFile) | includeDocs
         ]

copyToReleaseDir :: (MonadLogger m, MonadIO m) => BazelLocations -> Path Abs Dir -> Path Rel File -> Path Rel File -> m ()
copyToReleaseDir BazelLocations{..} releaseDir inp out = do
    let absIn = bazelBin </> inp
    let absOut = releaseDir </> out
    $logInfo ("Copying " <> pathToText absIn <> " to " <> pathToText absOut)
    createDirIfMissing True (parent absOut)
    copyFile absIn absOut

isDeployJar :: ReleaseType -> Bool
isDeployJar = (Jar Deploy ==)

isScalaJar :: ReleaseType -> Bool
isScalaJar = (Jar Scala ==)

osName ::  Text
osName
  | isWindows = "windows"
  | isMac = "osx"
  | otherwise = "linux"

runFastLoggingT :: LoggingT IO c -> IO c
runFastLoggingT m = do
  E.bracket
    (FastLogger.newStdoutLoggerSet FastLogger.defaultBufSize)
    FastLogger.rmLoggerSet
    (\logSet -> do
      let lf _location _source level msg = do
            let mbColor = case level of
                  LevelDebug -> Nothing
                  LevelInfo -> Just Cyan
                  LevelWarn -> Just Yellow
                  LevelError -> Just Red
                  LevelOther{} -> Just Magenta
            FastLogger.pushLogStr logSet $ case mbColor of
              Nothing -> msg <> "\n"
              Just color -> mconcat
                [ FastLogger.toLogStr (setSGRCode [SetColor Foreground Dull color])
                , msg
                , FastLogger.toLogStr (setSGRCode [Reset])
                , "\n"
                ]
      runLoggingT m lf)

-- | @loggedProcessCwd mbCwd ex args cont@ run exectuable @ex@ with @args@
--   (optionally in working directory @mbCwd`) and processes the output
--   with the continuation @cont@.
--
loggedProcessCwd ::
     MonadCI m
  => Maybe (Path Rel Dir) -- ^ optional working directory to run executable in
  -> Text    -- ^ executable
  -> [Text]  -- ^ arguments to executable
  -> (C.ConduitT () Text m () -> m b) -- ^ continuation to process the output from exec
  -> m b
loggedProcessCwd mbCwd ex args cont = do
  $logDebug ("Running "# ex #" "# T.intercalate " " (map showArg args))
  let p = (Proc.proc (unpack ex) (map unpack args)){Proc.cwd = fmap toFilePath mbCwd}
  Proc.withCheckedProcessCleanup p $
    \Proc.ClosedStream out err ->
      fmap snd $ Async.concurrently
        (C.runConduit (err .| reLog "err" .| C.awaitForever (\_ -> return ())))
        (cont (out .| reLog "out"))
  where
    showArg arg = if T.any isSpace arg
      then tshow arg
      else arg
    reLog stream =
      CT.decode CT.utf8 .| CT.lines .|
      C.awaitForever (\l -> do
        $logDebug (ex #" "# stream #": "# l)
        C.yield l)

loggedProcess ::
     MonadCI m
  => Text   -- ^ program
  -> [Text] -- ^ args
  -> (C.ConduitT () Text m () -> m b) -> m b
loggedProcess = loggedProcessCwd Nothing

loggedProcess_ ::
     MonadCI m
  => Text -> [Text]
  -> m ()
loggedProcess_ ex args =
  loggedProcess ex args $ \out ->
    C.runConduit (out .| C.awaitForever (\_ -> return ()))
