-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
module Util (
    getMavenDependencies,
    isReleaseCommit,
    readVersionAt,
    buildAllComponents,
    releaseToBintray,
    runFastLoggingT,
    slackReleaseMessage,
    whichOS,
  ) where


import qualified Control.Concurrent.Async.Lifted.Safe as Async
import qualified Control.Exception.Safe as E
import           Control.Monad
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Writer.Class (tell)
import           Control.Monad.State.Class (modify, get)
import           Control.Monad.Trans.RWS (RWST, execRWST)
import           Data.Aeson as Aeson
import           Data.Aeson.Types as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC (dropWhile, lines)
import           Data.Char (isSpace)
import           Data.Conduit ((.|))
import qualified Data.Conduit as C
import qualified Data.Conduit.Process as Proc
import qualified Data.Conduit.Text as CT
import           Data.Foldable (for_)
import qualified Data.HashMap.Strict as HMS
import           Data.Maybe
import           Data.Text (Text, unpack)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Traversable (for)
import           Path
import           Path.IO
import           System.Console.ANSI
                   (Color(..), SGR(SetColor, Reset), ConsoleLayer(Foreground),
                    ColorIntensity(..), setSGRCode)
import qualified System.Log.FastLogger as FastLogger

import Types

-- pom
-- --------------------------------------------------------------------

-- | the function below takes a text representation since we can can pass
-- various stuff when releasing versions locally
data PomArtifact = PomArtifact
  { pomArtGroupId :: GroupId
  , pomArtArtifactId :: ArtifactId
  , pomArtClassifier :: Maybe Classifier
  , pomArtVersion :: TextVersion
  } deriving (Eq, Show)


type Lang = Text
type BazelTarget = Text
data MavenArtifactInfo = MavenArtifactInfo
    { mLang :: Text
    , mVersion :: Maybe Text
    }
    deriving (Eq, Show)
type MavenDependencies = HMS.HashMap BazelTarget PomArtifact

instance FromJSON MavenArtifactInfo where
    parseJSON = withObject "MavenDependency" $ \v -> MavenArtifactInfo
        <$> v .: "lang"
        <*> v .:? "version"

trimmedUnlines :: [Text] -> Text
trimmedUnlines = T.unlines . filter (T.any (not . isSpace))

renderDependencies :: [PomArtifact] -> [Text]
renderDependencies deps = do
  PomArtifact{..} <- deps
  return $ trimmedUnlines
    [ "    <dependency>"
    , "      <groupId>"# T.intercalate "." pomArtGroupId #"</groupId>"
    , "      <artifactId>"# pomArtArtifactId #"</artifactId>"
    , "      <version>"# pomArtVersion #"</version>"
    , "      "# renderClassifier pomArtClassifier
    , "    </dependency>"
    ]

renderClassifier :: Maybe Classifier -> Text
renderClassifier = \case
  Just classifier -> "<classifier>" # classifier #"</classifier>"
  Nothing         -> ""

renderPom ::
     PomArtifact
  -> [PomArtifact] -- ^ dependencies
  -> ReleaseType
  -> Text
renderPom (PomArtifact groupId artifactId mbClassifier version) deps rtype = trimmedUnlines $
  [ "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
  , "<project xmlns=\"http://maven.apache.org/POM/4.0.0\""
  , "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
  , "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\">"
  , "  <modelVersion>4.0.0</modelVersion>"
  , "  <groupId>"# T.intercalate "." groupId #"</groupId>"
  , "  <artifactId>"# artifactId #"</artifactId>"
  , "  <version>"# version #"</version>"
  , "  <packaging>"# renderReleaseType rtype #"</packaging>"
  , "  "# renderClassifier mbClassifier
  , "  <dependencies>"
  ] ++ renderDependencies deps ++ [
    "  </dependencies>"
  , "</project>"
  ]
  where
    renderReleaseType :: ReleaseType -> Text
    renderReleaseType = \case
      Jar{} -> "jar"
      ProtoJar -> "jar"
      Zip -> error "zip file in renderPom"
      TarGz -> error "tar.gz file in renderPom"

bazelTarget :: PomArtifact -> Lang -> BazelTarget
bazelTarget PomArtifact{..} lang =
    "//3rdparty/jvm/" <> T.intercalate "/" (mangleGroupId pomArtGroupId) <> ":" <> mangleArtifactId (stripVersion pomArtArtifactId)
  where
    mangleGroupId = map . T.map $ \case{'-' -> '_'; x -> x}
    mangleArtifactId = T.map $ \case{'.' -> '_'; '-' -> '_'; x -> x}
    stripVersion artifactId
        | lang == "scala" = case T.stripSuffix ("_" <> scalaVersion) artifactId of
            Nothing -> error ("Invalid scala artifact id: " ++ T.unpack artifactId)
            Just x -> x
        | otherwise = artifactId

getMavenDependencies :: MonadCI m => Path Abs Dir -> m MavenDependencies
getMavenDependencies rootDir = do
    let workspaceBzl = rootDir </> $(mkRelFile "3rdparty/workspace.bzl")
    depLines <- liftIO $
        mapMaybe isDepLine . BSLC.lines <$> BSL.readFile (pathToString workspaceBzl)
    workspaceDeps <- HMS.fromList <$> mapM parseDepLine depLines
    pure (staticDeps `HMS.union` workspaceDeps)
  where
    isDepLine :: BSL.ByteString -> Maybe BSL.ByteString
    isDepLine (BSLC.dropWhile (==' ') -> line) = do
        guard $ "{\"artifact\":" `BSL.isPrefixOf` line
        BSL.stripSuffix "," line
    parser :: Aeson.Value -> Parser (BazelTarget, PomArtifact)
    parser = withObject "dependency line" $ \obj -> do
        coordinate <- obj .: "artifact"
        lang <- obj .: "lang"
        (groupId, artifactId, mbClassifier, version) <- case T.split (==':') coordinate of
            [groupId, artifactId, version] ->
                pure (groupId, artifactId, Nothing, version)
            [groupId, artifactId, "jar", classifier, version] ->
                pure (groupId, artifactId, Just classifier, version)
            _ -> fail ("Invalid maven coordinate: " ++ T.unpack coordinate)
        pure (mkDependency groupId artifactId mbClassifier version lang)
    parseDepLine :: MonadCI m => BSL.ByteString -> m (BazelTarget, PomArtifact)
    parseDepLine line = case eitherDecode' line >>= Aeson.parseEither parser of
        Left msg -> throwIO $ CIException $ T.pack msg
        Right artifact -> pure artifact
    mkDependency :: Text -> ArtifactId -> Maybe Classifier -> TextVersion -> Lang -> (BazelTarget, PomArtifact)
    mkDependency (T.split (=='.') -> pomArtGroupId) pomArtArtifactId pomArtClassifier pomArtVersion lang =
        let pom = PomArtifact{..}
        in (bazelTarget pom lang, pom)
    staticDeps :: MavenDependencies
    staticDeps = HMS.fromList
        [ mkDependency "org.scala-lang" "scala-compiler" Nothing scalaFullVersion "scala/unmangled"
        , mkDependency "org.scala-lang" "scala-library" Nothing scalaFullVersion "scala/unmangled"
        , mkDependency "org.scala-lang" "scala-reflect" Nothing scalaFullVersion "scala/unmangled"
        , mkDependency "org.scala-lang.modules" ("scala-parser-combinators_" <> scalaVersion) Nothing scalaParserCombinatorsVersion "scala"
        ]

-- FIXME(#422): These versions are taken from `rules_scala`. We need to stop
-- hardcoding them here but rather determine them programmatically.
scalaVersion, scalaFullVersion, scalaParserCombinatorsVersion :: Text
scalaVersion = "2.12"
scalaFullVersion = "2.12.2"
scalaParserCombinatorsVersion = "1.0.4"


data WhichPomDependencies =
    WPDAll
  | WPDOnlyThirdParty

bazelPomDependencies :: MonadCI m => WhichPomDependencies -> MavenDependencies -> Text -> m [PomArtifact]
bazelPomDependencies whichPomDependencies mavenDeps target = do
    let bazelArgs =
            [ "query"
            , case whichPomDependencies of
                WPDAll -> "let deps = labels(deps, "# target #") in $deps except attr(tags, compile_time_dep, $deps)"
                WPDOnlyThirdParty -> "rdeps(//3rdparty/jvm/... ^ deps("# target #"), //3rdparty/jvm/...)"
            ]
    bazelDeps <- loggedProcess "bazel" bazelArgs C.sourceToList
    for bazelDeps $ \dep ->
        case HMS.lookup dep mavenDeps of
            Nothing -> throwIO $ CIException $ "Cannot resolve POM dependency: " <> dep <> " for target "# target
            Just pom -> pure pom

-- components releases
-- --------------------------------------------------------------------

-- release files data for artifactory
type ReleaseDir = Path Abs Dir
type ReleaseArtifact = Path Rel File

data ReleaseType =
     TarGz
   | Jar{jarPrefix :: Text, jarOnly3rdPartyDependencies :: Bool}
   | ProtoJar
   -- ^ use this only for java_proto_library targets with _only one dep_, that is, targets
   -- that only produce a single jar.
   | Zip
   deriving (Eq, Show)

isProtoJar :: ReleaseType -> Bool
isProtoJar = \case
    ProtoJar -> True
    _ -> False

plainJar, libJar :: ReleaseType
plainJar = Jar "" False
libJar = Jar "lib" False

bintrayTargetLocation :: Component -> TextVersion -> Text
bintrayTargetLocation comp version =
    let package = case comp of
          SdkComponent -> "sdk-components"
          SdkMetadata -> "sdk"
    in "digitalassetsdk/DigitalAssetSDK/" # package # "/" # version

releaseToBintray ::
     MonadCI m
  => PerformUpload
  -> ReleaseDir
  -> [(Component, TextVersion, ReleaseArtifact)]
  -> m ()
releaseToBintray upload releaseDir artifacts = do
  for_ artifacts $ \(comp, version, artifact) -> do
    let sourcePath = pathToText (releaseDir </> artifact)
    let targetLocation = bintrayTargetLocation comp version
    let targetPath = pathToText artifact
    let msg = "Uploading "# sourcePath #" to target location "# targetLocation #" and target path "# targetPath
    if getPerformUpload upload
        then do
            $logInfo msg
            let args = ["bt", "upload", "--flat=false", "--publish=true", sourcePath, targetLocation, targetPath]
            mbErr <- E.try (loggedProcess_ "jfrog" args)
            case mbErr of
              Left (err :: Proc.ProcessExitedUnsuccessfully) ->
                $logError ("jfrog failed, assuming it's because the artifact was already there: "# tshow err)
              Right () -> return ()
        else
            $logInfo ("(In dry run, skipping) "# msg)

normalizeGitRev :: MonadCI m => GitRev -> m GitRev
normalizeGitRev rev =
  T.strip . T.unlines <$> loggedProcess "git" ["rev-parse", rev] C.sourceToList

renderOS :: OS -> Text
renderOS = \case
  Linux -> "linux"
  MacOS -> "osx"

whichOS :: MonadCI m => m OS
whichOS = do
  un <- T.strip . T.unlines <$> loggedProcess "uname" [] C.sourceToList
  case un of
    "Darwin" -> return MacOS
    "Linux" -> return Linux
    _ -> throwIO (CIException ("Unexpected result of uname "# un))

readVersionAt :: MonadCI m => GitRev -> m Version
readVersionAt rev = do
    txt <- T.unlines <$> loggedProcess "git" ["show", rev #":"# pathToText versionFile] C.sourceToList
    case parseVersion (T.strip txt) of
        Nothing ->
            throwIO (CIException ("Could not decode VERSION file at revision "# rev #": "# tshow txt))
        Just version ->
            pure version

gitChangedFiles :: MonadCI m => GitRev -> m [T.Text]
gitChangedFiles rev =
    loggedProcess "git" ["diff-tree", "--no-commit-id", "--name-only", rev] C.sourceToList

-- | as we build artifacts for internal dependencies, we update `MavenDependencies`
-- to contain them.
type BuildArtifactT = RWST () [(Component, TextVersion, ReleaseArtifact)] MavenDependencies

execBuildArtifactT ::
     Monad m
  => MavenDependencies
  -> BuildArtifactT m ()
  -> m [(Component, TextVersion, ReleaseArtifact)]
execBuildArtifactT mavDeps m = fmap snd (execRWST m () mavDeps)

buildArtifact ::
     MonadCI m
  => PlatformDependent
  -> OS
  -> Component
  -> ReleaseType
  -> ReleaseDir
  -> BazelTarget
  -> PomArtifact
  -> BuildArtifactT m ()
buildArtifact platfDep os comp releaseType releaseDir targ pomArt@(PomArtifact gid aid _ vers) = do
  outDir <- parseRelDir $ unpack $
    T.intercalate "/" gid #"/"# aid #"/"# vers #"/"
  createDirIfMissing True (releaseDir </> outDir)
  let tellArtifact platfDep' fp =
          -- NOTE(MH): We release the platform _independent_ artifacts on Linux,
          -- in particular .pom files.
          when (getPlatformDependent platfDep' || os == Linux) $
              tell [(comp, vers, outDir </> fp)]
  let ostxt = if getPlatformDependent platfDep then "-" <> renderOS os else ""
  $logInfo $ "Building " <> targ
  lift (loggedProcess_ "bazel" ["build", targ])
  -- we look for the bazel outputs in bazelBin and bazelGenfiles
  bazelBin <- lift $
    parseAbsDir =<<
    (T.unpack . T.strip . T.unlines <$> loggedProcess "bazel" ["info", "bazel-bin"] C.sourceToList)
  bazelGenfiles <- lift $
    parseAbsDir =<<
    (T.unpack . T.strip . T.unlines <$> loggedProcess "bazel" ["info", "bazel-genfiles"] C.sourceToList)
  -- the main artifact has the same structure for all release types.
  let ext = case releaseType of
        TarGz -> ".tar.gz"
        Jar{}  -> ".jar"
        Zip -> ".zip"
        ProtoJar -> ".jar"
  mainArtifactOut <- parseRelFile (unpack (aid #"-"# vers # ostxt # ext))
  -- for many targets, the file we're looking for is the same
  (directory, name) <- case T.split (':' ==) <$> T.stripPrefix "//" targ of
    Just [x, y] -> return (x, y)
    _ -> throwIO $ CIException $ "malformed bazel target: " <> targ
  let normalArtifactRelFile prefix = (</>)
        <$> parseRelDir (T.unpack directory)
        <*> (parseRelFile (T.unpack (prefix <> name)) >>= addFileExtension (T.unpack ext))
  -- common function to tell an artifact given some relative file
  let copyAndTellArtifact platfDep_ (relFile :: Path Rel File) out = do
        artInBin <- doesFileExist (bazelBin </> relFile)
        let absFile = if artInBin then bazelBin </> relFile else bazelGenfiles </> relFile
        absFileExists <- doesFileExist absFile
        unless absFileExists $
          throwIO (CIException ("Could not find "# pathToText relFile #" in "# pathToText bazelBin #" or "# pathToText bazelGenfiles))
        tellArtifact platfDep_ out
        copyFile absFile (releaseDir </> outDir </> out)
  -- for jars and proto jars, we factor out how to release the pom
  let releasePom = do
        outPom <- parseRelFile (unpack (aid #"-"# vers #".pom"))
        mavenDeps <- get
        -- proto jars contain references to internal proto targets these won't resolve, since we
        -- do not (and cannot in their raw form) publish them to maven. however, we know
        -- that they only depend on external stuff. so just look up external stuff.
        let whichPomDependencies = case releaseType of
              Jar{jarOnly3rdPartyDependencies = True} -> WPDOnlyThirdParty
              _ -> if isProtoJar releaseType
                then WPDOnlyThirdParty
                else WPDAll
        -- TODO(FM): I'm positive that for ProtoJars we need to include the grpc dependency explicitly,
        -- since java_proto_library does not.
        deps <- lift (bazelPomDependencies whichPomDependencies mavenDeps targ)
        let txt = renderPom pomArt deps releaseType
        $logInfo ("Writing pom file to "# pathToText (releaseDir </> outDir </> outPom))
        liftIO (BS.writeFile (toFilePath (releaseDir </> outDir </> outPom)) $ T.encodeUtf8 txt)
        tellArtifact (PlatformDependent False) outPom
        -- insert the pom file so that later packages can depend on it
        modify (HMS.insert targ pomArt)
  case releaseType of
    ProtoJar -> do
      -- NOTE: we rely on the proto libraries to have only one jar output here,
      -- which is actually not always the case. specifically, multiple jars are
      -- generated if a java_proto_library depends on multiple source deps.
      -- we should mechanically check this somehow. see also comment to 'ProtoJar'
      relFile <- (</>)
        <$> parseRelDir (T.unpack directory)
        -- bazel seems to strip the _java from proto targets.
        <*> parseRelFile (T.unpack ("lib" <> T.replace "_java" "" name <> "-speed.jar"))
      copyAndTellArtifact platfDep relFile mainArtifactOut
      -- release the pom
      releasePom
    Jar{jarPrefix} -> do
      relFile <- normalArtifactRelFile jarPrefix
      copyAndTellArtifact platfDep relFile mainArtifactOut
      -- check if a src jar exists, and if it does, tell that too
      let srcTarget = "//"# directory #":lib"# name #"-src.jar"
      srcTargetExists <- lift (targetExists srcTarget)
      when srcTargetExists $ do
        $logInfo ("Building " <> srcTarget)
        lift (loggedProcess_ "bazel" ["build", srcTarget])
        relSrcFile <- (</>)
          <$> parseRelDir (T.unpack directory)
          <*> parseRelFile (T.unpack ("lib"# name #"-src.jar"))
        srcOut <- parseRelFile (unpack (aid #"-"# vers # ostxt # "-sources"# ext))
        copyAndTellArtifact (PlatformDependent False) relSrcFile srcOut
      -- release the pom
      releasePom
    TarGz -> do
      relFile <- normalArtifactRelFile ""
      copyAndTellArtifact platfDep relFile mainArtifactOut
    Zip -> do
      relFile <- normalArtifactRelFile ""
      copyAndTellArtifact platfDep relFile mainArtifactOut
  where
    targetExists :: MonadCI m => Text -> m Bool
    targetExists target = do
      mbErr <- E.try (loggedProcess_ "bazel" ["query", target])
      case mbErr of
        Left (_ :: Proc.ProcessExitedUnsuccessfully) -> return False
        Right () -> return True

buildAllComponents ::
     MonadCI m
  => MavenDependencies
  -> ReleaseDir
  -> OS
  -> TextVersion
  -> TextVersion
  -> m [(Component, TextVersion, ReleaseArtifact)]
buildAllComponents mavenDeps releaseDir os sdkVersion compVersion = execBuildArtifactT mavenDeps $ do
          buildArtifact (PlatformDependent True) os SdkComponent TarGz releaseDir
            "//release:sdk-release-tarball"
            (PomArtifact ["com", "digitalasset"] "sdk-tarball" Nothing compVersion)
          let damlLfArchiveArtifact = PomArtifact ["com", "digitalasset"] "daml-lf-archive" Nothing compVersion
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/archive:daml_lf_archive_java"
            damlLfArchiveArtifact
          -- the bazel-level dependency is on //daml-lf/archive:daml_lf_java_proto, but we externally release the above.
          modify (HMS.insert "//daml-lf/archive:daml_lf_java_proto" damlLfArchiveArtifact)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/data:data"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-data_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkMetadata TarGz releaseDir
            "//release:sdk-metadata-tarball"
            (PomArtifact ["com", "digitalasset"] "sdk" Nothing sdkVersion)
          buildArtifact (PlatformDependent True) os SdkComponent TarGz releaseDir
            "//daml-foundations/daml-tools/da-hs-damlc-app:damlc-dist"
            (PomArtifact ["com", "digitalasset"] "damlc" Nothing compVersion)
          buildArtifact (PlatformDependent True) os SdkComponent plainJar releaseDir
            "//daml-foundations/daml-tools/damlc-jar:damlc_jar_deploy"
            (PomArtifact ["com", "digitalasset"] "damlc" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//daml-foundations/daml-tools/daml-extension:dist"
            (PomArtifact ["com", "digitalasset"] "daml-extension" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/archive:daml_lf_archive_scala"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-archive-scala_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent Zip releaseDir
            "//daml-lf/archive:daml_lf_archive_protos_zip"
            (PomArtifact ["com", "digitalasset"] "daml-lf-archive-protos" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//daml-lf/archive:daml_lf_archive_protos_tarball"
            (PomArtifact ["com", "digitalasset"] "daml-lf-archive-protos" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:value_java_proto"
            (PomArtifact ["com", "digitalasset"] "daml-lf-value-java-proto" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:transaction_java_proto"
            (PomArtifact ["com", "digitalasset"] "daml-lf-transaction-java-proto" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:blindinginfo_java_proto"
            (PomArtifact ["com", "digitalasset"] "daml-lf-blindinginfo-java-proto" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/transaction:transaction"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-transaction_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/transaction-scalacheck:transaction-scalacheck"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-transaction-scalacheck_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/lfpackage:lfpackage"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-package_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/interface:interface"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-interface_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/validation:validation"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-validation_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/interpreter:interpreter"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-interpreter_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/scenario-interpreter:scenario-interpreter"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-scenario-interpreter_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/engine:engine"
            (PomArtifact ["com", "digitalasset"] ("daml-lf-engine_"# scalaVersion) Nothing compVersion)
          -- TODO(MH): Port to bazel!
          -- buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
          --   "//daml-lf/repl:repl-lib"
          --   (PomArtifact ["com", "digitalasset"] ("daml-lf-repl-lib_"# scalaVersion) Nothing compVersion) []
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/repl:repl"
            (PomArtifact ["com", "digitalasset"] "daml-lf-repl" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/testing-tools:testing-tools"
            (PomArtifact ["com", "digitalasset"] "daml-lf-engine-testing-tools" Nothing compVersion)
          -- TODO(MH): Port to bazel!
          -- buildArtifact (PlatformDependent False) os SdkComponent Zip releaseDir
          --   "//daml-lf/transaction:daml-lf-engine-protos-zip"
          --   (PomArtifact ["com", "digitalasset"] "daml-lf-engine-protos" Nothing compVersion) []
          -- TODO(MH): Port to bazel!
          -- buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
          --   "//daml-lf/transaction:daml-lf-engine-values-java"
          --   (PomArtifact ["com", "digitalasset"] "daml-lf-engine-values-java" Nothing compVersion) []
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//ledger-api/grpc-definitions:ledger-api-protos-tarball"
            (PomArtifact ["com", "digitalasset"] "ledger-api-protos" Nothing compVersion)
          let rsGrpcBridgePom = PomArtifact ["com", "digitalasset", "ledger-api"] "rs-grpc-bridge" Nothing compVersion
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//ledger-api/rs-grpc-bridge:rs-grpc-bridge"
            rsGrpcBridgePom
          let bindingsJavaPom = PomArtifact ["com", "daml", "ledger"] "bindings-java" Nothing compVersion
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//language-support/java/bindings:bindings-java"
            bindingsJavaPom
          let bindingsRxJavaPom = PomArtifact ["com", "daml", "ledger"] "bindings-rxjava" Nothing compVersion
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//language-support/java/bindings-rxjava:bindings-rxjava"
            bindingsRxJavaPom
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//docs:quickstart-java"
            (PomArtifact ["com", "digitalasset", "docs"] "quickstart-java" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//ledger/sandbox:sandbox-tarball"
            (PomArtifact ["com", "digitalasset"] "sandbox" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//extractor:extractor-binary_deploy"
            (PomArtifact ["com", "digitalasset"] "extractor" Nothing compVersion)

          -- the bazel rules depend on the source .proto files, which we do not release.
          -- rely on the fact that we know the generated code only relies on external
          -- deps
          buildArtifact (PlatformDependent False) os SdkComponent plainJar{jarOnly3rdPartyDependencies = True} releaseDir
            "//ledger-api/grpc-definitions:ledger-api-scalapb"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger-api", "grpc-definitions"]
                , pomArtArtifactId = "ledger-api-scalapb_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger-api/testing-utils:testing-utils"
            (PomArtifact ["com", "digitalasset", "ledger-api"] ("testing-utils_" # scalaVersion) Nothing compVersion)

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/bindings:bindings"
            PomArtifact
                { pomArtGroupId = ["com", "daml", "scala"]
                , pomArtArtifactId = "bindings_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger-api/rs-grpc-akka:rs-grpc-akka"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger-api"]
                , pomArtArtifactId = "rs-grpc-akka_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-akka:ledger-api-akka"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger-api"]
                , pomArtArtifactId = "ledger-api-akka_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//scala-protoc-plugins/scala-logging:scala-logging-lib"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger-api"]
                , pomArtArtifactId = "scala-logging-lib_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-scala-logging:ledger-api-scala-logging"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger-api"]
                , pomArtArtifactId = "ledger-api-scala-logging_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/backend-api:backend-api"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger"]
                , pomArtArtifactId = "ledger-backend-api_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-client:ledger-api-client"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger"]
                , pomArtArtifactId = "ledger-api-client_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-domain:ledger-api-domain"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger"]
                , pomArtArtifactId = "ledger-api-domain_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-common:ledger-api-common"
            PomArtifact
                { pomArtGroupId = ["com", "digitalasset", "ledger"]
                , pomArtArtifactId = "ledger-api-common_" # scalaVersion
                , pomArtClassifier = Nothing
                , pomArtVersion = compVersion
                }

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/sandbox:sandbox"
            (PomArtifact ["com", "digitalasset", "platform"] "sandbox" Nothing compVersion)


          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-integration-tests:semantic-test-runner_deploy"
            (PomArtifact ["com", "digitalasset", "ledger"] "semantic-test-runner" Nothing compVersion)


          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen:codegen"
            (PomArtifact ["com", "daml", "scala"] ("codegen_" # scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen:codegen-main"
            (PomArtifact ["com", "daml", "scala"] ("codegen-main_" # scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen-sql-support:codegen-sql-support"
            (PomArtifact ["com", "daml", "scala"] ("codegen-sql-support_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen-sql-contract-query-framework:codegen-sql-contract-query-framework"
            (PomArtifact ["com", "daml", "scala"] ("codegen-sql-contract-query-framework_"# scalaVersion) Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/bindings-akka:bindings-akka"
            (PomArtifact ["com", "daml", "scala"] ("bindings-akka_" # scalaVersion) Nothing compVersion)

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/java/codegen:shaded_binary"
            (PomArtifact ["com", "daml", "java"] "codegen" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//navigator/backend:navigator-binary_deploy"
            (PomArtifact ["com", "digitalasset"] "navigator" Nothing compVersion)

versionFile :: Path Rel File
versionFile = $(mkRelFile "VERSION")

isReleaseCommit :: MonadCI m => GitRev -> m Bool
isReleaseCommit rev = do
    newRev <- normalizeGitRev rev
    oldRev <- normalizeGitRev (newRev <> "~1")
    files <- gitChangedFiles "HEAD"
    if "VERSION" `elem` files
        then do
            unless (length files == 1) $ throwIO $ CIException "Changed more than VERSION file"
            oldVersion <- readVersionAt oldRev
            newVersion <- readVersionAt newRev
            if
              | newVersion `isVersionBumpOf` oldVersion -> pure True
              -- NOTE(MH): We allow for decreasing the version in case we need
              -- to backout a release commit.
              | oldVersion `isVersionBumpOf` newVersion -> pure False
              | otherwise ->
                throwIO $ CIException $
                    "Forbidden version bump from " <> renderVersion oldVersion
                    <> " to " <> renderVersion newVersion
        else pure False

slackReleaseMessage :: OS -> TextVersion -> Text
slackReleaseMessage os sdkVersion =
  "sdk release ("# renderOS os #"): " # sdkVersion

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
