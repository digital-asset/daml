-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
module Util (
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
import           Control.Monad.Writer.Class
import Control.Monad.Trans.Writer (WriterT, execWriterT)
import           Data.Char (isSpace)
import           Data.Conduit ((.|))
import qualified Data.Conduit as C
import qualified Data.Conduit.Process as Proc
import qualified Data.Conduit.Text as CT
import           Data.Foldable (for_)
import           Data.Text (Text, unpack)
import qualified Data.Text as T
import           Path
import           Path.IO
import           System.Console.ANSI
                   (Color(..), SGR(SetColor, Reset), ConsoleLayer(Foreground),
                    ColorIntensity(..), setSGRCode)
import qualified System.Log.FastLogger as FastLogger
import qualified Text.XML as XML
import qualified Text.XML.Cursor as XML

import Types

-- pom
-- --------------------------------------------------------------------

data ArtifactLocation
    = Declared PomArtifact
    -- ^ Manual declaration of the artifact location.
    -- This is used for non-jars.
    | FetchFromPom -- ^ Artifact location should be fetched from pom.

data PomArtifact = PomArtifact
  { pomArtGroupId :: GroupId
  , pomArtArtifactId :: ArtifactId
  , pomArtClassifier :: Maybe Classifier
  , pomArtVersion :: TextVersion
  } deriving (Eq, Show)

-- | the function below takes a text representation since we can can pass
-- various stuff when releasing versions locally

type BazelTarget = Text
-- --------------------------------------------------------------------

-- release files data for artifactory
type ReleaseDir = Path Abs Dir
type ReleaseArtifact = Path Rel File

data ReleaseType =
     TarGz
   | Jar{jarPrefix :: Text, jarOnly3rdPartyDependencies :: Bool}
   | DeployJar
   | ProtoJar
   -- ^ use this only for java_proto_library targets with _only one dep_, that is, targets
   -- that only produce a single jar.
   | Zip
   deriving (Eq, Show)

isJar :: ReleaseType -> Bool
isJar t =
    case t of
        Jar{} -> True
        ProtoJar -> True
        DeployJar -> True
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
type BuildArtifactT = WriterT [(Component, TextVersion, ReleaseArtifact)]

execBuildArtifactT ::
     Monad m
  => BuildArtifactT m ()
  -> m [(Component, TextVersion, ReleaseArtifact)]
execBuildArtifactT m = execWriterT m


artifactName :: ReleaseType -> Text -> Text
artifactName releaseType target =
    -- Deploy jars are not built by default so we need to specify them explicitely.
    case releaseType of
        DeployJar -> target <> "_deploy.jar"
        _ -> target

artifactFileName :: ReleaseType -> Text -> Text
artifactFileName releaseType target =
    case releaseType of
        TarGz -> target <> ".tar.gz"
        Jar{..} -> jarPrefix <> target <> ".jar"
        DeployJar -> target <> "_deploy.jar"
        Zip -> target <> ".zip"
        -- NOTE: we rely on the proto libraries to have only one jar output here,
        -- which is actually not always the case. specifically, multiple jars are
        -- generated if a java_proto_library depends on multiple source deps.
        -- we should mechanically check this somehow. see also comment to 'ProtoJar'
        ProtoJar -> "lib" <> T.replace "_java" "" target <> "-speed.jar"

readPomData :: FilePath -> IO PomArtifact
readPomData f = do
    doc <- XML.readFile XML.def f
    let c = XML.fromDocument doc
    let elName name = XML.Name name (Just "http://maven.apache.org/POM/4.0.0") Nothing
    [artifactId] <- pure $ c XML.$/ (XML.element (elName "artifactId") XML.&/ XML.content)
    [groupId] <- pure $ c XML.$/ (XML.element (elName "groupId") XML.&/ XML.content)
    [version] <- pure $ c XML.$/ (XML.element (elName "version") XML.&/ XML.content)
    pure $ PomArtifact
        { pomArtGroupId = T.split (== '.') groupId
        , pomArtArtifactId = artifactId
        , pomArtClassifier = Nothing
        , pomArtVersion = version
        }

buildArtifact ::
     MonadCI m
  => PlatformDependent
  -> OS
  -> Component
  -> ReleaseType
  -> ReleaseDir
  -> BazelTarget
  -> ArtifactLocation
  -> BuildArtifactT m ()
buildArtifact platfDep os comp releaseType releaseDir targ artLocation = do
  -- we look for the bazel outputs in bazelBin and bazelGenfiles
  bazelBin <- lift $
    parseAbsDir =<<
    (T.unpack . T.strip . T.unlines <$> loggedProcess "bazel" ["info", "bazel-bin"] C.sourceToList)
  bazelGenfiles <- lift $
    parseAbsDir =<<
    (T.unpack . T.strip . T.unlines <$> loggedProcess "bazel" ["info", "bazel-genfiles"] C.sourceToList)

  let ostxt = if getPlatformDependent platfDep then "-" <> renderOS os else ""
  (directory, name) <- case T.split (':' ==) <$> T.stripPrefix "//" targ of
    Just [x, y] -> return (x, y)
    _ -> throwIO $ CIException $ "malformed bazel target: " <> targ
  directory' <- parseRelDir (T.unpack directory)
  $logInfo $ "Building " <> targ
  lift $ loggedProcess_ "bazel" $
      ["build", artifactName releaseType targ] <>
      [targ <> "_pom" | isJar releaseType]

  pomFileName <- parseRelFile (unpack (name <> "_pom.xml"))
  let pomFile = bazelBin </> directory' </> pomFileName
  PomArtifact gid aid _ vers <-
      case artLocation of
          Declared p -> pure p
          FetchFromPom -> liftIO $ readPomData (unpack $ pathToText pomFile)

  outDir <- parseRelDir $ unpack $
    T.intercalate "/" gid #"/"# aid #"/"# vers #"/"
  createDirIfMissing True (releaseDir </> outDir)
  let tellArtifact platfDep' fp =
          -- NOTE(MH): We release the platform _independent_ artifacts on Linux,
          -- in particular .pom files.
          when (getPlatformDependent platfDep' || os == Linux) $
              tell [(comp, vers, outDir </> fp)]
  -- the main artifact has the same structure for all release types.
  let ext = case releaseType of
        TarGz -> ".tar.gz"
        Jar{}  -> ".jar"
        Zip -> ".zip"
        ProtoJar -> ".jar"
        DeployJar -> ".jar"
  mainArtifactOut <- parseRelFile (unpack (aid #"-"# vers # ostxt # ext))
  -- for many targets, the file we're looking for is the same
  let normalArtifactRelFile = (directory' </>) <$> parseRelFile (T.unpack $ artifactFileName releaseType name)
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
        let pomPath = releaseDir </> outDir </> outPom
        $logInfo ("Writing pom file to "# pathToText pomPath)
        copyFile pomFile pomPath
        tellArtifact (PlatformDependent False) outPom
  relFile <- normalArtifactRelFile
  copyAndTellArtifact platfDep relFile mainArtifactOut
  when (isJar releaseType) releasePom
  case releaseType of
    Jar{} -> do
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
    _ -> pure ()
  where
    targetExists :: MonadCI m => Text -> m Bool
    targetExists target = do
      mbErr <- E.try (loggedProcess_ "bazel" ["query", target])
      case mbErr of
        Left (_ :: Proc.ProcessExitedUnsuccessfully) -> return False
        Right () -> return True

buildAllComponents ::
     MonadCI m
  => ReleaseDir
  -> OS
  -> TextVersion
  -> TextVersion
  -> m [(Component, TextVersion, ReleaseArtifact)]
buildAllComponents releaseDir os sdkVersion compVersion = execBuildArtifactT $ do
          buildArtifact (PlatformDependent True) os SdkComponent TarGz releaseDir
            "//release:sdk-release-tarball"
            (Declared $ PomArtifact ["com", "digitalasset"] "sdk-tarball" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/archive:daml_lf_archive_java"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/data:data"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkMetadata TarGz releaseDir
            "//release:sdk-metadata-tarball"
            (Declared $ PomArtifact ["com", "digitalasset"] "sdk" Nothing sdkVersion)
          buildArtifact (PlatformDependent True) os SdkComponent TarGz releaseDir
            "//daml-foundations/daml-tools/da-hs-damlc-app:damlc-dist"
            (Declared $ PomArtifact ["com", "digitalasset"] "damlc" Nothing compVersion)
          buildArtifact (PlatformDependent True) os SdkComponent DeployJar releaseDir
            "//daml-foundations/daml-tools/damlc-jar:damlc_jar"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//daml-foundations/daml-tools/daml-extension:dist"
            (Declared $ PomArtifact ["com", "digitalasset"] "daml-extension" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/archive:daml_lf_archive_scala"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent Zip releaseDir
            "//daml-lf/archive:daml_lf_archive_protos_zip"
            (Declared $ PomArtifact ["com", "digitalasset"] "daml-lf-archive-protos" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//daml-lf/archive:daml_lf_archive_protos_tarball"
            (Declared $ PomArtifact ["com", "digitalasset"] "daml-lf-archive-protos" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:value_java_proto"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:transaction_java_proto"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent ProtoJar releaseDir
            "//daml-lf/transaction/src/main/protobuf:blindinginfo_java_proto"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/transaction:transaction"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/transaction-scalacheck:transaction-scalacheck"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/lfpackage:lfpackage"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/interface:interface"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/validation:validation"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/interpreter:interpreter"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/scenario-interpreter:scenario-interpreter"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/engine:engine"
            FetchFromPom
          -- TODO(MH): Port to bazel!
          -- buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
          --   "//daml-lf/repl:repl-lib"
          --   (PomArtifact ["com", "digitalasset"] ("daml-lf-repl-lib_"# scalaVersion) Nothing compVersion) []
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/repl:repl"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//daml-lf/testing-tools:testing-tools"
            FetchFromPom
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
            (Declared $ PomArtifact ["com", "digitalasset"] "ledger-api-protos" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//ledger-api/rs-grpc-bridge:rs-grpc-bridge"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//language-support/java/bindings:bindings-java"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent libJar releaseDir
            "//language-support/java/bindings-rxjava:bindings-rxjava"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//docs:quickstart-java"
            (Declared $ PomArtifact ["com", "digitalasset", "docs"] "quickstart-java" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent TarGz releaseDir
            "//ledger/sandbox:sandbox-tarball"
            (Declared $ PomArtifact ["com", "digitalasset"] "sandbox" Nothing compVersion)
          buildArtifact (PlatformDependent False) os SdkComponent DeployJar releaseDir
            "//extractor:extractor-binary"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar{jarOnly3rdPartyDependencies = True} releaseDir
            "//ledger-api/grpc-definitions:ledger-api-scalapb"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger-api/testing-utils:testing-utils"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/bindings:bindings"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger-api/rs-grpc-akka:rs-grpc-akka"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-akka:ledger-api-akka"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//scala-protoc-plugins/scala-logging:scala-logging-lib"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-scala-logging:ledger-api-scala-logging"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/backend-api:backend-api"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-client:ledger-api-client"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-domain:ledger-api-domain"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/ledger-api-common:ledger-api-common"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//ledger/sandbox:sandbox"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent DeployJar releaseDir
            "//ledger/ledger-api-integration-tests:semantic-test-runner"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen:codegen"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/codegen:codegen-main"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/scala/bindings-akka:bindings-akka"
            FetchFromPom

          buildArtifact (PlatformDependent False) os SdkComponent plainJar releaseDir
            "//language-support/java/codegen:shaded_binary"
            FetchFromPom
          buildArtifact (PlatformDependent False) os SdkComponent DeployJar releaseDir
            "//navigator/backend:navigator-binary"
            FetchFromPom

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
