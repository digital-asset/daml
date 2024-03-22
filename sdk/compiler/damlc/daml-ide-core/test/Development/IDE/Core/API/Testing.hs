-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DuplicateRecordFields #-}

-- | Testing framework for Shake API.
module Development.IDE.Core.API.Testing
    ( ShakeTest
    , GoToDefinitionPattern (..)
    , HoverExpectation (..)
    , D.DiagnosticSeverity(..)
    , runShakeTest
    , runShakeTestOpts
    , makeFile
    , makeModule
    , setFilesOfInterest
    , setOpenVirtualResources
    , setBufferModified
    , setBufferNotModified
    , expectLastRebuilt
    , expectError
    , expectWarning
    , expectOneError
    , expectOnlyErrors
    , expectNoErrors
    , expectDiagnostic
    , expectOnlyDiagnostics
    , expectNoDiagnostics
    , expectGoToDefinition
    , expectTextOnHover
    , expectVirtualResource
    , expectVirtualResourceRegex
    , expectNoVirtualResource
    , expectVirtualResourceNote
    , expectNoVirtualResourceNote
    , timedSection
    , example
    ) where

-- * internal dependencies
import qualified Development.IDE.Core.API         as API
import Development.IDE.Core.Debouncer
import Development.IDE.Core.Shake (ShakeLspEnv(..), NotificationHandler(..))
import qualified Development.IDE.Types.Diagnostics as D
import qualified Development.IDE.Types.Location as D
import DA.Daml.LF.ScenarioServiceClient as SS
import Development.IDE.Core.Rules.Daml
import Development.IDE.Types.Logger
import DA.Daml.Options
import DA.Daml.Options.Types
import qualified DA.Daml.Options.Types as Daml (Options)
import Development.IDE.Core.Service.Daml(VirtualResource(..), mkDamlEnv)
import DA.Test.Util (standardizeQuotes)
import Language.LSP.Types hiding (SemanticTokenAbsolute (..), SemanticTokenRelative (..))

-- * external dependencies
import Control.Concurrent.STM
import Control.Exception.Extra
import qualified Control.Monad.Reader   as Reader
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Vector as V
import qualified Data.Text              as T
import qualified Data.Text.IO           as T.IO
import qualified Data.HashSet as HashSet
import Network.URI
import qualified System.FilePath        as FilePath
import qualified System.FilePath.Posix  as FPP
import qualified System.Directory       as Directory
import qualified Data.Time.Clock        as Clock
import           System.FilePath        ((</>))
import           Control.Monad.Except   (ExceptT (..), MonadError(..), runExceptT)
import           Control.Monad.Reader   (ReaderT (..))
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           System.IO.Temp         (withSystemTempDirectory)
import           System.IO.Extra
import           Control.Monad
import           Data.Maybe
import           Data.List.Extra
import           Text.Regex.TDFA
import           Text.Regex.TDFA.Text ()

-- | Short-circuiting errors that may occur during a test.
data ShakeTestError
    = ExpectedRelativePath FilePath
    | FilePathEscapesTestDir FilePath
    | ExpectedDiagnostics [(D.DiagnosticSeverity, Cursor, T.Text)] [D.FileDiagnostic]
    | ExpectedVirtualResource VirtualResource T.Text (Map VirtualResource T.Text)
    | ExpectedVirtualResourceRegex VirtualResource T.Text (Map VirtualResource T.Text)
    | ExpectedNoVirtualResource VirtualResource (Map VirtualResource T.Text)
    | ExpectedVirtualResourceNote VirtualResource T.Text (Map VirtualResource T.Text)
    | ExpectedNoVirtualResourceNote VirtualResource (Map VirtualResource T.Text)
    | ExpectedNoErrors [D.FileDiagnostic]
    | ExpectedDefinition Cursor GoToDefinitionPattern (Maybe D.Location)
    | ExpectedHoverText Cursor HoverExpectation [T.Text]
    | TimedSectionTookTooLong Clock.NominalDiffTime Clock.NominalDiffTime
    | PatternMatchFailure String
    deriving (Eq, Show)

-- | Results of a successful test.
-- (TODO: Decide what this should contain, if anything. Should performance data go here?)
data ShakeTestResults = ShakeTestResults
    deriving (Eq, Show)

-- | Environment in which to run test.
data ShakeTestEnv = ShakeTestEnv
    { steTestDirPath :: FilePath -- canonical absolute path of temporary test directory
    , steService :: API.IdeState
    , steVirtualResources :: TVar (Map VirtualResource T.Text)
    , steVirtualResourcesNotes :: TVar (Map VirtualResource T.Text)
    }

-- | Monad for specifying Shake API tests. This type is abstract.
newtype ShakeTest t = ShakeTest (ExceptT ShakeTestError (ReaderT ShakeTestEnv IO) t)
    deriving (Functor, Applicative, Monad, MonadIO, MonadError ShakeTestError)

instance MonadFail ShakeTest where
    fail =  throwError . PatternMatchFailure

pattern EventVirtualResourceChanged :: VirtualResource -> T.Text -> Aeson.Value
pattern EventVirtualResourceChanged vr doc <-
             (Aeson.parseMaybe Aeson.parseJSON ->
              Just (VirtualResourceChangedParams (parseURI . T.unpack >=> uriToVirtualResource -> Just vr) doc))

pattern EventVirtualResourceNoteSet :: VirtualResource -> T.Text -> Aeson.Value
pattern EventVirtualResourceNoteSet vr note <-
             (Aeson.parseMaybe Aeson.parseJSON ->
              Just (VirtualResourceNoteSetParams (parseURI . T.unpack >=> uriToVirtualResource -> Just vr) note))


-- | Run shake test on freshly initialised shake service.
runShakeTest :: Maybe SS.Handle -> ShakeTest () -> IO (Either ShakeTestError ShakeTestResults)
runShakeTest = runShakeTestOpts id

-- | Run shake test on freshly initialised shake service, with custom options.
runShakeTestOpts :: (Daml.Options -> Daml.Options) -> Maybe SS.Handle -> ShakeTest () -> IO (Either ShakeTestError ShakeTestResults)
runShakeTestOpts fOpts mbScenarioService (ShakeTest m) = do
    let options = fOpts (defaultOptions Nothing)
            { optDlintUsage = DlintEnabled DlintOptions
                { dlintRulesFile = DefaultDlintRulesFile
                , dlintHintFiles = NoDlintHintFiles
                }
            , optEnableOfInterestRule = True
            }
    virtualResources <- newTVarIO Map.empty
    virtualResourcesNotes <- newTVarIO Map.empty
    let eventLogger :: forall (m :: Method 'FromServer 'Notification). SMethod m -> MessageParams m -> IO ()
        eventLogger (SCustomMethod ((==virtualResourceChangedNotification) -> True)) (EventVirtualResourceChanged vr doc) = atomically $ do
            modifyTVar' virtualResources (Map.insert vr doc)
            modifyTVar' virtualResourcesNotes (Map.delete vr)
        eventLogger (SCustomMethod ((==virtualResourceNoteSetNotification) -> True)) (EventVirtualResourceNoteSet vr note) =
            atomically $ modifyTVar' virtualResourcesNotes (Map.insert vr note)
        eventLogger _ _ = pure ()
    vfs <- API.makeVFSHandle
    damlEnv <- mkDamlEnv options (StudioAutorunAllScenarios False) mbScenarioService
    service <- API.initialise (mainRule options) (DummyLspEnv $ NotificationHandler eventLogger) noLogging noopDebouncer damlEnv (toCompileOpts options) vfs
    result <- withSystemTempDirectory "shake-api-test" $ \testDirPath -> do
        let ste = ShakeTestEnv
                { steService = service
                , steTestDirPath = testDirPath
                , steVirtualResources = virtualResources
                , steVirtualResourcesNotes = virtualResourcesNotes
                }
        runReaderT (runExceptT m) ste

    API.shutdown service

    return (fmap (const ShakeTestResults) result) -- TODO: improve?

-- | (internal) Make sure the path is relative, and it remains inside the
-- temporary test directory tree, and return the corresponding absolute path.
checkRelativePath :: D.NormalizedFilePath -> ShakeTest D.NormalizedFilePath
checkRelativePath (D.fromNormalizedFilePath -> relPath) = do
    unless (FilePath.isRelative relPath) $
        throwError (ExpectedRelativePath relPath)
    testDirPath <- ShakeTest $ Reader.asks steTestDirPath
    let path = D.toNormalizedFilePath' $ testDirPath </> relPath
    checkPath path
    return path

-- | (internal) Make sure the path is absolute and is contained inside the
-- temporary test directory tree.
checkPath :: D.NormalizedFilePath -> ShakeTest ()
checkPath (D.fromNormalizedFilePath -> relPath) = ShakeTest $ do
    testDirPath <- Reader.asks steTestDirPath
    canPath <- liftIO $ Directory.canonicalizePath relPath
    unless (testDirPath `isPrefixOf` canPath) $
        throwError (FilePathEscapesTestDir relPath)

-- | Make a file with given contents.
-- Only call this with relative paths.
makeFile :: D.NormalizedFilePath -> T.Text -> ShakeTest D.NormalizedFilePath
makeFile relPath contents = do
    absPath <- checkRelativePath relPath
    let absPath' = D.fromNormalizedFilePath absPath
    ShakeTest . liftIO $ Directory.createDirectoryIfMissing True $ FilePath.takeDirectory absPath'
    ShakeTest . liftIO $ T.IO.writeFile absPath' contents
    return absPath

-- | (internal) Turn a module name into a relative file path.
moduleNameToFilePath :: String -> D.NormalizedFilePath
moduleNameToFilePath modName = D.toNormalizedFilePath' $ FilePath.addExtension (replace "." [FilePath.pathSeparator] modName) "daml"

-- | Similar to makeFile but including a header derived from the module name.
makeModule :: String -> [T.Text] -> ShakeTest D.NormalizedFilePath
makeModule modName body = do
    let modPath = moduleNameToFilePath modName
    makeFile modPath . T.unlines $
        [ "module " <> T.pack modName <> " where"
        ] ++ body

-- | Set files of interest.
setFilesOfInterest :: [D.NormalizedFilePath] -> ShakeTest ()
setFilesOfInterest paths = do
    forM_ paths checkPath
    service <- ShakeTest $ Reader.asks steService
    ShakeTest . liftIO $ API.setFilesOfInterest service (HashSet.fromList paths)

-- | Set open virtual resources, i.e., open scenario results.
setOpenVirtualResources :: [VirtualResource] -> ShakeTest ()
setOpenVirtualResources vrs = do
    mapM_ (checkPath . vrScenarioFile) vrs
    service <- ShakeTest $ Reader.asks steService
    ShakeTest . liftIO $ API.setOpenVirtualResources service (HashSet.fromList vrs)

-- | Notify compiler service that buffer is modified, with these new contents.
setBufferModified :: D.NormalizedFilePath -> T.Text -> ShakeTest ()
setBufferModified absPath text = setBufferModifiedMaybe absPath (Just text)

-- | Notify compiler service that buffer is not modified, relative to the file on disk.
setBufferNotModified :: D.NormalizedFilePath -> ShakeTest ()
setBufferNotModified absPath = setBufferModifiedMaybe absPath Nothing

-- | (internal) Notify compiler service that buffer is either modified or not.
setBufferModifiedMaybe :: D.NormalizedFilePath -> Maybe T.Text -> ShakeTest ()
setBufferModifiedMaybe absPath maybeText = ShakeTest $ do
    service <- Reader.asks steService
    case maybeText of
        Nothing -> liftIO $ API.setBufferModified service absPath Nothing
        Just content -> liftIO $ API.setBufferModified service absPath (Just content)

-- | (internal) Get diagnostics.
getDiagnostics :: ShakeTest [D.FileDiagnostic]
getDiagnostics = ShakeTest $ do
    service <- Reader.asks steService
    liftIO $ do
        void $ API.runActionSync service $ return ()
        API.getDiagnostics service

-- | Everything that rebuilt in the last execution must pass the predicate
expectLastRebuilt :: Partial => (String -> FilePath -> Bool) -> ShakeTest ()
expectLastRebuilt predicate = ShakeTest $ do
    service <- Reader.asks steService
    testDir <- Reader.asks steTestDirPath
    liftIO $ withTempDir $ \dir -> do
        let file = dir </> "temp.json"
        void $ API.runActionSync service $ return ()
        API.writeProfile service file
        rebuilt <- either error (return . parseShakeProfileJSON testDir) =<< Aeson.eitherDecodeFileStrict' file
        -- ignore those which are set to alwaysRerun - not interesting
        let alwaysRerun typ = typ `elem`
                [ "GetFileExists"
                , "GetFilesOfInterest"
                , "GetModificationTime"
                , "GetOpenVirtualResources"
                , "GetScenarioRoots"
                , "OfInterest"
                ]
        when (null rebuilt) $
            error "Detected that zero files have rebuilt. Most likely that's a bug and we failed to parse the Shake output file."
        let bad = filter (\(typ, file) -> not $ alwaysRerun typ || predicate typ file) rebuilt
        when (bad /= []) $
            error $ unlines $ "Some unexpected entries changed:" : map show bad


-- | Converts from the Shake JSON format, which reads roughly:
--
-- > [["GetModificationTime; /my/file/name", 0, 0]
-- > ,["GetModificationTime; /my/file/more", 0, 0]
-- > ]
parseShakeProfileJSON :: FilePath -> Aeson.Value -> [(String, FilePath)]
parseShakeProfileJSON testDir json =
    [ res
    | Aeson.Array entries <- [json]
    , Aeson.Array entry <- V.toList entries
     -- Number == 0, built in the last run
    , Aeson.String name : _ : Aeson.Number 0 : _ <- [V.toList entry]
    , Just res <- [stripInfix "; " $ replace (FPP.addTrailingPathSeparator testDir') "" $ T.unpack name]
    ]
  where testDir' = D.fromNormalizedFilePath $ D.toNormalizedFilePath' testDir


getVirtualResources :: ShakeTest (Map VirtualResource T.Text)
getVirtualResources = ShakeTest $ do
    service <- Reader.asks steService
    virtualResources <- Reader.asks steVirtualResources
    liftIO $ do
      void $ API.runActionSync service $ return ()
      readTVarIO virtualResources

getVirtualResourcesNotes :: ShakeTest (Map VirtualResource T.Text)
getVirtualResourcesNotes = ShakeTest $ do
    service <- Reader.asks steService
    virtualResourcesNotes <- Reader.asks steVirtualResourcesNotes
    liftIO $ do
      void $ API.runActionSync service $ return ()
      readTVarIO virtualResourcesNotes

-- | Convenient grouping of file path, 0-based line number, 0-based column number.
-- This isn't a record or anything because it's simple enough and generally
-- easier to read as a tuple.
type Cursor = (D.NormalizedFilePath, Int, Int)

cursorFilePath :: Cursor -> D.NormalizedFilePath
cursorFilePath ( absPath, _line, _col) = absPath

cursorPosition :: Cursor -> D.Position
cursorPosition (_absPath,  line,  col) =
    D.Position
        (fromIntegral line)
        (fromIntegral col)

locationStartCursor :: D.Location -> Cursor
locationStartCursor (D.Location path (D.Range (D.Position line col) _)) =
    ( D.toNormalizedFilePath' $ fromMaybe D.noFilePath $ D.uriToFilePath' path
    , fromIntegral line
    , fromIntegral col
    )

-- | Same as Cursor, but passing a list of columns, so you can specify a range
-- such as (foo,1,[10..20]).
type CursorRange = (D.NormalizedFilePath, Int, [Int])

cursorRangeFilePath :: CursorRange -> D.NormalizedFilePath
cursorRangeFilePath (path, _line, _cols) = path

cursorRangeList :: CursorRange -> [Cursor]
cursorRangeList (path, line, cols) = map (path, line,) cols

-- | (internal) Check for a diagnostic (i.e. an error or warning).
-- Declares test a failure if expected diagnostic is missing.
--
-- The match is made based on the file and line number (0-based).
-- We also check the {error,warning} message for a substring.
-- This check is case-insensitive because sometimes minor changes in
-- error message will result in lower vs uppercase, for example
-- "Parse error" vs "parse error" could both be correct.
--
-- In future, we may move to regex matching.
searchDiagnostics :: (D.DiagnosticSeverity, Cursor, T.Text) -> [D.FileDiagnostic] -> ShakeTest ()
searchDiagnostics expected@(severity, cursor, message) actuals =
    unless (any match actuals) $
        throwError $ ExpectedDiagnostics [expected] actuals
  where
    match :: D.FileDiagnostic -> Bool
    match (fp, _, d) =
        Just severity == D._severity d
        && cursorFilePath cursor == fp
        && cursorPosition cursor == D._start ((D._range :: D.Diagnostic -> Range) d)
        && ((standardizeQuotes $ T.toLower message) `T.isInfixOf` (standardizeQuotes $ T.toLower ((D._message :: D.Diagnostic -> T.Text) d)))

expectDiagnostic :: D.DiagnosticSeverity -> Cursor -> T.Text -> ShakeTest ()
expectDiagnostic severity cursor msg = do
    checkPath (cursorFilePath cursor)
    diagnostics <- getDiagnostics
    searchDiagnostics (severity, cursor, msg) diagnostics

-- | Imprecise matching of several diagnostics.
-- Note that this check is lenient because it allows two expected diagnostics to
-- match the same actual diagnostic. Therefore there may be actual diagnostics
-- which are not accounted for in the expected list.
expectOnlyDiagnostics :: [(D.DiagnosticSeverity, Cursor, T.Text)] -> ShakeTest ()
expectOnlyDiagnostics expected = do
    actuals <- getDiagnostics
    forM_ expected $ \e -> searchDiagnostics e actuals
    unless (length expected == length actuals) $
        throwError $ ExpectedDiagnostics expected actuals

-- | Check that the given virtual resource exists and that the given text is
-- an infix of the content.
expectVirtualResource :: VirtualResource -> T.Text -> ShakeTest ()
expectVirtualResource vr content = do
    vrs <- getVirtualResources
    case Map.lookup vr vrs of
      Just res
        | content `T.isInfixOf` res -> pure ()
      _ -> throwError (ExpectedVirtualResource vr content vrs)

-- | Check that the given virtual resource exists and that its content matches
-- the regular expression.
expectVirtualResourceRegex :: VirtualResource -> T.Text -> ShakeTest ()
expectVirtualResourceRegex vr regex = do
    vrs <- getVirtualResources
    case Map.lookup vr vrs of
      Just res
        | res =~ regex -> pure ()
      _ -> throwError (ExpectedVirtualResourceRegex vr regex vrs)

-- | Check that the given virtual resource does not exist.
expectNoVirtualResource :: VirtualResource -> ShakeTest ()
expectNoVirtualResource vr = do
  vrs <- getVirtualResources
  when (vr `Map.member` vrs) $
    throwError (ExpectedNoVirtualResource vr vrs)


-- | Check that the given virtual resource contains a note and that the given text is
-- an infix of the content.
expectVirtualResourceNote :: VirtualResource -> T.Text -> ShakeTest ()
expectVirtualResourceNote vr note = do
    vrsNotes <- getVirtualResourcesNotes
    case Map.lookup vr vrsNotes of
      Just res
        | note `T.isInfixOf` res -> pure ()
      _ -> throwError (ExpectedVirtualResourceNote vr note vrsNotes)

-- | Check that the given virtual resource does not contain a note.
expectNoVirtualResourceNote :: VirtualResource -> ShakeTest ()
expectNoVirtualResourceNote vr = do
  vrsNotes <- getVirtualResourcesNotes
  when (vr `Map.member` vrsNotes) $
    throwError (ExpectedNoVirtualResourceNote vr vrsNotes)

-- | Expect error in given file and (0-based) line number. Require
-- the error message contains a certain substring (case-insensitive).
expectError :: Cursor -> T.Text -> ShakeTest ()
expectError = expectDiagnostic D.DsError

-- | Expect warning in given file and (0-based) line number. Require
-- the warning message contains a certain string (case-insensitive).
expectWarning :: Cursor -> T.Text -> ShakeTest ()
expectWarning = expectDiagnostic D.DsWarning

-- | Expect one error and no other diagnostics.
-- Fails by showing all the diagnostics.
expectOneError :: Cursor -> T.Text -> ShakeTest ()
expectOneError cursor message = expectOnlyDiagnostics [(D.DsError, cursor, message)]

expectOnlyErrors :: [(Cursor, T.Text)] -> ShakeTest ()
expectOnlyErrors = expectOnlyDiagnostics . map (\(cursor, msg) -> (D.DsError, cursor, msg))

-- | Expect no errors anywhere.
expectNoErrors :: ShakeTest ()
expectNoErrors = do
    diagnostics <- getDiagnostics
    let errors = filter (\(_,_,d) -> D._severity d == Just D.DsError) diagnostics
    unless (null errors) $
        throwError (ExpectedNoErrors errors)

-- | Expect no diagnostics whatsoever.
expectNoDiagnostics :: ShakeTest ()
expectNoDiagnostics = expectOnlyDiagnostics []

-- | Express the expected result of go to definition.
data GoToDefinitionPattern
    = Missing
    | At Cursor
    | In String -- module name
    deriving (Eq,Show)

-- | (internal) Match location with go to definition pattern.
matchGoToDefinitionPattern :: GoToDefinitionPattern -> Maybe D.Location -> Bool
matchGoToDefinitionPattern = \case
    Missing -> isNothing
    At c -> maybe False ((c ==) . locationStartCursor)
    In m -> \l -> (Just True ==) $ do
        l' <- l
        let uri = D._uri l'
        fp <- D.uriToFilePath' uri
        pure $ isSuffixOf (D.fromNormalizedFilePath $ moduleNameToFilePath m) (D.fromNormalizedFilePath $ D.toNormalizedFilePath' fp)

-- | Expect "go to definition" to point us at a certain location or to fail.
expectGoToDefinition :: CursorRange -> GoToDefinitionPattern -> ShakeTest ()
expectGoToDefinition cursorRange pattern' = do
    checkPath (cursorRangeFilePath cursorRange)
    service <- ShakeTest $ Reader.asks steService
    forM_ (cursorRangeList cursorRange) $ \cursor -> do
        maybeLoc <- ShakeTest . liftIO . API.runActionSync service $
            API.getDefinition (cursorFilePath cursor) (cursorPosition cursor)
        unless (matchGoToDefinitionPattern pattern' maybeLoc) $
            throwError (ExpectedDefinition cursor pattern' maybeLoc)

-- Expectation of the contents of some hover information.
data HoverExpectation
    = NoInfo -- no hover info at all
    | Contains T.Text -- text argument appears somewhere in the hover info
    | NotContaining T.Text -- text argument appears nowhere in the hover info
    | HasType T.Text -- one of the hover elements ends in ": T" where T is the type
    deriving (Eq, Show)

expectTextOnHover :: CursorRange -> HoverExpectation -> ShakeTest ()
expectTextOnHover cursorRange expectedInfo = do
    let path = cursorRangeFilePath cursorRange
    checkPath path
    service <- ShakeTest $ Reader.asks steService
    forM_ (cursorRangeList cursorRange) $ \cursor -> do
        mbInfo <- ShakeTest . liftIO . API.runActionSync service $
                    API.getAtPoint path (cursorPosition cursor)
        let actualInfo :: [T.Text] = maybe [] snd mbInfo
        unless (hoverPredicate actualInfo) $
            throwError $ ExpectedHoverText cursor expectedInfo actualInfo
  where
    hoverPredicate :: [T.Text] -> Bool
    hoverPredicate = case expectedInfo of
        NoInfo -> null
        Contains t -> any (T.isInfixOf t)
        NotContaining t -> not . any (T.isInfixOf t)
        HasType t -> any (T.isSuffixOf $ ": " <> t) . concatMap T.lines

-- | Expect a certain section to take fewer than the specified number of seconds.
timedSection :: Clock.NominalDiffTime -> ShakeTest t -> ShakeTest t
timedSection targetDiffTime block = do
    startTime <- ShakeTest $ liftIO Clock.getCurrentTime
    value <- block
    endTime <- ShakeTest $ liftIO Clock.getCurrentTime
    let actualDiffTime = Clock.diffUTCTime endTime startTime
    when (actualDiffTime > targetDiffTime) $ do
        throwError $ TimedSectionTookTooLong targetDiffTime actualDiffTime
    return value

-- | Example testing scenario.
example :: ShakeTest ()
example = do
    fooPath <- makeFile "src/Foo.daml" $ T.unlines
        [ "module Foo where"
        , "data Foo = Foo"
        , "  with"
        , "    bar : Party"
        , "    baz : Bool"
        , "  deriving (Eq, Show)"
        ]
    setFilesOfInterest [fooPath]
    expectNoErrors
