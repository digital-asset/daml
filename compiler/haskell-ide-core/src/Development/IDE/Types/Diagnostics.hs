-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE BlockArguments #-}
module Development.IDE.Types.Diagnostics (
  LSP.Diagnostic(..),
  FileDiagnostics,
  FileDiagnostic,
  Location(..),
  Range(..),
  LSP.DiagnosticSeverity(..),
  Position(..),
  DiagnosticStore,
  DiagnosticRelatedInformation(..),
  List(..),
  StoreItem(..),
  Uri(..),
  noLocation,
  noRange,
  noFilePath,
  ideErrorText,
  ideErrorPretty,
  errorDiag,
  ideTryIOException,
  showDiagnostics,
  showDiagnosticsColored,
  prettyDiagnosticStore,
  defDiagnostic,
  addDiagnostics,
  filterSeriousErrors,
  filePathToUri,
  getDiagnosticsFromStore,
  Diagnostics,
  setStageDiagnostics,
  getStageDiagnostics,
  getAllDiagnostics,
  filterDiagnostics,
  getFileDiagnostics,
  prettyDiagnostics
  ) where

import Control.Exception
import Data.Either.Combinators
import Data.Maybe as Maybe
import Data.Foldable
import qualified Data.Map as Map
import Data.String (IsString)
import Data.Time.Clock
import Data.Time.Clock.POSIX
import qualified Data.Text as T
import Data.Text.Prettyprint.Doc.Syntax
import qualified Data.SortedList as SL
import System.Directory
import qualified Text.PrettyPrint.Annotated.HughesPJClass as Pretty
import           Language.Haskell.LSP.Types as LSP (
    DiagnosticSeverity(..)
  , Diagnostic(..)
  , filePathToUri
  , uriToFilePath
  , List(..)
  , DiagnosticRelatedInformation(..)
  , Uri(..)
  )
import Language.Haskell.LSP.Diagnostics
import GHC.Stack

import Development.IDE.Types.Location

ideErrorText :: FilePath -> T.Text -> FileDiagnostic
ideErrorText fp = errorDiag fp "Ide Error"

ideErrorPretty :: Pretty.Pretty e => FilePath -> e -> FileDiagnostic
ideErrorPretty fp = ideErrorText fp . T.pack . Pretty.prettyShow

errorDiag :: FilePath -> T.Text -> T.Text -> FileDiagnostic
errorDiag fp src msg =
  (fp,  diagnostic noRange LSP.DsError src msg)

-- | This is for compatibility with our old diagnostic type
diagnostic :: Range
           -> LSP.DiagnosticSeverity
           -> T.Text -- ^ source
           -> T.Text -- ^ message
           -> LSP.Diagnostic
diagnostic rng sev src msg
    = LSP.Diagnostic {
          _range = rng,
          _severity = Just sev,
          _code = Nothing,
          _source = Just src,
          _message = msg,
          _relatedInformation = Nothing
          }

-- | Any optional field is instantiated to Nothing
defDiagnostic ::
  Range ->
  T.Text -> -- ^ error message
  LSP.Diagnostic
defDiagnostic _range _message = LSP.Diagnostic {
    _range
  , _message
  , _severity = Nothing
  , _code = Nothing
  , _source = Nothing
  , _relatedInformation = Nothing
  }

filterSeriousErrors ::
    FilePath ->
    [LSP.Diagnostic] ->
    [LSP.Diagnostic]
filterSeriousErrors fp =
    filter (maybe False hasSeriousErrors . LSP._relatedInformation)
    where
        hasSeriousErrors :: List DiagnosticRelatedInformation -> Bool
        hasSeriousErrors (List a) = any ((/=) uri . _uri . _location) a
        uri = LSP.filePathToUri fp

addDiagnostics ::
  FilePath ->
  [LSP.Diagnostic] ->
  DiagnosticStore -> DiagnosticStore
addDiagnostics fp diags ds =
    updateDiagnostics
    ds
    (LSP.filePathToUri fp)
    Nothing $
    partitionBySource diags

ideTryIOException :: FilePath -> IO a -> IO (Either FileDiagnostic a)
ideTryIOException fp act =
  mapLeft (\(e :: IOException) -> ideErrorText fp $ T.pack $ show e) <$> try act

-- | Human readable diagnostics for a specific file.
--
--   This type packages a pretty printed, human readable error message
--   along with the related source location so that we can display the error
--   on either the console or in the IDE at the right source location.
--
type FileDiagnostics = (FilePath, [Diagnostic])
type FileDiagnostic = (FilePath, Diagnostic)

prettyRange :: Range -> Doc SyntaxClass
prettyRange Range{..} = f _start <> "-" <> f _end
    where f Position{..} = pretty (_line+1) <> colon <> pretty _character

stringParagraphs :: T.Text -> Doc a
stringParagraphs = vcat . map (fillSep . map pretty . T.words) . T.lines

showDiagnostics :: [FileDiagnostic] -> T.Text
showDiagnostics = srenderPlain . vcat . map prettyDiagnostic

showDiagnosticsColored :: [FileDiagnostic] -> T.Text
showDiagnosticsColored = srenderColored . vcat . map prettyDiagnostic

prettyDiagnostic :: FileDiagnostic -> Doc SyntaxClass
prettyDiagnostic (fp, d) =
    vcat $ fileSC fp : diagSC d

fileSC :: FilePath -> Doc SyntaxClass
fileSC = slabel_ "File:    " . pretty

diagSC :: LSP.Diagnostic -> [Doc SyntaxClass]
diagSC LSP.Diagnostic{..} =
    [ slabel_ "Range:   " $ prettyRange _range
    , slabel_ "Source:  " $ pretty _source
    , slabel_ "Severity:" $ pretty $ show sev
    , slabel_ "Message: "
        $ case sev of
              LSP.DsError -> annotate ErrorSC
              LSP.DsWarning -> annotate WarningSC
              LSP.DsInfo -> annotate InfoSC
              LSP.DsHint -> annotate HintSC
        $ stringParagraphs _message
    , slabel_ "Code:" $ pretty _code
    ]
    where
        sev = fromMaybe LSP.DsError _severity

prettyDiagnosticStore :: DiagnosticStore -> Doc SyntaxClass
prettyDiagnosticStore ds =
    vcat $
    map (\(uri, diags) -> prettyFileDiagnostics (fromUri uri, diags)) $
    Map.assocs $
    Map.map getDiagnosticsFromStore ds

prettyFileDiagnostics :: FileDiagnostics -> Doc SyntaxClass
prettyFileDiagnostics (filePath, diags) =
    slabel_ "Compiler error in" $ vcat
        [ slabel_ "File:" $ pretty filePath
        , slabel_ "Errors:" $ vcat $ map (prettyDiagnostic . (filePath,)) diags
        ]

getDiagnosticsFromStore :: StoreItem -> [Diagnostic]
getDiagnosticsFromStore (StoreItem _ diags) =
    toList =<< Map.elems diags

-- | A wrapper around the lsp diagnostics store, the constraint describes how compilation steps
--   are represented
newtype Diagnostics stage = Diagnostics {getStore :: DiagnosticStore}
    deriving Show

instance Semigroup (Diagnostics stage) where
    (Diagnostics a) <> (Diagnostics b) =
        Diagnostics $
        Map.unionWith (curry combineStores)
        a b where
        combineStores = \case
            (StoreItem Nothing x, StoreItem Nothing y) ->
                StoreItem Nothing $ Map.unionWith SL.union x y
            _ -> noVersions

instance Monoid (Diagnostics k) where
    mempty = emptyDiagnostics

prettyDiagnostics :: Diagnostics stage -> Doc SyntaxClass
prettyDiagnostics ds =
    slabel_ "Compiler errors in" $ vcat $ concatMap fileErrors storeContents where

    fileErrors :: (FilePath, [(T.Text, [LSP.Diagnostic])]) -> [Doc SyntaxClass]
    fileErrors (filePath, stages) =
        [ slabel_ "File:" $ pretty filePath
        , slabel_ "Errors:" $ vcat $ map prettyStage stages
        ]

    prettyStage :: (T.Text, [LSP.Diagnostic]) -> Doc SyntaxClass
    prettyStage (stage,diags) =
        slabel_ ("Stage: "<>T.unpack stage) $ vcat $ concatMap diagSC diags

    storeContents ::
        [(FilePath, [(T.Text, [LSP.Diagnostic])])]
        -- ^ Source File, Stage Source, Diags
    storeContents =
        map (\(uri, StoreItem _ si) ->
                 (fromMaybe dontKnow $ uriToFilePath uri, getDiags si))
            $ Map.assocs
            $ getStore
            $ removeEmptyStages ds

    dontKnow :: IsString s => s
    dontKnow = "<unknown>"

    getDiags :: DiagnosticsBySource -> [(T.Text, [LSP.Diagnostic])]
    getDiags = map (\(ds, diag) -> (fromMaybe dontKnow ds, toList diag)) . Map.assocs

emptyDiagnostics :: Diagnostics stage
emptyDiagnostics = Diagnostics mempty

-- | Sets the diagnostics for a file and compilation step
--   if you want to clear the diagnostics call this with an empty list
setStageDiagnostics ::
  Show stage =>
  FilePath ->
  Maybe UTCTime ->
  -- ^ the time that the file these diagnostics originate from was last edited
  stage ->
  [LSP.Diagnostic] ->
  Diagnostics stage ->
  IO (Diagnostics stage)
setStageDiagnostics fp timeM stage diags (Diagnostics ds) = do
    uri <- toUri fp
    let thisFile = Map.lookup uri ds
        addedStage :: StoreItem
        addedStage = StoreItem (Just posixTime) $ storeItem thisFile
    pure $ Diagnostics $ Map.insert uri addedStage ds where

    storeItem :: Maybe StoreItem -> DiagnosticsBySource
    storeItem = \case
        Nothing ->
            Map.singleton k v

        Just (StoreItem Nothing _) ->
            noVersions

        Just (StoreItem (Just prevTime) bySource) ->
            case compare posixTime prevTime of
                EQ -> insert bySource
                GT -> insert $ Map.map (SL.map tagOutdated)  bySource
                LT -> bySource

    -- Change the color of outdated diagnostics
    tagOutdated :: Diagnostic -> Diagnostic
    tagOutdated d = d{_severity = Just DsInfo}

    isOutdated :: Diagnostic -> Bool
    isOutdated Diagnostic{_severity} = _severity == Just DsInfo

    removeOutdated, insert :: DiagnosticsBySource -> DiagnosticsBySource
    removeOutdated = Map.filter (not . any isOutdated)

    insert bySource =
        let removeExpired =
                if Map.lookup k bySource == Just (SL.map tagOutdated v)
                then removeOutdated
                else id
        in Map.insert k v $ removeExpired bySource

    k = Just $ T.pack $ show stage
    v = SL.toSortedList diags

    posixTime :: Int
    posixTime = maybe 0 (fromEnum . utcTimeToPOSIXSeconds) timeM

noVersions :: HasCallStack => a
noVersions =
    error "All store items must have versions"

fromUri :: LSP.Uri -> FilePath
fromUri = fromMaybe noFilePath . uriToFilePath

-- TODO (MK) We canonicalize to make sure that the two files agree on use of
-- / and \ and other shenanigans.
-- haskell-lsp also has serious problems with relative file paths
toUri :: FilePath -> IO LSP.Uri
toUri = fmap filePathToUri . canonicalizePath

getAllDiagnostics ::
    Diagnostics stage ->
    [FileDiagnostic]
getAllDiagnostics =
    concatMap (\(k,v) -> map (fromUri k,) $ getDiagnosticsFromStore v) . Map.toList . getStore

getFileDiagnostics ::
    FilePath ->
    Diagnostics stage ->
    IO [LSP.Diagnostic]
getFileDiagnostics fp ds = do
    uri <- toUri fp
    pure $
        maybe [] getDiagnosticsFromStore $
        Map.lookup uri $
        getStore ds

getStageDiagnostics ::
    Show stage =>
    FilePath ->
    stage ->
    Diagnostics stage ->
    IO [LSP.Diagnostic]
getStageDiagnostics fp stage (Diagnostics ds) = do
    uri <- toUri fp
    pure $ fromMaybe [] $ do
        (StoreItem _ f) <- Map.lookup uri ds
        toList <$> Map.lookup (Just $ T.pack $ show stage) f

filterDiagnostics ::
    (FilePath -> Bool) ->
    Diagnostics stage ->
    Diagnostics stage
filterDiagnostics keep =
    Diagnostics .
    Map.filterWithKey (\file _ -> maybe False keep $ uriToFilePath file) .
    getStore .
    removeEmptyStages

removeEmptyStages ::
    Diagnostics key ->
    Diagnostics ke
removeEmptyStages =
    Diagnostics .
    Map.filter (not . null . getDiagnosticsFromStore) .
    Map.map (\(StoreItem s stages) -> StoreItem s $ Map.filter (not . null) stages ) .
    getStore
