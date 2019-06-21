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
  NormalizedUri,
  LSP.toNormalizedUri,
  LSP.fromNormalizedUri,
  NormalizedFilePath,
  toNormalizedFilePath,
  fromNormalizedFilePath,
  noLocation,
  noRange,
  noFilePath,
  ideErrorText,
  ideErrorPretty,
  errorDiag,
  ideTryIOException,
  showDiagnostics,
  showDiagnosticsColored,
  defDiagnostic,
  filePathToUri,
  filePathToUri',
  uriToFilePath',
  ProjectDiagnostics,
  emptyDiagnostics,
  setStageDiagnostics,
  getAllDiagnostics,
  filterDiagnostics,
  getFileDiagnostics,
  prettyDiagnostics
  ) where

import Control.DeepSeq
import Control.Exception
import Data.Either.Combinators
import Data.Maybe as Maybe
import Data.Foldable
import Data.Hashable
import qualified Data.Map as Map
import Data.String
import qualified Data.Text as T
import Data.Text.Prettyprint.Doc.Syntax
import qualified Data.SortedList as SL
import System.FilePath
import qualified Text.PrettyPrint.Annotated.HughesPJClass as Pretty
import qualified Language.Haskell.LSP.Types as LSP
import Language.Haskell.LSP.Types as LSP (
    DiagnosticSeverity(..)
  , Diagnostic(..)
  , filePathToUri
  , List(..)
  , DiagnosticRelatedInformation(..)
  , NormalizedUri(..)
  , Uri(..)
  , toNormalizedUri
  , fromNormalizedUri
  )
import Language.Haskell.LSP.Diagnostics

import Development.IDE.Types.Location

-- | Newtype wrapper around FilePath that always has normalized slashes.
newtype NormalizedFilePath = NormalizedFilePath FilePath
    deriving (Eq, Ord, Show, Hashable, NFData)

instance IsString NormalizedFilePath where
    fromString = toNormalizedFilePath

toNormalizedFilePath :: FilePath -> NormalizedFilePath
toNormalizedFilePath "" = NormalizedFilePath ""
toNormalizedFilePath fp = NormalizedFilePath $ normalise' fp
    where
        -- We do not use System.FilePath’s normalise here since that
        -- also normalises things like the case of the drive letter
        -- which NormalizedUri does not normalise so we get VFS lookup failures.
        normalise' :: FilePath -> FilePath
        normalise' = map (\c -> if isPathSeparator c then pathSeparator else c)

fromNormalizedFilePath :: NormalizedFilePath -> FilePath
fromNormalizedFilePath (NormalizedFilePath fp) = fp

-- | We use an empty string as a filepath when we don’t have a file.
-- However, haskell-lsp doesn’t support that in uriToFilePath and given
-- that it is not a valid filepath it does not make sense to upstream a fix.
-- So we have our own wrapper here that supports empty filepaths.
uriToFilePath' :: Uri -> Maybe FilePath
uriToFilePath' uri
    | uri == filePathToUri "" = Just ""
    | otherwise = LSP.uriToFilePath uri

filePathToUri' :: NormalizedFilePath -> NormalizedUri
filePathToUri' = toNormalizedUri . filePathToUri . fromNormalizedFilePath

ideErrorText :: NormalizedFilePath -> T.Text -> FileDiagnostic
ideErrorText fp = errorDiag fp "Ide Error"

ideErrorPretty :: Pretty.Pretty e => NormalizedFilePath -> e -> FileDiagnostic
ideErrorPretty fp = ideErrorText fp . T.pack . Pretty.prettyShow

errorDiag :: NormalizedFilePath -> T.Text -> T.Text -> FileDiagnostic
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

ideTryIOException :: NormalizedFilePath -> IO a -> IO (Either FileDiagnostic a)
ideTryIOException fp act =
  mapLeft
      (\(e :: IOException) -> ideErrorText fp $ T.pack $ show e)
      <$> try act

-- | Human readable diagnostics for a specific file.
--
--   This type packages a pretty printed, human readable error message
--   along with the related source location so that we can display the error
--   on either the console or in the IDE at the right source location.
--
type FileDiagnostics = (NormalizedFilePath, [Diagnostic])
type FileDiagnostic = (NormalizedFilePath, Diagnostic)

prettyRange :: Range -> Doc SyntaxClass
prettyRange Range{..} = f _start <> "-" <> f _end
    where f Position{..} = pretty (_line+1) <> colon <> pretty _character

stringParagraphs :: T.Text -> Doc a
stringParagraphs = vcat . map (fillSep . map pretty . T.words) . T.lines

showDiagnostics :: [FileDiagnostic] -> T.Text
showDiagnostics = srenderPlain . prettyDiagnostics

showDiagnosticsColored :: [FileDiagnostic] -> T.Text
showDiagnosticsColored = srenderColored . prettyDiagnostics


prettyDiagnostics :: [FileDiagnostic] -> Doc SyntaxClass
prettyDiagnostics = vcat . map prettyDiagnostic

prettyDiagnostic :: FileDiagnostic -> Doc SyntaxClass
prettyDiagnostic (fp, LSP.Diagnostic{..}) =
    vcat
        [ slabel_ "File:    " $ pretty (fromNormalizedFilePath fp)
        , slabel_ "Range:   " $ prettyRange _range
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

getDiagnosticsFromStore :: StoreItem -> [Diagnostic]
getDiagnosticsFromStore (StoreItem _ diags) =
    toList =<< Map.elems diags

-- | This represents every diagnostic in a LSP project, the stage type variable is
--   the type of the compiler stages, in this project that is always the Key data
--   type found in Development.IDE.State.Shake
newtype ProjectDiagnostics stage = ProjectDiagnostics {getStore :: DiagnosticStore}
    deriving Show

emptyDiagnostics :: ProjectDiagnostics stage
emptyDiagnostics = ProjectDiagnostics mempty

-- | Sets the diagnostics for a file and compilation step
--   if you want to clear the diagnostics call this with an empty list
setStageDiagnostics ::
  Show stage =>
  NormalizedFilePath ->
  Maybe Int ->
  -- ^ the time that the file these diagnostics originate from was last edited
  stage ->
  [LSP.Diagnostic] ->
  ProjectDiagnostics stage ->
  ProjectDiagnostics stage
setStageDiagnostics fp timeM stage diags (ProjectDiagnostics ds) =
    ProjectDiagnostics $ updateDiagnostics ds uri timeM diagsBySource
    where
        diagsBySource = Map.singleton (Just $ T.pack $ show stage) (SL.toSortedList diags)
        uri = filePathToUri' fp

fromUri :: LSP.NormalizedUri -> NormalizedFilePath
fromUri = toNormalizedFilePath . fromMaybe noFilePath . uriToFilePath' . fromNormalizedUri

getAllDiagnostics ::
    ProjectDiagnostics stage ->
    [FileDiagnostic]
getAllDiagnostics =
    concatMap (\(k,v) -> map (fromUri k,) $ getDiagnosticsFromStore v) . Map.toList . getStore

getFileDiagnostics ::
    NormalizedFilePath ->
    ProjectDiagnostics stage ->
    [LSP.Diagnostic]
getFileDiagnostics fp ds =
    maybe [] getDiagnosticsFromStore $
    Map.lookup (filePathToUri' fp) $
    getStore ds

filterDiagnostics ::
    (NormalizedFilePath -> Bool) ->
    ProjectDiagnostics stage ->
    ProjectDiagnostics stage
filterDiagnostics keep =
    ProjectDiagnostics .
    Map.filterWithKey (\uri _ -> maybe True (keep . toNormalizedFilePath) $ uriToFilePath' $ fromNormalizedUri uri) .
    getStore
