-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}

module DA.LanguageServer.Protocol where
import qualified Data.Map.Strict  as MS
import qualified Data.Aeson       as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.Text        as T
import qualified Data.Scientific  as Scientific

import           DA.LanguageServer.TH (deriveJSON)
import           DA.Prelude
import qualified DA.Service.JsonRpc  as JsonRpc
import qualified DA.Pretty           as P

-------------------------------------------------------------------------
-- Basic protocol types
-------------------------------------------------------------------------

-- | Text document URI
data DocumentUriTag
type DocumentUri = Tagged DocumentUriTag T.Text

-- | Process id of the server
data ProcessIdTag
type ProcessId = Tagged ProcessIdTag Int

-- | Position in a text document expressed as zero-based line and character offset.
data Position = Position
    { posLine      :: !Int
      -- ^ Zero-based line position in the document.
    , posCharacter :: !Int
      -- ^ Zero-based character offset on the line.
    }

-- | A range in a text document expressed as (zero-based) start and end positions.
data Range = Range
    { _start :: !Position
      -- ^ The start position of the range.
    , _end   :: !Position
      -- ^ The end position of the range.
    }

-- | Represents a location inside a resource, such as a line inside a text file.
data Location = Location
    { locUri   :: !DocumentUri
      -- ^ The uri of the document.
    , locRange :: !Range
      -- ^ The range within the document.
    }

-- | Represents a diagnostic, such as a compiler error or warning.
-- Diagnostic objects are only valid in the scope of a resource.
data Diagnostic = Diagnostic
    { diagRange    :: !Range
      -- ^ The range to which the diagnostic applies.
    , diagSeverity :: !(Maybe DiagnosticSeverity)
      -- ^ Optional severity for the diagnostic message.
    , diagCode     :: !(Maybe Int)
      -- ^ Optional code for the diagnostic.
    , diagSource   :: !(Maybe T.Text)
      -- ^ Human-readable description for the source of the diagnostic,
      -- for example 'parser'.
    , diagMessage  :: !T.Text
      -- ^ The diagnostic's message.
    }

-- | The severity attached to a diagnostic.
--
--   NOTE: The constructors must be in this specific order as we use
--   the Enum instance to convert them to numeric values.
--
data DiagnosticSeverity
   = None | Error | Warning | Information | Hint
    deriving (Enum)

data PublishDiagnosticsParams = PublishDiagnosticsParams
    { sdpUri         :: !DocumentUri
    , sdpDiagnostics :: ![Diagnostic]
    }

-- | Represents a reference to a command. Provides a title which will be used to
--   represent a command in the UI and, optionally, an array of arguments which will
--   be passed to the command handler function when invoked.
data Command = Command
    { cmdTitle     :: !T.Text
      -- ^ Command title, like 'save'.
    , cmdCommand   :: !T.Text
      -- ^ Command identifier
    , cmdArguments :: !(Maybe [Aeson.Value])
      -- ^ Arguments to the command handler
    }

-- | A textual edit applicable to a text document.
data TextEdit = TextEdit
    { teRange   :: !Range
      -- ^ Range of the text to be manipulated.
    , teNewText :: !T.Text
      -- ^ The text to be inserted. To delete use an empty string.
    }

-- | A workspace edit represents changes to many resources managed in the workspace.
data WorkspaceEdit = WorkspaceEdit
    { weChanges :: !(MS.Map String [TextEdit]) }

-- | Text documents are identified using a URI.
--   On the protocol level, URIs are passed as strings.
newtype TextDocumentIdentifier = TextDocumentIdentifier
    { tdidUri :: DocumentUri }

-- | Text document item represents a document transferred from the client to the server.
data TextDocumentItem = TextDocumentItem
    { tdiUri        :: !DocumentUri
      -- ^ The text document's URI.
    , tdiLanguageId :: !T.Text
      -- ^ The document's language identifier.
    , tdiVersion    :: !Int
      -- ^ Monotonically increasing version number of the text document.
    , tdiText       :: !T.Text
      -- ^ Content of the opened text document.
    }

-- | Text document identifier pointing to a specific version of the document.
data VersionedTextDocumentIdentifier = VersionedTextDocumentIdentifier
    { vtdiVersion :: !Int
      -- ^ Version number of the document
    , vtdiUri :: !DocumentUri
      -- ^ The text document's URI
    }

-- | Defines the document synchronization mechanism.
--
--   NOTE: The constructors must be in this specific order as we use
--   the Enum instance to convert them to numeric values.
--
data TextDocumentSyncKind =
      SyncNone        -- ^ Documents should not be synchronized.
    | SyncFull        -- ^ Documents should be synchronized by sending full content.
    | SyncIncremental -- ^ Documents should be synchronized incrementally.
  deriving (Enum)


--   NOTE: The constructors must be in this specific order as we use
--   the Enum instance to convert them to numeric values.
data CompletionItemKind =
      CIText
    | CIMethod
    | CIFunction
    | CIConstructor
    | CIField
    | CIVariable
    | CIClass
    | CIInterface
    | CIModule
    | CIProperty
    | CIUnit
    | CIValue
    | CIEnum
    | CIKeyword
    | CISnippet
    | CIColor
    | CIFile
    | CIReference
    deriving (Enum)

data CompletionItem = CompletionItem
    { ciLabel         :: !T.Text
    , ciKind          :: !(Maybe CompletionItemKind)
    , ciDetail        :: !(Maybe T.Text)
    , ciDocumentation :: !(Maybe T.Text)
    , ciSortText      :: !(Maybe T.Text)
    , ciFilterText    :: !(Maybe T.Text)
    , ciInsertText    :: !(Maybe T.Text)
    , ciTextEdit      :: !(Maybe TextEdit)
    , ciData          :: !(Maybe Aeson.Value)
    }

data CompletionList = CompletionList
    { clIsIncomplete :: !Bool
      -- ^ The list is incomplete, further typing would recompute.
    , clItems        :: ![CompletionItem]
    }

-- | The completion options supported by the server.
data CompletionOptions = CompletionOptions
    { coResolveProvider   :: !Bool
      -- ^ Server provides support to resolve additional information
      -- for a completion item.
    , coTriggerCharacters :: !(Maybe [T.Text])
      -- ^ The characters that trigger automatic completion.
    }

-- | Signature information represents the signature of something callable.
--   A signature can have a label, like a function-name, a doc-comment, and
--   a set of parameters.
data SignatureInformation = SignatureInformation
    { siLabel         :: !T.Text
    , siDocumentation :: !T.Text
    , siParameters    :: ![ParameterInformation]
    }

data ParameterInformation = ParameterInformation
   { piLabel         :: !T.Text
   , piDocumentation :: !(Maybe T.Text)
   }

-- | The signature help options
newtype SignatureHelpOptions = SignatureHelpOptions
    { shoTriggerCharacters :: Maybe [T.Text]
      -- ^ The characters that trigger signature help automatically.
    }

-- | Code lens options
newtype CodeLensOptions = CodeLensOptions
    { cloResolveProvider :: Bool
      -- ^ Code lens has a resolve provider.
    }

-- | Formatting options.
data FormattingOptions = FormattingOptions
    { foTabSize :: !Int
    , foInsertSpaces :: !Bool
    }

-- | Format document on type options
data DocumentOnTypeFormattingOptions = DocumentOnTypeFormattingOptions
    { fmtFirstTriggerCharacter :: !T.Text
      -- ^ Character on which formatting should be triggered.
    , fmtMoreTriggerCharacter  :: ![T.Text]
    }


-- NOTE: The constructors must be in this specific order as we use
-- the Enum instance to convert them to numeric values.
data SymbolKind
    = SKFile
    | SKModule
    | SKNamespace
    | SKPackage
    | SKClass
    | SKMethod
    | SKProperty
    | SKField
    | SKConstructor
    | SKEnum
    | SKInterface
    | SKFunction
    | SKVariable
    | SKConstant
    | SKString
    | SKNumber
    | SKBoolean
    | SKArray
    deriving (Enum)


data SymbolInformation = SymbolInformation
    { skName          :: !T.Text
    , skKind          :: !SymbolKind
    , skLocation      :: !Location
    , skContainerName :: !(Maybe T.Text)
    }

-- | Server capabilities
data ServerCapabilities = ServerCapabilities
    { scTextDocumentSync                 :: !(Maybe TextDocumentSyncKind)
    , scHoverProvider                    :: !Bool
    , scCompletionProvider               :: !(Maybe CompletionOptions)
    , scSignatureHelpProvider            :: !(Maybe SignatureHelpOptions)
    , scDefinitionProvider               :: !Bool
    , scReferencesProvider               :: !Bool
    , scDocumentHighlightProvider        :: !Bool
    , scDocumentSymbolProvider           :: !Bool
    , scWorkspaceSymbolProvider          :: !Bool
    , scCodeActionProvider               :: !Bool
    , scCodeLensProvider                 :: !Bool
    , scDocumentFormattingProvider       :: !Bool
    , scDocumentRangeFormattingProvider  :: !Bool
    , scDocumentOnTypeFormattingProvider :: !Bool
    , scRenameProvider                   :: !Bool
    }

newtype MessageActionItem = MessageActionItem { maiTitle :: T.Text }

data MessageType = MessageNone | MessageError | MessageWarning | MessageInfo | MessageLog
    deriving (Enum)

-- | The document change notification is sent from the client to the server
-- to signal changes to a text document.
data TextDocumentContentChangeEvent = TextDocumentContentChangeEvent
    { tdcceRange :: !(Maybe Range)
    , tdcceRangeLength :: !(Maybe Int)
    , tdcceText :: !T.Text
    }

data FileEvent = FileEvent
    { feUri  :: !DocumentUri
    , feType :: !FileChangeType
    }

data FileChangeType = FileCreated | FileChanged | FileDeleted
    deriving (Enum)


-------------------------------------------------------------------------
-- Language server requests and results
-------------------------------------------------------------------------

-- | Request sent by the client to the server.
data ServerRequest
    = Initialize      !InitializeParams
    | Shutdown
    | KeepAlive
    | Completion      !TextDocumentPositionParams
    | SignatureHelp   !TextDocumentPositionParams
    | Hover           !TextDocumentPositionParams
    | Definition      !TextDocumentPositionParams
    | References      !ReferenceParams
    | CodeLens        !CodeLensParams
    | Rename          !RenameParams
    | DocumentSymbol  !TextDocumentParams
    | WorkspaceSymbol !WorkspaceSymbolParams
    | Formatting      !DocumentFormattingParams
    | Upgrade         !UpgradeParams
    | UnknownRequest  !T.Text !Aeson.Value

-- | Server errors
data ServerError
    = ParseError
    | InvalidRequest
    | MethodNotFound
    | InvalidParams
    | InternalError

serverErrorToErrorObj :: ServerError -> JsonRpc.ErrorObj
serverErrorToErrorObj = \case
    ParseError     -> JsonRpc.ErrorObj "Parse error"        (-32700) Aeson.Null
    InvalidRequest -> JsonRpc.ErrorObj "Invalid request"    (-32600) Aeson.Null
    MethodNotFound -> JsonRpc.ErrorObj "Method not found"   (-32601) Aeson.Null
    InvalidParams  -> JsonRpc.ErrorObj "Invalid parameters" (-32602) Aeson.Null
    InternalError  -> JsonRpc.ErrorObj "Internal error"     (-32603) Aeson.Null


data ServerNotification
    = DidOpenTextDocument   DidOpenTextDocumentParams
    | DidChangeTextDocument DidChangeTextDocumentParams
    | DidCloseTextDocument  DidCloseTextDocumentParams
    | DidSaveTextDocument   DidSaveTextDocumentParams
    | UnknownNotification   T.Text Aeson.Value

instance JsonRpc.FromRequest ServerRequest where
    parseParams = \case
        "initialize"                         -> parseTo Initialize
        "shutdown"                           -> Just $ const $ return Shutdown
        "textDocument/completion"            -> parseTo Completion
        "textDocument/hover"                 -> parseTo Hover
        "textDocument/signatureHelp"         -> parseTo SignatureHelp
        "textDocument/definition"            -> parseTo Definition
        "textDocument/references"            -> parseTo References
        "textDocument/codeLens"              -> parseTo CodeLens
        "textDocument/rename"                -> parseTo Rename
        "textDocument/formatting"            -> parseTo Formatting
        "textDocument/documentSymbol"        -> parseTo DocumentSymbol
        "workspace/symbol"                   -> parseTo WorkspaceSymbol
        "daml/upgrade"                       -> parseTo Upgrade
        "daml/keepAlive"                     -> Just $ const $ return KeepAlive
        method                               -> Just $ return . UnknownRequest method
      where
        parseTo ctor = Just $ fmap ctor . Aeson.parseJSON

instance JsonRpc.FromRequest ServerNotification where
    parseParams = \case
        "textDocument/didOpen"    -> parseTo DidOpenTextDocument
        "textDocument/didChange"  -> parseTo DidChangeTextDocument
        "textDocument/didClose"   -> parseTo DidCloseTextDocument
        "textDocument/didSave"    -> parseTo DidSaveTextDocument
        method                    -> Just $ return . UnknownNotification method
      where
        parseTo ctor = Just $ fmap ctor . Aeson.parseJSON



-- | Initialization parameters
data InitializeParams = InitializeParams
    { ipProcessId :: !ProcessId
    , ipRootPath  :: !(Maybe FilePath)
    }

-- | Initialization result sent to the client.
data InitializeResult = InitializeResult
    { irCapabilities :: !ServerCapabilities
      -- ^ The provided language server capabilities.
      --
    }

-- | Initialization error data
data InitializeError = InitializeError
    { ieRetry :: !Bool
      -- ^ Indicates that the client should retry the initialization.
    }




data TextDocumentPositionParams = TextDocumentPositionParams
    { tdppTextDocument :: !TextDocumentIdentifier
    , tdppPosition     :: !Position
    }

data TextDocumentParams = TextDocumentParams
    { tdpTextDocument :: !TextDocumentIdentifier
    }

data RenameParams = RenameParams
    { renamerparamsTextDocument :: !TextDocumentIdentifier
    , renamerparamsPosition     :: !Position
    , renamerparamsNewName      :: !T.Text
    }

-- | An identifier for a programming language.
data LanguageIdentifierTag = LanguageIdentifierTag
type LanguageIdentifier = Tagged LanguageIdentifierTag T.Text

-- | Either a 'T.Text', or a 'T.Text' in a certain programming language.
data MarkedString
    = MarkedString !T.Text
    | MarkedStringWithLanguage
        { mswlLanguage :: !LanguageIdentifier
        , mswlValue    :: !T.Text
        }

data HoverResult = HoverResult
    { hrContents :: ![MarkedString]
    , hrRange    :: !(Maybe Range)
    }

data SignatureHelpResult = SignatureHelpResult
    { shrSignatures      :: ![SignatureInformation]
    , shrActiveSignature :: !(Maybe Int)
    , shrActiveParameter :: !(Maybe Int)
    }

data ReferenceParams = ReferenceParams
    { rpTextDocument :: !TextDocumentIdentifier
    , rpPosition     :: !Position
    , rpContext      :: !ReferenceContext
    }

newtype ReferenceContext = ReferenceContext
    { rcIncludeDeclaration :: Bool }


data CodeLensParams = CodeLensParams
    { clpTextDocument :: !TextDocumentIdentifier }

data CodeLensEntry = CodeLensEntry
    { cleRange :: !Range
      -- ^ The range in which this code lens is valid. Should only span a single line.
    , cleCommand :: !(Maybe Command)
      -- ^ The command this code lens represents.
    , cleData :: !(Maybe Aeson.Value)
      -- ^ A data entry field that is preserved on a code lens item between
      -- a code lens and a code lens resolve request.
    }

data WorkspaceSymbolParams = WorkspaceSymbolParams
    { wspQuery :: !T.Text
    }

data UpgradeParams = UpgradeParams
    { documentUri :: !TextDocumentIdentifier
    }

data DocumentFormattingParams = DocumentFormattingParams
    { dfpTextDocument :: !TextDocumentIdentifier
    , dfpOptions      :: !FormattingOptions
    }

-- Notification parameters
--------------------------

data DidOpenTextDocumentParams = DidOpenTextDocumentParams
    { dotdpTextDocument :: !TextDocumentItem
    }

data DidChangeTextDocumentParams = DidChangeTextDocumentParams
    { dctdpTextDocument   :: !VersionedTextDocumentIdentifier
    , dctdpContentChanges :: ![TextDocumentContentChangeEvent]
    }

-- | The document close notification is sent from the client to the server when
-- the document got closed in the client. The document's truth now exists
-- where the document's uri points to (e.g. if the document's uri is a file uri
-- the truth now exists on disk).

newtype DidCloseTextDocumentParams = DidCloseTextDocumentParams
    { cltdpTextDocument :: TextDocumentIdentifier
    }

-- | The document save notification is sent from the client to the server when
-- the document got saved in the client.

newtype DidSaveTextDocumentParams = DidSaveTextDocumentParams
    { stdpTextDocument :: TextDocumentIdentifier
    }



-- | The watched files notification is sent from the client to the server when
-- the client detects changes to file watched by the lanaguage client.
newtype DidChangeWatchedFilesParams = DidChangeWatchedFilesParams
    { cwfpChanges :: [FileEvent]
    }

-------------------------------------------------------------------------
-- Client (IDE) requests and responses
-------------------------------------------------------------------------

-- | Request sent by the language server to the client.
data ClientRequest =
     ShowMessageRequest ShowMessageRequestParams

instance JsonRpc.ToRequest ClientRequest where
    requestMethod  (ShowMessageRequest _) = "window/showMessageRequest"
    requestIsNotif = const False

instance Aeson.ToJSON ClientRequest where
    toJSON req = case req of
      ShowMessageRequest params -> Aeson.toJSON params

-- | Notification sent by the language server to the client.
data ClientNotification
    = ShowMessage LogMessageParams
    | LogMessage LogMessageParams
    | SendTelemetry Aeson.Value
    | PublishDiagnostics PublishDiagnosticsParams
    | CustomNotification T.Text Aeson.Value

instance JsonRpc.ToRequest ClientNotification where
    requestMethod (ShowMessage _)         = "window/showMessage"
    requestMethod (LogMessage _)          = "window/logMessage"
    requestMethod (SendTelemetry _)       = "telemetry/event"
    requestMethod (PublishDiagnostics _)  = "textDocument/publishDiagnostics"
    requestMethod (CustomNotification method _) = method
    requestIsNotif = const True

instance Aeson.ToJSON ClientNotification where
    toJSON req = case req of
      ShowMessage params        -> Aeson.toJSON params
      LogMessage params         -> Aeson.toJSON params
      SendTelemetry value       -> value
      PublishDiagnostics params -> Aeson.toJSON params
      CustomNotification _ value -> value

-- | The show message request is sent from the server to the client to
-- show a particular message and to ask the client to select an action
-- and wait for the answer.
data ShowMessageRequestParams = ShowMessageRequestParams
    { smrpType    :: !MessageType
    , smrpMessage :: !T.Text
    , smrpActions :: !(Maybe [MessageActionItem])
    }

-- | The log message notification is sent from the server to the client to ask
-- the client to log a particular message.
data LogMessageParams = LogMessageParams
    { lmpType    :: !MessageType
    , lmpMessage :: !T.Text
    }

-------------------------------------------------------------------------
-- Instances
-------------------------------------------------------------------------

concatSequenceA
    [ deriveJSON ''CompletionItem
    , deriveJSON ''CodeLensEntry
    , deriveJSON ''CodeLensParams
    , deriveJSON ''Command
    , deriveJSON ''CompletionOptions
    , deriveJSON ''Diagnostic
    , deriveJSON ''DidChangeTextDocumentParams
    , deriveJSON ''DidCloseTextDocumentParams
    , deriveJSON ''DidOpenTextDocumentParams
    , deriveJSON ''DidSaveTextDocumentParams
    , deriveJSON ''DocumentFormattingParams
    , deriveJSON ''FormattingOptions
    , deriveJSON ''HoverResult
    , deriveJSON ''InitializeError
    , deriveJSON ''InitializeParams
    , deriveJSON ''InitializeResult
    , deriveJSON ''Location
    , deriveJSON ''LogMessageParams
    , deriveJSON ''MessageActionItem
    , deriveJSON ''ParameterInformation
    , deriveJSON ''Position
    , deriveJSON ''PublishDiagnosticsParams
    , deriveJSON ''Range
    , deriveJSON ''ReferenceContext
    , deriveJSON ''ReferenceParams
    , deriveJSON ''RenameParams
    , deriveJSON ''ServerCapabilities
    , deriveJSON ''ServerNotification
    , deriveJSON ''ServerRequest
    , deriveJSON ''ShowMessageRequestParams
    , deriveJSON ''SignatureHelpOptions
    , deriveJSON ''SignatureHelpResult
    , deriveJSON ''SignatureInformation
    , deriveJSON ''SymbolInformation
    , deriveJSON ''TextDocumentContentChangeEvent
    , deriveJSON ''TextDocumentIdentifier
    , deriveJSON ''TextDocumentItem
    , deriveJSON ''TextDocumentParams
    , deriveJSON ''TextDocumentPositionParams
    , deriveJSON ''TextEdit
    , deriveJSON ''UpgradeParams
    , deriveJSON ''VersionedTextDocumentIdentifier
    , deriveJSON ''WorkspaceEdit
    , deriveJSON ''WorkspaceSymbolParams
    ]

enumToJSON :: Enum a => a -> Aeson.Value
enumToJSON = Aeson.toJSON . fromEnum

enumFromJSON :: Enum a => String -> Aeson.Value -> Aeson.Parser a
enumFromJSON name value = case value of
    v@(Aeson.Number n) ->
        case (Scientific.floatingOrInteger n :: Either Double Int) of
          Left _        -> Aeson.typeMismatch name v
          Right integer -> return $ toEnum integer

    invalid -> Aeson.typeMismatch name invalid

instance Aeson.ToJSON TextDocumentSyncKind where
    toJSON = enumToJSON
instance Aeson.FromJSON TextDocumentSyncKind where
    parseJSON = enumFromJSON "TextDocumentSyncKind"

instance Aeson.ToJSON CompletionItemKind where
    toJSON = enumToJSON
instance Aeson.FromJSON CompletionItemKind where
    parseJSON = enumFromJSON "CompletionItemKind"

instance Aeson.ToJSON SymbolKind where
    toJSON = enumToJSON
instance Aeson.FromJSON SymbolKind where
    parseJSON = enumFromJSON "SymbolKind"

instance Aeson.ToJSON DiagnosticSeverity where
    toJSON = enumToJSON
instance Aeson.FromJSON DiagnosticSeverity where
    parseJSON = enumFromJSON "DiagnosticSeverity"

instance Aeson.ToJSON MessageType where
    toJSON = enumToJSON
instance Aeson.FromJSON MessageType where
    parseJSON = enumFromJSON "MessageType"

instance Aeson.ToJSON MarkedString where
    toJSON markedString =
      case markedString of
        MarkedString str -> Aeson.String str
        MarkedStringWithLanguage language value ->
            Aeson.object
                [ "language" Aeson..= language
                , "value"    Aeson..= value
                ]

instance Aeson.FromJSON MarkedString where
    parseJSON markedString =
        case markedString of
            Aeson.String s -> return $ MarkedString s
            Aeson.Object o -> do
                language <- o Aeson..: "language"
                value <- o Aeson..: "value"
                return $ MarkedStringWithLanguage language value
            _ -> fail "Should be type MarkedString = string | { language: string; value: string };"

----------------------------------------------------------------------------------------------------
-- Pretty printing
----------------------------------------------------------------------------------------------------

instance P.Pretty Position where
    pPrint pos = P.int (posLine pos + 1) <> P.colon <> P.int (posCharacter pos + 1)

instance P.Pretty Range where
    pPrint range = P.pretty (_start range) <> P.char '-' <> P.pretty (_end range)

instance P.Pretty Location where
    pPrint loc = P.text (unTagged (locUri loc)) P.<-> P.parens (P.pretty (locRange loc))
