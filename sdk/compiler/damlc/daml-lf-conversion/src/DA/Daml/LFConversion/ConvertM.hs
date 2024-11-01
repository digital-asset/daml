-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LFConversion.ConvertM (
    ConversionEnv(..),
    ConvertM(..),
    runConvertM,
    withRange,
    freshTmVar,
    resetFreshVarCounters,
    conversionWarning,
    conversionError,
    conversionDiagnostic,
    unsupported,
    unknown,
    unhandled,
    StandaloneWarning(..),
    StandaloneError(..),
    ErrorOrWarning(..),
    InvalidInterfaceError(..),
    damlWarningFlagParser,
    warnLargeTuplesFlag
  ) where

import           DA.Daml.UtilLF

import           Development.IDE.Types.Diagnostics
import           Development.IDE.Types.Location
import           Development.IDE.GHC.Util

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           DA.Daml.LF.Ast as LF
import           Data.Data hiding (TyCon)
import           Data.List.Extra
import qualified Data.Map.Strict as MS
import qualified Data.Text.Extended as T
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>), notNull)
import           DA.Pretty (renderPretty)

import DA.Daml.LF.TypeChecker.Error.WarningFlags

import "ghc-lib" TyCoRep

data ConversionEnv = ConversionEnv
  { convModuleFilePath :: !NormalizedFilePath
  , convRange :: !(Maybe SourceLoc)
  , convWarningFlags :: DamlWarningFlags ErrorOrWarning
  }

damlWarningFlagParser :: DamlWarningFlagParser ErrorOrWarning
damlWarningFlagParser =
  DamlWarningFlagParser
    { dwfpFlagParsers = [(warnLargeTuplesName, warnLargeTuplesFlag)]
    , dwfpDefault = \case
        LargeTuple _ -> AsWarning
    }

warnLargeTuplesName :: String
warnLargeTuplesName = "large-tuples"

warnLargeTuplesFlag :: DamlWarningFlagStatus -> DamlWarningFlag ErrorOrWarning
warnLargeTuplesFlag status = RawDamlWarningFlag
  { rfName = warnLargeTuplesName
  , rfStatus = status
  , rfFilter = \case
      LargeTuple _ -> True
  }

data ConversionState = ConversionState
    { freshTmVarCounter :: Int
    , warnings :: [FileDiagnostic]
    }

data ErrorOrWarning
  = LargeTuple Int

data Warning
  = StandaloneWarning StandaloneWarning
  | ErrorOrWarningAsWarning ErrorOrWarning

data Error
  = StandaloneError StandaloneError
  | ErrorOrWarningAsError ErrorOrWarning

data StandaloneWarning
  = PolymorphicTopLevelScript GHC.Var GHC.Type GHC.TyVar

data StandaloneError
  = Unhandled String Constr String
  | Unsupported String String
  | OnlySupportedOnDev String
  | UnknownPackage GHC.UnitId (MS.Map GHC.UnitId DalfPackage)
  | ScenariosNoLongerSupported GHC.Var GHC.Type
  | UnknownPrimitive String LF.Type
  | RawError String
  | NoViewFoundForInterface LF.TypeConName
  | InvalidInterface GHC.TyCon GHC.TyCon InvalidInterfaceError
  | CannotRequireNonInterface GHC.TyCon

data InvalidInterfaceError
  = NotATemplate GHC.TyCon
  | NotAnInterface GHC.TyCon
  | DoesNotMatchEnclosingTemplateDeclaration
  | NoViewDefined
  | MoreThanOneViewDefined

ppError :: Error -> String
ppError (StandaloneError err) = ppStandaloneError err
ppError (ErrorOrWarningAsError errOrWarn) = ppErrorOrWarning errOrWarn

ppWarning :: Warning -> String
ppWarning (StandaloneWarning warn) = ppStandaloneWarning warn
ppWarning (ErrorOrWarningAsWarning errOrWarn) = ppErrorOrWarning errOrWarn

ppStandaloneError :: StandaloneError -> String
ppStandaloneError = \case
  Unsupported typ x ->
    "Failure to process Daml program, this feature is not currently supported.\n" ++
    typ ++ "\n" ++
    prettyPrint x
  Unhandled typ constr x ->
    "Failure to process Daml program, this feature is not currently supported.\n" ++
    (typ ++ " with " ++ lower (show constr)) ++ "\n" ++
    x
  OnlySupportedOnDev feature ->
    feature <> " only available with --target=1.dev"
  UnknownPackage unitId pkgMap ->
    "Unknown package: " ++ GHC.unitIdString unitId
    ++ "\n" ++  "Loaded packages are:" ++ prettyPrint (MS.keys pkgMap)
  ScenariosNoLongerSupported name ty ->
    unlines
      [ "Scenarios are no longer supported."
      , "Instead, consider using Daml Script (https://docs.daml.com/daml-script/index.html)."
      , "When compiling " <> prettyPrint name <> " : " <> prettyPrint ty <> "."
      ]
  UnknownPrimitive x ty ->
    "Unknown primitive " ++ show x ++ " at type " ++ renderPretty ty
  RawError msg -> msg
  NoViewFoundForInterface intName ->
    "No view found for interface " <> renderPretty intName
  InvalidInterface iface tpl err ->
    unwords
      [ "Invalid 'interface instance"
      , prettyPrint iface
      , "for"
      , prettyPrint tpl <> "':"
      , ppInvalidInterfaceError err
      ]
  CannotRequireNonInterface tyCon ->
    "cannot require '" ++ prettyPrint tyCon ++ "' because it is not an interface"

ppInvalidInterfaceError :: InvalidInterfaceError -> String
ppInvalidInterfaceError (NotAnInterface iface) = "'" <> prettyPrint iface <> "' is not an interface"
ppInvalidInterfaceError (NotATemplate tpl) = "'" <> prettyPrint tpl <> "' is not a template"
ppInvalidInterfaceError DoesNotMatchEnclosingTemplateDeclaration = unwords
  [ "The template of this interface instance does not match the"
  , "enclosing template declaration."
  ]
ppInvalidInterfaceError NoViewDefined = "no view implementation defined"
ppInvalidInterfaceError MoreThanOneViewDefined = "more than one view implementation defined"

ppStandaloneWarning :: StandaloneWarning -> String
ppStandaloneWarning (PolymorphicTopLevelScript name ty tyVar) =
  unlines
    [ "This method is implicitly polymorphic. Top level polymorphic scripts will not be run as tests."
    , "If this is intentional, please write an explicit type signature with the `forall'"
    , "If not, either provide a specialised type signature, such as:"
    , showSDocUnsafe $ quotes $ ppr name <+> ":" <+> pprType (substUnit tyVar ty)
    , "Or apply the unit type to the top level `script' call:"
    , showSDocUnsafe $ quotes $ ppr name <+> "= script @() do ..."
    ]
  where
  substUnit :: GHC.TyVar -> GHC.Type -> GHC.Type
  substUnit tyVar ty = TyCoRep.substTy (setTvSubstEnv emptyTCvSubst $ mkVarEnv [(tyVar, TyConApp unitTyCon [])]) ty

ppErrorOrWarning :: ErrorOrWarning -> String
ppErrorOrWarning (LargeTuple n) = "Used tuple of size " <> show n <> ". Daml only has Show, Eq, Ord instances for tuples of size <= 5."

newtype ConvertM a = ConvertM (ReaderT ConversionEnv (StateT ConversionState (Except FileDiagnostic)) a)
  deriving (Functor, Applicative, Monad, MonadError FileDiagnostic, MonadState ConversionState, MonadReader ConversionEnv)

instance MonadFail ConvertM where
    fail = conversionError . RawError

-- The left case is for the single error thrown, the right case for a list of
-- non-fatal warnings
runConvertM :: ConversionEnv -> ConvertM a -> Either FileDiagnostic (a, [FileDiagnostic])
runConvertM s (ConvertM a) = runExcept $ do
  (a, convState) <- runStateT (runReaderT a s) st0
  pure (a, reverse $ warnings convState)
  where
    st0 = ConversionState
        { freshTmVarCounter = 0
        , warnings = []
        }

withRange :: Maybe SourceLoc -> ConvertM a -> ConvertM a
withRange r = local (\s -> s { convRange = r })

freshTmVar :: ConvertM LF.ExprVarName
freshTmVar = do
    n <- state (\st -> let k = freshTmVarCounter st + 1 in (k, st{freshTmVarCounter = k}))
    pure $ LF.ExprVarName ("$$v" <> T.show n)

resetFreshVarCounters :: ConvertM ()
resetFreshVarCounters = modify' (\st -> st{freshTmVarCounter = 0})

---------------------------------------------------------------------
-- FAILURE REPORTING

conversionError :: StandaloneError -> ConvertM e
conversionError = conversionErrorRaw . StandaloneError

conversionErrorRaw :: Error -> ConvertM e
conversionErrorRaw err = do
  ConversionEnv{..} <- ask
  throwError $ (convModuleFilePath,ShowDiag,) Diagnostic
      { _range = maybe noRange sourceLocToRange convRange
      , _severity = Just DsError
      , _source = Just "Core to Daml-LF"
      , _message = T.pack (ppError err)
      , _code = Nothing
      , _relatedInformation = Nothing
      , _tags = Nothing
      }

conversionWarning :: StandaloneWarning -> ConvertM ()
conversionWarning = conversionWarningRaw . StandaloneWarning

conversionWarningRaw :: Warning -> ConvertM ()
conversionWarningRaw msg = do
  ConversionEnv{..} <- ask
  let diagnostic = Diagnostic
        { _range = maybe noRange sourceLocToRange convRange
        , _severity = Just DsWarning
        , _source = Just "Core to Daml-LF"
        , _message = T.pack (ppWarning msg)
        , _code = Nothing
        , _relatedInformation = Nothing
        , _tags = Nothing
        }
      fileDiagnostic = (convModuleFilePath, ShowDiag, diagnostic)
  modify $ \s -> s { warnings = fileDiagnostic : warnings s }

class IsErrorOrWarning e where
  conversionDiagnostic :: e -> ConvertM ()

instance IsErrorOrWarning StandaloneError where
  conversionDiagnostic = conversionError

instance IsErrorOrWarning StandaloneWarning where
  conversionDiagnostic = conversionWarning

instance IsErrorOrWarning ErrorOrWarning where
  conversionDiagnostic errOrWarn = conversionWarningRaw (ErrorOrWarningAsWarning errOrWarn)

unsupported :: (HasCallStack, Outputable a) => String -> a -> ConvertM e
unsupported typ x = conversionError (Unsupported typ (prettyPrint x))

unknown :: HasCallStack => GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> ConvertM e
unknown unitId pkgMap = conversionError (UnknownPackage unitId pkgMap)

unhandled :: (HasCallStack, Data a, Outputable a) => String -> a -> ConvertM e
unhandled typ x = conversionError (Unhandled typ (toConstr x) (prettyPrint x))


