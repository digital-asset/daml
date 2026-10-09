-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    warningFlagParser,
  ) where

import           DA.Daml.LFConversion.Errors
import           DA.Daml.UtilLF

import           Development.IDE.Types.Diagnostics
import           Development.IDE.Types.Location
import           Development.IDE.GHC.Util

import           Control.Monad.Except
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           DA.Daml.LF.Ast as LF
import           Data.Data hiding (TyCon)
import qualified Data.Map.Strict as MS
import qualified Data.Text.Extended as T
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>), notNull)
import           DA.Daml.LF.TypeChecker.Error.WarningFlags

data ConversionEnv = ConversionEnv
  { convModuleFilePath :: !NormalizedFilePath
  , convRange :: !(Maybe SourceLoc)
  , convWarningFlags :: WarningFlags ErrorOrWarning
  }

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

getErrorOrWarningStatus :: ErrorOrWarning -> ConvertM WarningFlagStatus
getErrorOrWarningStatus errOrWarn = do
   warningFlags <- asks convWarningFlags
   pure (getWarningStatus warningFlags errOrWarn)

class IsErrorOrWarning e where
  conversionDiagnostic :: e -> ConvertM ()

instance IsErrorOrWarning StandaloneError where
  conversionDiagnostic = conversionError

instance IsErrorOrWarning StandaloneWarning where
  conversionDiagnostic = conversionWarning

instance IsErrorOrWarning ErrorOrWarning where
  conversionDiagnostic errOrWarn = do
    status <- getErrorOrWarningStatus errOrWarn
    case status of
      AsError -> conversionErrorRaw (ErrorOrWarningAsError errOrWarn)
      AsWarning -> conversionWarningRaw (ErrorOrWarningAsWarning errOrWarn)
      Hidden -> pure ()

unsupported :: (HasCallStack, Outputable a) => String -> a -> ConvertM e
unsupported typ x = conversionError (Unsupported typ (prettyPrint x))

unknown :: HasCallStack => GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> ConvertM e
unknown unitId pkgMap = conversionError (UnknownPackage unitId pkgMap)

unhandled :: (HasCallStack, Data a, Outputable a) => String -> a -> ConvertM e
unhandled typ x = conversionError (Unhandled typ (toConstr x) (prettyPrint x))


