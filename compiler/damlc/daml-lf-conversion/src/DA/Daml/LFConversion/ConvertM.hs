-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LFConversion.ConvertM (
    ConversionError(..),
    ConversionEnv(..),
    ConvertM(..),
    runConvertM,
    withRange,
    freshTmVar,
    resetFreshVarCounters,
    conversionWarning,
    conversionError,
    unsupported,
    unknown,
    unhandled
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

data ConversionError
  = ConversionError
     { errorFilePath :: !NormalizedFilePath
     , errorRange :: !(Maybe Range)
     , errorMessage :: !String
     }
  deriving Show

data ConversionEnv = ConversionEnv
  { convModuleFilePath :: !NormalizedFilePath
  , convRange :: !(Maybe SourceLoc)
  }

data ConversionState = ConversionState
    { freshTmVarCounter :: Int
    , warnings :: [FileDiagnostic]
    }

newtype ConvertM a = ConvertM (ReaderT ConversionEnv (StateT ConversionState (Except FileDiagnostic)) a)
  deriving (Functor, Applicative, Monad, MonadError FileDiagnostic, MonadState ConversionState, MonadReader ConversionEnv)

instance MonadFail ConvertM where
    fail = conversionError

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

conversionError :: String -> ConvertM e
conversionError msg = do
  ConversionEnv{..} <- ask
  throwError $ (convModuleFilePath,ShowDiag,) Diagnostic
      { _range = maybe noRange sourceLocToRange convRange
      , _severity = Just DsError
      , _source = Just "Core to Daml-LF"
      , _message = T.pack msg
      , _code = Nothing
      , _relatedInformation = Nothing
      , _tags = Nothing
      }

conversionWarning :: String -> ConvertM ()
conversionWarning msg = do
  ConversionEnv{..} <- ask
  let diagnostic = Diagnostic
        { _range = maybe noRange sourceLocToRange convRange
        , _severity = Just DsWarning
        , _source = Just "Core to Daml-LF"
        , _message = T.pack msg
        , _code = Nothing
        , _relatedInformation = Nothing
        , _tags = Nothing
        }
      fileDiagnostic = (convModuleFilePath, ShowDiag, diagnostic)
  modify $ \s -> s { warnings = fileDiagnostic : warnings s }

unsupported :: (HasCallStack, Outputable a) => String -> a -> ConvertM e
unsupported typ x = conversionError errMsg
    where
         errMsg =
             "Failure to process Daml program, this feature is not currently supported.\n" ++
             typ ++ "\n" ++
             prettyPrint x

unknown :: HasCallStack => GHC.UnitId -> MS.Map GHC.UnitId DalfPackage -> ConvertM e
unknown unitId pkgMap = conversionError errMsg
    where errMsg =
              "Unknown package: " ++ GHC.unitIdString unitId
              ++ "\n" ++  "Loaded packages are:" ++ prettyPrint (MS.keys pkgMap)

unhandled :: (HasCallStack, Data a, Outputable a) => String -> a -> ConvertM e
unhandled typ x = unsupported (typ ++ " with " ++ lower (show (toConstr x))) x


