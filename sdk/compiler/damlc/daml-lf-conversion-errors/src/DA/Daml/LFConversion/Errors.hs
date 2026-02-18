-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LFConversion.Errors (
        module DA.Daml.LFConversion.Errors
    ) where

import           Development.IDE.Types.Diagnostics
import           Development.IDE.GHC.Util

import           DA.Daml.LF.Ast as LF
import           Data.Data hiding (TyCon)
import           Data.List.Extra
import qualified Data.Text            as T
import qualified Data.Map.Strict as MS
import           "ghc-lib" GHC
import           "ghc-lib" GhcPlugins as GHC hiding ((<>), notNull)
import           DA.Pretty (renderPretty)

import DA.Daml.LF.TypeChecker.Error.WarningFlags

import "ghc-lib" TyCoRep

warningFlagParser :: WarningFlagParser ErrorOrWarning
warningFlagParser =
  mkWarningFlagParser
    (\case
        LargeTuple _ -> AsWarning)
    [ WarningFlagSpec "large-tuples" True (\case
        LargeTuple _ -> True)
    ]

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
  | FeatureNotSupported LF.Feature LF.Version
  | UnknownPackage GHC.UnitId (MS.Map GHC.UnitId DalfPackage)
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
    feature <> " only available with --target=2.dev"
  FeatureNotSupported Feature{..} version ->
    T.unpack featureName <> " not supported on current lf version (" <> renderPretty version <> "), feature supported in " <> renderPretty featureVersionReq
  UnknownPackage unitId pkgMap ->
    "Unknown package: " ++ GHC.unitIdString unitId
    ++ "\n" ++  "Loaded packages are:" ++ prettyPrint (MS.keys pkgMap)
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

