-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE TypeFamilies #-}

module DA.Daml.LF.Proto3.DecodeV1
    ( decodePackage
    , decodeScenarioModule
    , Error(..)
    ) where

import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.Util as Util
import Data.Coerce
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Int
import Text.Read
import           Data.List
import    DA.Daml.StablePackagesList
import           DA.Daml.LF.Mangling
import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Data.NameMap as NM
import qualified Data.Text as T
import qualified Data.Set as S
import qualified Data.Text.Lazy as TL
import qualified Data.Vector.Extended as V
import qualified Proto3.Suite as Proto


data DecodeEnv = DecodeEnv
    -- We cache unmangled identifiers here so that we only do the unmangling once
    -- and so that we can share the unmangled identifiers. Since not all strings in the string
    -- interning tables are mangled, we store the potential error from unmangling rather than
    -- erroring out when producing the string interning table.
    { internedStrings :: !(V.Vector (T.Text, Either String UnmangledIdentifier))
    , internedDottedNames :: !(V.Vector ([T.Text], Either String [UnmangledIdentifier]))
    , internedTypes :: !(V.Vector Type)
    , selfPackageRef :: PackageRef
    }

newtype Decode a = Decode{unDecode :: ReaderT DecodeEnv (Except Error) a}
    deriving (Functor, Applicative, Monad, MonadError Error, MonadReader DecodeEnv)

runDecode :: DecodeEnv -> Decode a -> Either Error a
runDecode env act = runExcept $ runReaderT (unDecode act) env

lookupInterned :: V.Vector a -> (Int32 -> Error) -> Int32 -> Decode a
lookupInterned interned mkError id = do
    case interned V.!? fromIntegral id of
          Nothing -> throwError $ mkError id
          Just x -> pure x

lookupString :: Int32 -> Decode (T.Text, Either String UnmangledIdentifier)
lookupString strId = do
    DecodeEnv{internedStrings} <- ask
    lookupInterned internedStrings BadStringId strId

lookupDottedName :: Int32 -> Decode ([T.Text], Either String [UnmangledIdentifier])
lookupDottedName id = do
    DecodeEnv{internedDottedNames} <- ask
    lookupInterned internedDottedNames BadDottedNameId id

------------------------------------------------------------------------
-- Decodings of things related to string interning
------------------------------------------------------------------------

-- | Decode of a string that cannot be interned, e.g, the entries of the
-- interning table itself.
decodeString :: TL.Text -> T.Text
decodeString = TL.toStrict

-- | Decode of a string that cannot be interned, e.g, the entries of the
-- interning table itself.
decodeMangledString :: TL.Text -> (T.Text, Either String UnmangledIdentifier)
decodeMangledString t = (decoded, unmangledOrErr)
    where !decoded = decodeString t
          unmangledOrErr = unmangleIdentifier decoded

-- | Decode a string that will be interned in Daml-LF 1.7 and onwards.
-- At the protobuf level, we represent internable non-empty lists of strings
-- by a repeatable string and a number. If there's at least one string,
-- then the number must not be set, i.e. zero. If there are no strings,
-- then the number is treated as an index into the interning table.
decodeInternableStrings :: V.Vector TL.Text -> Int32 -> Decode ([T.Text], Either String [UnmangledIdentifier])
decodeInternableStrings strs id
    | V.null strs = lookupDottedName id
    | id == 0 =
      let decodedStrs = map decodeString (V.toList strs)
          unmangled = mapM unmangleIdentifier decodedStrs
      in pure (decodedStrs, unmangled)
    | otherwise = throwError $ ParseError "items and interned id both set for string list"

-- | Decode the name of a syntactic object, e.g., a variable or a data
-- constructor. These strings are mangled to escape special characters. All
-- names will be interned in Daml-LF 1.7 and onwards.
decodeName
    :: Util.EitherLike TL.Text Int32 e
    => (T.Text -> a) -> Maybe e -> Decode a
decodeName wrapName mbStrOrId = mayDecode "name" mbStrOrId $ \strOrId -> do
    unmangledOrErr <- case Util.toEither strOrId of
        Left str -> pure $ snd $ decodeMangledString str
        Right strId -> snd <$> lookupString strId
    decodeNameString wrapName unmangledOrErr

decodeNameId :: (T.Text -> a) -> Int32 -> Decode a
decodeNameId wrapName strId = do
    (_, s) <- lookupString strId
    decodeNameString wrapName s

decodeNameString :: (T.Text -> a) -> Either String UnmangledIdentifier -> Decode a
decodeNameString wrapName unmangledOrErr =
    case unmangledOrErr of
        Left err -> throwError $ ParseError err
        Right (UnmangledIdentifier unmangled) -> pure $ wrapName unmangled

-- | Decode the multi-component name of a syntactic object, e.g., a type
-- constructor. All compononents are mangled. Dotted names will be interned
-- in Daml-LF 1.7 and onwards.
decodeDottedName :: Util.EitherLike LF1.DottedName Int32 e
                 => ([T.Text] -> a) -> Maybe e -> Decode a
decodeDottedName wrapDottedName mbDottedNameOrId = mayDecode "dottedName" mbDottedNameOrId $ \dottedNameOrId -> do
    (_, unmangledOrErr) <- case Util.toEither dottedNameOrId of
        Left (LF1.DottedName mangledV) -> decodeInternableStrings mangledV 0
        Right dnId -> lookupDottedName dnId
    case unmangledOrErr of
      Left err -> throwError $ ParseError err
      Right unmangled -> pure $ wrapDottedName (coerce unmangled)

decodeDottedNameId :: ([T.Text] -> a) -> Int32 -> Decode a
decodeDottedNameId wrapDottedName dnId = do
  (_, unmangledOrErr) <- lookupDottedName dnId
  case unmangledOrErr of
    Left err -> throwError $ ParseError err
    Right unmangled -> pure $ wrapDottedName (coerce unmangled)

-- | Decode the name of a top-level value. The name is mangled and will be
-- interned in Daml-LF 1.7 and onwards.
decodeValueName :: String -> V.Vector TL.Text -> Int32 -> Decode ExprValName
decodeValueName ident mangledV dnId = do
    (mangled, unmangledOrErr) <- decodeInternableStrings mangledV dnId
    case unmangledOrErr of
      Left err -> throwError $ ParseError err
      Right [UnmangledIdentifier unmangled] -> pure $ ExprValName unmangled
      Right [] -> throwError $ MissingField ident
      Right _ ->
          throwError $ ParseError $ "Unexpected multi-segment def name: " ++ show mangledV ++ "//" ++ show mangled

-- | Decode a reference to a top-level value. The name is mangled and will be
-- interned in Daml-LF 1.7 and onwards.
decodeValName :: LF1.ValName -> Decode (Qualified ExprValName)
decodeValName LF1.ValName{..} = do
  (pref, mname) <- mayDecode "valNameModule" valNameModule decodeModuleRef
  name <- decodeValueName "valNameName" valNameNameDname valNameNameInternedDname
  pure $ Qualified pref mname name

-- | Decode a reference to a package. Package names are not mangled. Package
-- name are interned since Daml-LF 1.6.
decodePackageRef :: LF1.PackageRef -> Decode PackageRef
decodePackageRef (LF1.PackageRef pref) =
    mayDecode "packageRefSum" pref $ \case
        LF1.PackageRefSumSelf _ -> asks selfPackageRef
        LF1.PackageRefSumPackageIdStr pkgId -> pure $ PRImport $ PackageId $ decodeString pkgId
        LF1.PackageRefSumPackageIdInternedStr strId -> PRImport . PackageId . fst <$> lookupString strId

------------------------------------------------------------------------
-- Decodings of everything else
------------------------------------------------------------------------

decodeVersion :: Maybe LF.PackageId -> T.Text -> Either Error Version
decodeVersion mbPkgId minorText = do
  let unsupported :: Either Error a
      unsupported = throwError (UnsupportedMinorVersion minorText)
  -- we translate "no version" to minor version 0, since we introduced
  -- minor versions once Daml-LF v1 was already out, and we want to be
  -- able to parse packages that were compiled before minor versions
  -- were a thing. DO NOT replicate this code bejond major version 1!
  minor <- if
    | T.null minorText -> pure $ LF.PointStable 0
    | Just minor <- LF.parseMinorVersion (T.unpack minorText) -> pure minor
    | otherwise -> unsupported
  let version = V1 minor
  if  isStablePackage || version `elem` LF.supportedInputVersions then pure version else unsupported
  where
    isStablePackage = maybe False (`elem` stablePackages) mbPkgId

decodeInternedDottedName :: LF1.InternedDottedName -> Decode ([T.Text], Either String [UnmangledIdentifier])
decodeInternedDottedName (LF1.InternedDottedName ids) = do
    (mangled, unmangledOrErr) <- unzip <$> mapM lookupString (V.toList ids)
    pure (mangled, sequence unmangledOrErr)

-- The package id is optional since we also call this function from decodeScenarioModule
decodePackage :: Maybe LF.PackageId -> TL.Text -> LF.PackageRef -> LF1.Package -> Either Error Package
decodePackage mbPkgId minorText selfPackageRef (LF1.Package mods internedStringsV internedDottedNamesV metadata internedTypesV) = do
  version <- decodeVersion mbPkgId (decodeString minorText)
  let internedStrings = V.map decodeMangledString internedStringsV
  let internedDottedNames = V.empty
  let internedTypes = V.empty
  let env0 = DecodeEnv{..}
  internedDottedNames <- runDecode env0 $ mapM decodeInternedDottedName internedDottedNamesV
  let env1 = env0{internedDottedNames}
  internedTypes <- V.constructNE (V.length internedTypesV) $ \prefix i ->
      runDecode env1{internedTypes = prefix} $ decodeType (internedTypesV V.! i)
  let env2 = env1{internedTypes}
  runDecode env2 $ do
    Package version <$> decodeNM DuplicateModule decodeModule mods <*> traverse decodePackageMetadata metadata

decodePackageMetadata :: LF1.PackageMetadata -> Decode PackageMetadata
decodePackageMetadata LF1.PackageMetadata{..} = do
    pkgName <- PackageName . fst <$> lookupString packageMetadataNameInternedStr
    pkgVersion <- PackageVersion . fst <$> lookupString packageMetadataVersionInternedStr
    pure (PackageMetadata pkgName pkgVersion)

decodeScenarioModule :: TL.Text -> LF1.Package -> Either Error Module
decodeScenarioModule minorText protoPkg = do
    Package { packageModules = modules } <- decodePackage Nothing minorText PRSelf protoPkg
    pure $ head $ NM.toList modules

decodeModule :: LF1.Module -> Decode Module
decodeModule (LF1.Module name flags synonyms dataTypes values templates exceptions interfaces) =
  Module
    <$> decodeDottedName ModuleName name
    <*> pure Nothing
    <*> mayDecode "flags" flags decodeFeatureFlags
    <*> decodeNM DuplicateTypeSyn decodeDefTypeSyn synonyms
    <*> decodeNM DuplicateDataType decodeDefDataType dataTypes
    <*> decodeNM DuplicateValue decodeDefValue values
    <*> decodeNM EDuplicateTemplate decodeDefTemplate templates
    <*> decodeNM DuplicateException decodeDefException exceptions
    <*> decodeNM DuplicateInterface decodeDefInterface interfaces

decodeDefInterface :: LF1.DefInterface -> Decode DefInterface
decodeDefInterface LF1.DefInterface {..} = do
  intLocation <- traverse decodeLocation defInterfaceLocation
  intName <- decodeDottedNameId TypeConName defInterfaceTyconInternedDname
  intRequires <- decodeSet DuplicateRequires decodeTypeConName defInterfaceRequires
  intParam <- decodeNameId ExprVarName defInterfaceParamInternedStr
  intChoices <- decodeNM DuplicateChoice decodeChoice defInterfaceChoices
  intMethods <- decodeNM DuplicateMethod decodeInterfaceMethod defInterfaceMethods
  intCoImplements <- decodeNM DuplicateCoImplements decodeInterfaceCoImplements defInterfaceCoImplements
  intView <- mayDecode "defInterfaceView" defInterfaceView decodeType
  pure DefInterface {..}

decodeInterfaceMethod :: LF1.InterfaceMethod -> Decode InterfaceMethod
decodeInterfaceMethod LF1.InterfaceMethod {..} = InterfaceMethod
  <$> traverse decodeLocation interfaceMethodLocation
  <*> decodeMethodName interfaceMethodMethodInternedName
  <*> mayDecode "interfaceMethodType" interfaceMethodType decodeType

decodeMethodName :: Int32 -> Decode MethodName
decodeMethodName = decodeNameId MethodName

decodeInterfaceCoImplements :: LF1.DefInterface_CoImplements -> Decode InterfaceCoImplements
decodeInterfaceCoImplements LF1.DefInterface_CoImplements {..} = InterfaceCoImplements
  <$> mayDecode "defInterface_CoImplementsTemplate" defInterface_CoImplementsTemplate decodeTypeConName
  <*> mayDecode "defInterface_CoImplementsBody" defInterface_CoImplementsBody decodeInterfaceInstanceBody

decodeFeatureFlags :: LF1.FeatureFlags -> Decode FeatureFlags
decodeFeatureFlags LF1.FeatureFlags{..} =
  if not featureFlagsDontDivulgeContractIdsInCreateArguments || not featureFlagsDontDiscloseNonConsumingChoicesToObservers || not featureFlagsForbidPartyLiterals
    -- We do not support these anymore -- see #157
    then throwError (ParseError "Package uses unsupported flags dontDivulgeContractIdsInCreateArguments, dontDiscloseNonConsumingChoicesToObservers or featureFlagsForbidPartyLiterals")
    else pure FeatureFlags

decodeDefTypeSyn :: LF1.DefTypeSyn -> Decode DefTypeSyn
decodeDefTypeSyn LF1.DefTypeSyn{..} =
  DefTypeSyn
    <$> traverse decodeLocation defTypeSynLocation
    <*> decodeDottedName TypeSynName defTypeSynName
    <*> traverse decodeTypeVarWithKind (V.toList defTypeSynParams)
    <*> mayDecode "typeSynType" defTypeSynType decodeType

decodeDefException :: LF1.DefException -> Decode DefException
decodeDefException LF1.DefException{..} =
  DefException
    <$> traverse decodeLocation defExceptionLocation
    <*> decodeDottedNameId TypeConName defExceptionNameInternedDname
    <*> mayDecode "exceptionMessage" defExceptionMessage decodeExpr

decodeDefDataType :: LF1.DefDataType -> Decode DefDataType
decodeDefDataType LF1.DefDataType{..} =
  DefDataType
    <$> traverse decodeLocation defDataTypeLocation
    <*> decodeDottedName TypeConName defDataTypeName
    <*> pure (IsSerializable defDataTypeSerializable)
    <*> traverse decodeTypeVarWithKind (V.toList defDataTypeParams)
    <*> mayDecode "dataTypeDataCons" defDataTypeDataCons decodeDataCons

decodeDataCons :: LF1.DefDataTypeDataCons -> Decode DataCons
decodeDataCons = \case
  LF1.DefDataTypeDataConsRecord (LF1.DefDataType_Fields fs) ->
    DataRecord <$> mapM (decodeFieldWithType FieldName) (V.toList fs)
  LF1.DefDataTypeDataConsVariant (LF1.DefDataType_Fields fs) ->
    DataVariant <$> mapM (decodeFieldWithType VariantConName) (V.toList fs)
  LF1.DefDataTypeDataConsEnum (LF1.DefDataType_EnumConstructors cs cIds) -> do
    unmangledOrErr <- if
      | V.null cIds -> pure $ map (snd . decodeMangledString) (V.toList cs)
      | V.null cs -> mapM (fmap snd . lookupString) (V.toList cIds)
      | otherwise -> throwError $ ParseError "strings and interned string ids both set for enum constructor"
    DataEnum <$> mapM (decodeNameString VariantConName) unmangledOrErr
  LF1.DefDataTypeDataConsInterface LF1.Unit -> pure DataInterface

decodeDefValueNameWithType :: LF1.DefValue_NameWithType -> Decode (ExprValName, Type)
decodeDefValueNameWithType LF1.DefValue_NameWithType{..} = (,)
  <$> decodeValueName "defValueName" defValue_NameWithTypeNameDname defValue_NameWithTypeNameInternedDname
  <*> mayDecode "defValueType" defValue_NameWithTypeType decodeType

decodeDefValue :: LF1.DefValue -> Decode DefValue
decodeDefValue (LF1.DefValue mbBinder mbBody noParties isTest mbLoc) = do
  when (not noParties) $
    throwError (ParseError "DefValue uses unsupported no_party_literals flag")
  DefValue
    <$> traverse decodeLocation mbLoc
    <*> mayDecode "defValueName" mbBinder decodeDefValueNameWithType
    <*> pure (IsTest isTest)
    <*> mayDecode "defValueExpr" mbBody decodeExpr

decodeDefTemplate :: LF1.DefTemplate -> Decode Template
decodeDefTemplate LF1.DefTemplate{..} = do
  tplParam <- decodeName ExprVarName defTemplateParam
  Template
    <$> traverse decodeLocation defTemplateLocation
    <*> decodeDottedName TypeConName defTemplateTycon
    <*> pure tplParam
    <*> mayDecode "defTemplatePrecond" defTemplatePrecond decodeExpr
    <*> mayDecode "defTemplateSignatories" defTemplateSignatories decodeExpr
    <*> mayDecode "defTemplateObservers" defTemplateObservers decodeExpr
    <*> mayDecode "defTemplateAgreement" defTemplateAgreement decodeExpr
    <*> decodeNM DuplicateChoice decodeChoice defTemplateChoices
    <*> mapM (decodeDefTemplateKey tplParam) defTemplateKey
    <*> decodeNM DuplicateImplements decodeDefTemplateImplements defTemplateImplements

decodeDefTemplateImplements :: LF1.DefTemplate_Implements -> Decode TemplateImplements
decodeDefTemplateImplements LF1.DefTemplate_Implements{..} = TemplateImplements
  <$> mayDecode "defTemplate_ImplementsInterface" defTemplate_ImplementsInterface decodeTypeConName
  <*> mayDecode "defTemplate_ImplementsBody" defTemplate_ImplementsBody decodeInterfaceInstanceBody

decodeInterfaceInstanceBody :: LF1.InterfaceInstanceBody -> Decode InterfaceInstanceBody
decodeInterfaceInstanceBody LF1.InterfaceInstanceBody{..} = InterfaceInstanceBody
  <$> decodeNM DuplicateMethod decodeInterfaceInstanceMethod interfaceInstanceBodyMethods
  <*> mayDecode "defTemplate_ImplementsView" interfaceInstanceBodyView decodeExpr

decodeInterfaceInstanceMethod :: LF1.InterfaceInstanceBody_InterfaceInstanceMethod -> Decode InterfaceInstanceMethod
decodeInterfaceInstanceMethod LF1.InterfaceInstanceBody_InterfaceInstanceMethod{..} = InterfaceInstanceMethod
  <$> decodeMethodName interfaceInstanceBody_InterfaceInstanceMethodMethodInternedName
  <*> mayDecode "interfaceInstanceBody_InterfaceInstanceMethodValue" interfaceInstanceBody_InterfaceInstanceMethodValue decodeExpr

decodeDefTemplateKey :: ExprVarName -> LF1.DefTemplate_DefKey -> Decode TemplateKey
decodeDefTemplateKey templateParam LF1.DefTemplate_DefKey{..} = do
  typ <- mayDecode "defTemplate_DefKeyType" defTemplate_DefKeyType decodeType
  key <- mayDecode "defTemplate_DefKeyKeyExpr" defTemplate_DefKeyKeyExpr (decodeKeyExpr templateParam)
  maintainers <- mayDecode "defTemplate_DefKeyMaintainers" defTemplate_DefKeyMaintainers decodeExpr
  return (TemplateKey typ key maintainers)

decodeKeyExpr :: ExprVarName -> LF1.DefTemplate_DefKeyKeyExpr -> Decode Expr
decodeKeyExpr templateParam = \case
    LF1.DefTemplate_DefKeyKeyExprKey simpleKeyExpr ->
        decodeSimpleKeyExpr templateParam simpleKeyExpr
    LF1.DefTemplate_DefKeyKeyExprComplexKey keyExpr ->
        decodeExpr keyExpr

decodeSimpleKeyExpr :: ExprVarName -> LF1.KeyExpr -> Decode Expr
decodeSimpleKeyExpr templateParam LF1.KeyExpr{..} = mayDecode "keyExprSum" keyExprSum $ \case
  LF1.KeyExprSumProjections LF1.KeyExpr_Projections{..} ->
    foldM
      (\rec_ LF1.KeyExpr_Projection{..} ->
        ERecProj
          <$> mayDecode "KeyExpr_ProjectionTyCon" keyExpr_ProjectionTycon decodeTypeConApp
          <*> decodeName FieldName keyExpr_ProjectionField
          <*> pure rec_)
      (EVar templateParam) keyExpr_ProjectionsProjections
  LF1.KeyExprSumRecord LF1.KeyExpr_Record{..} ->
    ERecCon
      <$> mayDecode "keyExpr_RecordTycon" keyExpr_RecordTycon decodeTypeConApp
      <*> mapM (decodeFieldWithSimpleKeyExpr templateParam) (V.toList keyExpr_RecordFields)

decodeFieldWithSimpleKeyExpr :: ExprVarName -> LF1.KeyExpr_RecordField -> Decode (FieldName, Expr)
decodeFieldWithSimpleKeyExpr templateParam LF1.KeyExpr_RecordField{..} =
  (,)
  <$> decodeName FieldName keyExpr_RecordFieldField
  <*> mayDecode "keyExpr_RecordFieldExpr" keyExpr_RecordFieldExpr (decodeSimpleKeyExpr templateParam)

decodeChoice :: LF1.TemplateChoice -> Decode TemplateChoice
decodeChoice LF1.TemplateChoice{..} =
  TemplateChoice
    <$> traverse decodeLocation templateChoiceLocation
    <*> decodeName ChoiceName templateChoiceName
    <*> pure templateChoiceConsuming
    <*> mayDecode "templateChoiceControllers" templateChoiceControllers decodeExpr
    <*> traverse decodeExpr templateChoiceObservers
    <*> decodeName ExprVarName templateChoiceSelfBinder
    <*> mayDecode "templateChoiceArgBinder" templateChoiceArgBinder decodeVarWithType
    <*> mayDecode "templateChoiceRetType" templateChoiceRetType decodeType
    <*> mayDecode "templateChoiceUpdate" templateChoiceUpdate decodeExpr

decodeBuiltinFunction :: LF1.BuiltinFunction -> Decode BuiltinExpr
decodeBuiltinFunction = \case
  LF1.BuiltinFunctionEQUAL -> pure BEEqualGeneric
  LF1.BuiltinFunctionLESS -> pure BELessGeneric
  LF1.BuiltinFunctionLESS_EQ -> pure BELessEqGeneric
  LF1.BuiltinFunctionGREATER -> pure BEGreaterGeneric
  LF1.BuiltinFunctionGREATER_EQ -> pure BEGreaterEqGeneric

  LF1.BuiltinFunctionEQUAL_INT64 -> pure (BEEqual BTInt64)
  LF1.BuiltinFunctionEQUAL_NUMERIC -> pure BEEqualNumeric
  LF1.BuiltinFunctionEQUAL_TEXT -> pure (BEEqual BTText)
  LF1.BuiltinFunctionEQUAL_TIMESTAMP -> pure (BEEqual BTTimestamp)
  LF1.BuiltinFunctionEQUAL_DATE -> pure (BEEqual BTDate)
  LF1.BuiltinFunctionEQUAL_PARTY -> pure (BEEqual BTParty)
  LF1.BuiltinFunctionEQUAL_BOOL -> pure (BEEqual BTBool)
  LF1.BuiltinFunctionEQUAL_TYPE_REP -> pure (BEEqual BTTypeRep)

  LF1.BuiltinFunctionLEQ_INT64 -> pure (BELessEq BTInt64)
  LF1.BuiltinFunctionLEQ_NUMERIC -> pure BELessEqNumeric
  LF1.BuiltinFunctionLEQ_TEXT -> pure (BELessEq BTText)
  LF1.BuiltinFunctionLEQ_TIMESTAMP -> pure (BELessEq BTTimestamp)
  LF1.BuiltinFunctionLEQ_DATE -> pure (BELessEq BTDate)
  LF1.BuiltinFunctionLEQ_PARTY -> pure (BELessEq BTParty)

  LF1.BuiltinFunctionLESS_INT64 -> pure (BELess BTInt64)
  LF1.BuiltinFunctionLESS_NUMERIC -> pure BELessNumeric
  LF1.BuiltinFunctionLESS_TEXT -> pure (BELess BTText)
  LF1.BuiltinFunctionLESS_TIMESTAMP -> pure (BELess BTTimestamp)
  LF1.BuiltinFunctionLESS_DATE -> pure (BELess BTDate)
  LF1.BuiltinFunctionLESS_PARTY -> pure (BELess BTParty)

  LF1.BuiltinFunctionGEQ_INT64 -> pure (BEGreaterEq BTInt64)
  LF1.BuiltinFunctionGEQ_NUMERIC -> pure BEGreaterEqNumeric
  LF1.BuiltinFunctionGEQ_TEXT -> pure (BEGreaterEq BTText)
  LF1.BuiltinFunctionGEQ_TIMESTAMP -> pure (BEGreaterEq BTTimestamp)
  LF1.BuiltinFunctionGEQ_DATE -> pure (BEGreaterEq BTDate)
  LF1.BuiltinFunctionGEQ_PARTY -> pure (BEGreaterEq BTParty)

  LF1.BuiltinFunctionGREATER_INT64 -> pure (BEGreater BTInt64)
  LF1.BuiltinFunctionGREATER_NUMERIC -> pure BEGreaterNumeric
  LF1.BuiltinFunctionGREATER_TEXT -> pure (BEGreater BTText)
  LF1.BuiltinFunctionGREATER_TIMESTAMP -> pure (BEGreater BTTimestamp)
  LF1.BuiltinFunctionGREATER_DATE -> pure (BEGreater BTDate)
  LF1.BuiltinFunctionGREATER_PARTY -> pure (BEGreater BTParty)

  LF1.BuiltinFunctionINT64_TO_TEXT -> pure (BEToText BTInt64)
  LF1.BuiltinFunctionNUMERIC_TO_TEXT -> pure BENumericToText
  LF1.BuiltinFunctionTEXT_TO_TEXT -> pure (BEToText BTText)
  LF1.BuiltinFunctionTIMESTAMP_TO_TEXT -> pure (BEToText BTTimestamp)
  LF1.BuiltinFunctionPARTY_TO_TEXT -> pure (BEToText BTParty)
  LF1.BuiltinFunctionDATE_TO_TEXT -> pure (BEToText BTDate)
  LF1.BuiltinFunctionCONTRACT_ID_TO_TEXT -> pure BEContractIdToText
  LF1.BuiltinFunctionBIGNUMERIC_TO_TEXT -> pure (BEToText BTBigNumeric)
  LF1.BuiltinFunctionCODE_POINTS_TO_TEXT -> pure BECodePointsToText
  LF1.BuiltinFunctionTEXT_TO_PARTY -> pure BETextToParty
  LF1.BuiltinFunctionTEXT_TO_INT64 -> pure BETextToInt64
  LF1.BuiltinFunctionTEXT_TO_NUMERIC -> pure BETextToNumeric
  LF1.BuiltinFunctionTEXT_TO_CODE_POINTS -> pure BETextToCodePoints
  LF1.BuiltinFunctionPARTY_TO_QUOTED_TEXT -> pure BEPartyToQuotedText

  LF1.BuiltinFunctionADD_NUMERIC   -> pure BEAddNumeric
  LF1.BuiltinFunctionSUB_NUMERIC   -> pure BESubNumeric
  LF1.BuiltinFunctionMUL_NUMERIC   -> pure BEMulNumeric
  LF1.BuiltinFunctionDIV_NUMERIC   -> pure BEDivNumeric
  LF1.BuiltinFunctionROUND_NUMERIC -> pure BERoundNumeric
  LF1.BuiltinFunctionCAST_NUMERIC  -> pure BECastNumeric
  LF1.BuiltinFunctionSHIFT_NUMERIC -> pure BEShiftNumeric

  LF1.BuiltinFunctionADD_INT64 -> pure BEAddInt64
  LF1.BuiltinFunctionSUB_INT64 -> pure BESubInt64
  LF1.BuiltinFunctionMUL_INT64 -> pure BEMulInt64
  LF1.BuiltinFunctionDIV_INT64 -> pure BEDivInt64
  LF1.BuiltinFunctionMOD_INT64 -> pure BEModInt64
  LF1.BuiltinFunctionEXP_INT64 -> pure BEExpInt64

  LF1.BuiltinFunctionFOLDL          -> pure BEFoldl
  LF1.BuiltinFunctionFOLDR          -> pure BEFoldr
  LF1.BuiltinFunctionEQUAL_LIST     -> pure BEEqualList
  LF1.BuiltinFunctionAPPEND_TEXT    -> pure BEAppendText

  LF1.BuiltinFunctionERROR          -> pure BEError
  LF1.BuiltinFunctionANY_EXCEPTION_MESSAGE -> pure BEAnyExceptionMessage

  LF1.BuiltinFunctionTEXTMAP_EMPTY      -> pure BETextMapEmpty
  LF1.BuiltinFunctionTEXTMAP_INSERT     -> pure BETextMapInsert
  LF1.BuiltinFunctionTEXTMAP_LOOKUP     -> pure BETextMapLookup
  LF1.BuiltinFunctionTEXTMAP_DELETE     -> pure BETextMapDelete
  LF1.BuiltinFunctionTEXTMAP_TO_LIST    -> pure BETextMapToList
  LF1.BuiltinFunctionTEXTMAP_SIZE       -> pure BETextMapSize

  LF1.BuiltinFunctionGENMAP_EMPTY      -> pure BEGenMapEmpty
  LF1.BuiltinFunctionGENMAP_INSERT     -> pure BEGenMapInsert
  LF1.BuiltinFunctionGENMAP_LOOKUP     -> pure BEGenMapLookup
  LF1.BuiltinFunctionGENMAP_DELETE     -> pure BEGenMapDelete
  LF1.BuiltinFunctionGENMAP_KEYS       -> pure BEGenMapKeys
  LF1.BuiltinFunctionGENMAP_VALUES     -> pure BEGenMapValues
  LF1.BuiltinFunctionGENMAP_SIZE       -> pure BEGenMapSize

  LF1.BuiltinFunctionEXPLODE_TEXT -> pure BEExplodeText
  LF1.BuiltinFunctionIMPLODE_TEXT -> pure BEImplodeText
  LF1.BuiltinFunctionSHA256_TEXT  -> pure BESha256Text

  LF1.BuiltinFunctionDATE_TO_UNIX_DAYS -> pure BEDateToUnixDays
  LF1.BuiltinFunctionUNIX_DAYS_TO_DATE -> pure BEUnixDaysToDate
  LF1.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS -> pure BETimestampToUnixMicroseconds
  LF1.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP -> pure BEUnixMicrosecondsToTimestamp

  LF1.BuiltinFunctionINT64_TO_NUMERIC -> pure BEInt64ToNumeric
  LF1.BuiltinFunctionNUMERIC_TO_INT64 -> pure BENumericToInt64

  LF1.BuiltinFunctionTRACE -> pure BETrace
  LF1.BuiltinFunctionEQUAL_CONTRACT_ID -> pure BEEqualContractId
  LF1.BuiltinFunctionCOERCE_CONTRACT_ID -> pure BECoerceContractId

  LF1.BuiltinFunctionTYPEREP_TYCON_NAME -> pure BETypeRepTyConName

  LF1.BuiltinFunctionTEXT_TO_UPPER -> pure BETextToUpper
  LF1.BuiltinFunctionTEXT_TO_LOWER -> pure BETextToLower
  LF1.BuiltinFunctionTEXT_SLICE -> pure BETextSlice
  LF1.BuiltinFunctionTEXT_SLICE_INDEX -> pure BETextSliceIndex
  LF1.BuiltinFunctionTEXT_CONTAINS_ONLY -> pure BETextContainsOnly
  LF1.BuiltinFunctionTEXT_REPLICATE -> pure BETextReplicate
  LF1.BuiltinFunctionTEXT_SPLIT_ON -> pure BETextSplitOn
  LF1.BuiltinFunctionTEXT_INTERCALATE -> pure BETextIntercalate

  LF1.BuiltinFunctionSCALE_BIGNUMERIC -> pure BEScaleBigNumeric
  LF1.BuiltinFunctionPRECISION_BIGNUMERIC -> pure BEPrecisionBigNumeric
  LF1.BuiltinFunctionADD_BIGNUMERIC -> pure BEAddBigNumeric
  LF1.BuiltinFunctionSUB_BIGNUMERIC -> pure BESubBigNumeric
  LF1.BuiltinFunctionMUL_BIGNUMERIC -> pure BEMulBigNumeric
  LF1.BuiltinFunctionDIV_BIGNUMERIC -> pure BEDivBigNumeric
  LF1.BuiltinFunctionSHIFT_RIGHT_BIGNUMERIC -> pure BEShiftRightBigNumeric
  LF1.BuiltinFunctionBIGNUMERIC_TO_NUMERIC -> pure BEBigNumericToNumeric
  LF1.BuiltinFunctionNUMERIC_TO_BIGNUMERIC -> pure BENumericToBigNumeric

  LF1.BuiltinFunctionADD_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionSUB_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionMUL_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionDIV_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionROUND_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionLEQ_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionLESS_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionGEQ_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionGREATER_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionDECIMAL_TO_TEXT -> unsupportedDecimal
  LF1.BuiltinFunctionINT64_TO_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionDECIMAL_TO_INT64 -> unsupportedDecimal
  LF1.BuiltinFunctionEQUAL_DECIMAL -> unsupportedDecimal
  LF1.BuiltinFunctionTEXT_TO_DECIMAL -> unsupportedDecimal

decodeLocation :: LF1.Location -> Decode SourceLoc
decodeLocation (LF1.Location mbModRef mbRange) = do
  mbModRef' <- traverse decodeModuleRef mbModRef
  LF1.Location_Range sline scol eline ecol <- mayDecode "Location_Range" mbRange pure
  pure $ SourceLoc
    mbModRef'
    (fromIntegral sline) (fromIntegral scol)
    (fromIntegral eline) (fromIntegral ecol)

decodeExpr :: LF1.Expr -> Decode Expr
decodeExpr (LF1.Expr mbLoc exprSum) = case mbLoc of
  Nothing -> decodeExprSum exprSum
  Just loc -> ELocation <$> decodeLocation loc <*> decodeExprSum exprSum

decodeExprSum :: Maybe LF1.ExprSum -> Decode Expr
decodeExprSum exprSum = mayDecode "exprSum" exprSum $ \case
  LF1.ExprSumVarStr var -> EVar <$> decodeNameString ExprVarName (snd $ decodeMangledString var)
  LF1.ExprSumVarInternedStr strId -> EVar <$> decodeNameId ExprVarName strId
  LF1.ExprSumVal val -> EVal <$> decodeValName val
  LF1.ExprSumBuiltin (Proto.Enumerated (Right bi)) -> EBuiltin <$> decodeBuiltinFunction bi
  LF1.ExprSumBuiltin (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumBuiltin" num)
  LF1.ExprSumPrimCon (Proto.Enumerated (Right con)) -> pure $ EBuiltin $ case con of
    LF1.PrimConCON_UNIT -> BEUnit
    LF1.PrimConCON_TRUE -> BEBool True
    LF1.PrimConCON_FALSE -> BEBool False

  LF1.ExprSumPrimCon (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumPrimCon" num)
  LF1.ExprSumPrimLit lit ->
    EBuiltin <$> decodePrimLit lit
  LF1.ExprSumRecCon (LF1.Expr_RecCon mbTycon fields) ->
    ERecCon
      <$> mayDecode "Expr_RecConTycon" mbTycon decodeTypeConApp
      <*> mapM decodeFieldWithExpr (V.toList fields)
  LF1.ExprSumRecProj (LF1.Expr_RecProj mbTycon field mbRecord) ->
    ERecProj
      <$> mayDecode "Expr_RecProjTycon" mbTycon decodeTypeConApp
      <*> decodeName FieldName field
      <*> mayDecode "Expr_RecProjRecord" mbRecord decodeExpr
  LF1.ExprSumRecUpd (LF1.Expr_RecUpd mbTycon field mbRecord mbUpdate) ->
    ERecUpd
      <$> mayDecode "Expr_RecUpdTycon" mbTycon decodeTypeConApp
      <*> decodeName FieldName field
      <*> mayDecode "Expr_RecUpdRecord" mbRecord decodeExpr
      <*> mayDecode "Expr_RecUpdUpdate" mbUpdate decodeExpr
  LF1.ExprSumVariantCon (LF1.Expr_VariantCon mbTycon variant mbArg) ->
    EVariantCon
      <$> mayDecode "Expr_VariantConTycon" mbTycon decodeTypeConApp
      <*> decodeName VariantConName variant
      <*> mayDecode "Expr_VariantConVariantArg" mbArg decodeExpr
  LF1.ExprSumEnumCon (LF1.Expr_EnumCon mbTypeCon dataCon) ->
    EEnumCon
      <$> mayDecode "Expr_EnumConTycon" mbTypeCon decodeTypeConName
      <*> decodeName VariantConName dataCon
  LF1.ExprSumStructCon (LF1.Expr_StructCon fields) ->
    EStructCon
      <$> mapM decodeFieldWithExpr (V.toList fields)
  LF1.ExprSumStructProj (LF1.Expr_StructProj field mbStruct) ->
    EStructProj
      <$> decodeName FieldName field
      <*> mayDecode "Expr_StructProjStruct" mbStruct decodeExpr
  LF1.ExprSumStructUpd (LF1.Expr_StructUpd field mbStruct mbUpdate) ->
    EStructUpd
      <$> decodeName FieldName field
      <*> mayDecode "Expr_StructUpdStruct" mbStruct decodeExpr
      <*> mayDecode "Expr_StructUpdUpdate" mbUpdate decodeExpr
  LF1.ExprSumApp (LF1.Expr_App mbFun args) -> do
    fun <- mayDecode "Expr_AppFun" mbFun decodeExpr
    foldl' ETmApp fun <$> mapM decodeExpr (V.toList args)
  LF1.ExprSumTyApp (LF1.Expr_TyApp mbFun args) -> do
    fun <- mayDecode "Expr_TyAppFun" mbFun decodeExpr
    foldl' ETyApp fun <$> mapM decodeType (V.toList args)
  LF1.ExprSumAbs (LF1.Expr_Abs params mbBody) -> do
    body <- mayDecode "Expr_AbsBody" mbBody decodeExpr
    foldr ETmLam body <$> mapM decodeVarWithType (V.toList params)
  LF1.ExprSumTyAbs (LF1.Expr_TyAbs params mbBody) -> do
    body <- mayDecode "Expr_TyAbsBody" mbBody decodeExpr
    foldr ETyLam body <$> traverse decodeTypeVarWithKind (V.toList params)
  LF1.ExprSumCase (LF1.Case mbScrut alts) ->
    ECase
      <$> mayDecode "Case_caseScrut" mbScrut decodeExpr
      <*> mapM decodeCaseAlt (V.toList alts)
  LF1.ExprSumLet (LF1.Block lets mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr ELet body <$> mapM decodeBinding (V.toList lets)
  LF1.ExprSumNil (LF1.Expr_Nil mbType) ->
    ENil <$> mayDecode "expr_NilType" mbType decodeType
  LF1.ExprSumCons (LF1.Expr_Cons mbType front mbTail) -> do
    ctype <- mayDecode "expr_ConsType" mbType decodeType
    ctail <- mayDecode "expr_ConsTail" mbTail decodeExpr
    foldr (ECons ctype) ctail <$> mapM decodeExpr (V.toList front)
  LF1.ExprSumUpdate upd ->
    decodeUpdate upd
  LF1.ExprSumScenario scen ->
    decodeScenario scen
  LF1.ExprSumOptionalNone (LF1.Expr_OptionalNone mbType) -> do
    bodyType <- mayDecode "expr_OptionalNoneType" mbType decodeType
    return (ENone bodyType)
  LF1.ExprSumOptionalSome (LF1.Expr_OptionalSome mbType mbBody) -> do
    bodyType <- mayDecode "expr_OptionalSomeType" mbType decodeType
    bodyExpr <- mayDecode "expr_OptionalSomeBody" mbBody decodeExpr
    return (ESome bodyType bodyExpr)
  LF1.ExprSumToAny (LF1.Expr_ToAny mbType mbExpr) -> do
    type' <- mayDecode "expr_ToAnyType" mbType decodeType
    body <- mayDecode "expr_toAnyExpr" mbExpr decodeExpr
    return (EToAny type' body)
  LF1.ExprSumFromAny (LF1.Expr_FromAny mbType mbExpr) -> do
    type' <- mayDecode "expr_FromAnyType" mbType decodeType
    expr <- mayDecode "expr_FromAnyExpr" mbExpr decodeExpr
    return (EFromAny type' expr)
  LF1.ExprSumTypeRep typ ->
    ETypeRep <$> decodeType typ
  LF1.ExprSumToAnyException LF1.Expr_ToAnyException {..} -> EToAnyException
    <$> mayDecode "expr_ToAnyExceptionType" expr_ToAnyExceptionType decodeType
    <*> mayDecode "expr_ToAnyExceptionExpr" expr_ToAnyExceptionExpr decodeExpr
  LF1.ExprSumFromAnyException LF1.Expr_FromAnyException {..} -> EFromAnyException
    <$> mayDecode "expr_FromAnyExceptionType" expr_FromAnyExceptionType decodeType
    <*> mayDecode "expr_FromAnyExceptionExpr" expr_FromAnyExceptionExpr decodeExpr
  LF1.ExprSumThrow LF1.Expr_Throw {..} -> EThrow
    <$> mayDecode "expr_ThrowReturnType" expr_ThrowReturnType decodeType
    <*> mayDecode "expr_ThrowExceptionType" expr_ThrowExceptionType decodeType
    <*> mayDecode "expr_ThrowExceptionExpr" expr_ThrowExceptionExpr decodeExpr
  LF1.ExprSumToInterface LF1.Expr_ToInterface {..} -> EToInterface
    <$> mayDecode "expr_ToInterfaceInterfaceType" expr_ToInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_ToInterfaceTemplateType" expr_ToInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_ToInterfaceTemplateExpr" expr_ToInterfaceTemplateExpr decodeExpr
  LF1.ExprSumFromInterface LF1.Expr_FromInterface {..} -> EFromInterface
    <$> mayDecode "expr_FromInterfaceInterfaceType" expr_FromInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_FromInterfaceTemplateType" expr_FromInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_FromInterfaceInterfaceExpr" expr_FromInterfaceInterfaceExpr decodeExpr
  LF1.ExprSumUnsafeFromInterface LF1.Expr_UnsafeFromInterface {..} -> EUnsafeFromInterface
    <$> mayDecode "expr_UnsafeFromInterfaceInterfaceType" expr_UnsafeFromInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_UnsafeFromInterfaceTemplateType" expr_UnsafeFromInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_UnsafeFromInterfaceContractIdExpr" expr_UnsafeFromInterfaceContractIdExpr decodeExpr
    <*> mayDecode "expr_UnsafeFromInterfaceInterfaceExpr" expr_UnsafeFromInterfaceInterfaceExpr decodeExpr
  LF1.ExprSumCallInterface LF1.Expr_CallInterface {..} -> ECallInterface
    <$> mayDecode "expr_CallInterfaceInterfaceType" expr_CallInterfaceInterfaceType decodeTypeConName
    <*> decodeMethodName expr_CallInterfaceMethodInternedName
    <*> mayDecode "expr_CallInterfaceInterfaceExpr" expr_CallInterfaceInterfaceExpr decodeExpr
  LF1.ExprSumToRequiredInterface LF1.Expr_ToRequiredInterface {..} -> EToRequiredInterface
    <$> mayDecode "expr_ToRequiredInterfaceRequiredInterface" expr_ToRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_ToRequiredInterfaceRequiringInterface" expr_ToRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_ToRequiredInterfaceExpr" expr_ToRequiredInterfaceExpr decodeExpr
  LF1.ExprSumFromRequiredInterface LF1.Expr_FromRequiredInterface {..} -> EFromRequiredInterface
    <$> mayDecode "expr_FromRequiredInterfaceRequiredInterface" expr_FromRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_FromRequiredInterfaceRequiringInterface" expr_FromRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_FromRequiredInterfaceExpr" expr_FromRequiredInterfaceExpr decodeExpr
  LF1.ExprSumUnsafeFromRequiredInterface LF1.Expr_UnsafeFromRequiredInterface {..} -> EUnsafeFromRequiredInterface
    <$> mayDecode "expr_UnsafeFromRequiredInterfaceRequiredInterface" expr_UnsafeFromRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceRequiringInterface" expr_UnsafeFromRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceContractIdExpr" expr_UnsafeFromRequiredInterfaceContractIdExpr decodeExpr
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceInterfaceExpr" expr_UnsafeFromRequiredInterfaceInterfaceExpr decodeExpr
  LF1.ExprSumInterfaceTemplateTypeRep LF1.Expr_InterfaceTemplateTypeRep {..} -> EInterfaceTemplateTypeRep
    <$> mayDecode "expr_InterfaceTemplateTypeRepInterface" expr_InterfaceTemplateTypeRepInterface decodeTypeConName
    <*> mayDecode "expr_InterfaceTemplateTypeRepExpr" expr_InterfaceTemplateTypeRepExpr decodeExpr
  LF1.ExprSumSignatoryInterface LF1.Expr_SignatoryInterface {..} -> ESignatoryInterface
    <$> mayDecode "expr_SignatoryInterfaceInterface" expr_SignatoryInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_SignatoryInterfaceExpr" expr_SignatoryInterfaceExpr decodeExpr
  LF1.ExprSumObserverInterface LF1.Expr_ObserverInterface {..} -> EObserverInterface
    <$> mayDecode "expr_ObserverInterfaceInterface" expr_ObserverInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_ObserverInterfaceExpr" expr_ObserverInterfaceExpr decodeExpr
  LF1.ExprSumViewInterface LF1.Expr_ViewInterface {..} -> EViewInterface
    <$> mayDecode "expr_ViewInterfaceInterface" expr_ViewInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_ViewInterfaceExpr" expr_ViewInterfaceExpr decodeExpr
  LF1.ExprSumExperimental (LF1.Expr_Experimental name mbType) -> do
    ty <- mayDecode "expr_Experimental" mbType decodeType
    pure $ EExperimental (decodeString name) ty

decodeUpdate :: LF1.Update -> Decode Expr
decodeUpdate LF1.Update{..} = mayDecode "updateSum" updateSum $ \case
  LF1.UpdateSumPure (LF1.Pure mbType mbExpr) ->
    fmap EUpdate $ UPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF1.UpdateSumBlock (LF1.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EUpdate $ UBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF1.UpdateSumCreate (LF1.Update_Create mbTycon mbExpr) ->
    fmap EUpdate $ UCreate
      <$> mayDecode "update_CreateTemplate" mbTycon decodeTypeConName
      <*> mayDecode "update_CreateExpr" mbExpr decodeExpr
  LF1.UpdateSumCreateInterface (LF1.Update_CreateInterface mbTycon mbExpr) ->
    fmap EUpdate $ UCreateInterface
      <$> mayDecode "update_CreateInterfaceInterface" mbTycon decodeTypeConName
      <*> mayDecode "update_CreateInterfaceExpr" mbExpr decodeExpr
  LF1.UpdateSumExercise LF1.Update_Exercise{..} ->
    fmap EUpdate $ UExercise
      <$> mayDecode "update_ExerciseTemplate" update_ExerciseTemplate decodeTypeConName
      <*> decodeName ChoiceName update_ExerciseChoice
      <*> mayDecode "update_ExerciseCid" update_ExerciseCid decodeExpr
      <*> mayDecode "update_ExerciseArg" update_ExerciseArg decodeExpr
  LF1.UpdateSumExerciseInterface LF1.Update_ExerciseInterface{..} ->
    fmap EUpdate $ UExerciseInterface
      <$> mayDecode "update_ExerciseInterfaceInterface" update_ExerciseInterfaceInterface decodeTypeConName
      <*> decodeNameId ChoiceName update_ExerciseInterfaceChoiceInternedStr
      <*> mayDecode "update_ExerciseInterfaceCid" update_ExerciseInterfaceCid decodeExpr
      <*> mayDecode "update_ExerciseInterfaceArg" update_ExerciseInterfaceArg decodeExpr
      <*> traverse decodeExpr update_ExerciseInterfaceGuard
  LF1.UpdateSumExerciseByKey LF1.Update_ExerciseByKey{..} ->
    fmap EUpdate $ UExerciseByKey
      <$> mayDecode "update_ExerciseByKeyTemplate" update_ExerciseByKeyTemplate decodeTypeConName
      <*> decodeNameId ChoiceName update_ExerciseByKeyChoiceInternedStr
      <*> mayDecode "update_ExerciseByKeyKey" update_ExerciseByKeyKey decodeExpr
      <*> mayDecode "update_ExerciseByKeyArg" update_ExerciseByKeyArg decodeExpr
  LF1.UpdateSumFetch LF1.Update_Fetch{..} ->
    fmap EUpdate $ UFetch
      <$> mayDecode "update_FetchTemplate" update_FetchTemplate decodeTypeConName
      <*> mayDecode "update_FetchCid" update_FetchCid decodeExpr
  LF1.UpdateSumFetchInterface LF1.Update_FetchInterface{..} ->
    fmap EUpdate $ UFetchInterface
      <$> mayDecode "update_FetchInterfaceInterface" update_FetchInterfaceInterface decodeTypeConName
      <*> mayDecode "update_FetchInterfaceCid" update_FetchInterfaceCid decodeExpr
  LF1.UpdateSumGetTime LF1.Unit ->
    pure (EUpdate UGetTime)
  LF1.UpdateSumEmbedExpr LF1.Update_EmbedExpr{..} ->
    fmap EUpdate $ UEmbedExpr
      <$> mayDecode "update_EmbedExprType" update_EmbedExprType decodeType
      <*> mayDecode "update_EmbedExprBody" update_EmbedExprBody decodeExpr
  LF1.UpdateSumLookupByKey retrieveByKey ->
    fmap (EUpdate . ULookupByKey) (decodeRetrieveByKey retrieveByKey)
  LF1.UpdateSumFetchByKey retrieveByKey ->
    fmap (EUpdate . UFetchByKey) (decodeRetrieveByKey retrieveByKey)
  LF1.UpdateSumTryCatch LF1.Update_TryCatch{..} ->
    fmap EUpdate $ UTryCatch
      <$> mayDecode "update_TryCatchReturnType" update_TryCatchReturnType decodeType
      <*> mayDecode "update_TryCatchTryExpr" update_TryCatchTryExpr decodeExpr
      <*> decodeNameId ExprVarName update_TryCatchVarInternedStr
      <*> mayDecode "update_TryCatchCatchExpr" update_TryCatchCatchExpr decodeExpr

decodeRetrieveByKey :: LF1.Update_RetrieveByKey -> Decode RetrieveByKey
decodeRetrieveByKey LF1.Update_RetrieveByKey{..} = RetrieveByKey
  <$> mayDecode "update_RetrieveByKeyTemplate" update_RetrieveByKeyTemplate decodeTypeConName
  <*> mayDecode "update_RetrieveByKeyKey" update_RetrieveByKeyKey decodeExpr

decodeScenario :: LF1.Scenario -> Decode Expr
decodeScenario LF1.Scenario{..} = mayDecode "scenarioSum" scenarioSum $ \case
  LF1.ScenarioSumPure (LF1.Pure mbType mbExpr) ->
    fmap EScenario $ SPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF1.ScenarioSumBlock (LF1.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EScenario $ SBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF1.ScenarioSumCommit LF1.Scenario_Commit{..} ->
    fmap EScenario $ SCommit
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF1.ScenarioSumMustFailAt LF1.Scenario_Commit{..} ->
    fmap EScenario $ SMustFailAt
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF1.ScenarioSumPass delta ->
    EScenario . SPass <$> decodeExpr delta
  LF1.ScenarioSumGetTime LF1.Unit ->
    pure (EScenario SGetTime)
  LF1.ScenarioSumGetParty name ->
    EScenario . SGetParty <$> decodeExpr name
  LF1.ScenarioSumEmbedExpr LF1.Scenario_EmbedExpr{..} ->
    fmap EScenario $ SEmbedExpr
      <$> mayDecode "scenario_EmbedExprType" scenario_EmbedExprType decodeType
      <*> mayDecode "scenario_EmbedExprBody" scenario_EmbedExprBody decodeExpr

decodeCaseAlt :: LF1.CaseAlt -> Decode CaseAlternative
decodeCaseAlt LF1.CaseAlt{..} = do
  pat <- mayDecode "caseAltSum" caseAltSum $ \case
    LF1.CaseAltSumDefault LF1.Unit -> pure CPDefault
    LF1.CaseAltSumVariant LF1.CaseAlt_Variant{..} ->
      CPVariant
        <$> mayDecode "caseAlt_VariantCon" caseAlt_VariantCon decodeTypeConName
        <*> decodeName VariantConName caseAlt_VariantVariant
        <*> decodeName ExprVarName caseAlt_VariantBinder
    LF1.CaseAltSumEnum LF1.CaseAlt_Enum{..} ->
      CPEnum
        <$> mayDecode "caseAlt_DataCon" caseAlt_EnumCon decodeTypeConName
        <*> decodeName VariantConName caseAlt_EnumConstructor
    LF1.CaseAltSumPrimCon (Proto.Enumerated (Right pcon)) -> pure $ case pcon of
      LF1.PrimConCON_UNIT -> CPUnit
      LF1.PrimConCON_TRUE -> CPBool True
      LF1.PrimConCON_FALSE -> CPBool False
    LF1.CaseAltSumPrimCon (Proto.Enumerated (Left idx)) ->
      throwError (UnknownEnum "CaseAltSumPrimCon" idx)
    LF1.CaseAltSumNil LF1.Unit -> pure CPNil
    LF1.CaseAltSumCons LF1.CaseAlt_Cons{..} ->
      CPCons <$> decodeName ExprVarName caseAlt_ConsVarHead <*> decodeName ExprVarName caseAlt_ConsVarTail
    LF1.CaseAltSumOptionalNone LF1.Unit -> pure CPNone
    LF1.CaseAltSumOptionalSome LF1.CaseAlt_OptionalSome{..} ->
      CPSome <$> decodeName ExprVarName caseAlt_OptionalSomeVarBody
  body <- mayDecode "caseAltBody" caseAltBody decodeExpr
  pure $ CaseAlternative pat body

decodeBinding :: LF1.Binding -> Decode Binding
decodeBinding (LF1.Binding mbBinder mbBound) =
  Binding
    <$> mayDecode "bindingBinder" mbBinder decodeVarWithType
    <*> mayDecode "bindingBound" mbBound decodeExpr

decodeTypeVarWithKind :: LF1.TypeVarWithKind -> Decode (TypeVarName, Kind)
decodeTypeVarWithKind LF1.TypeVarWithKind{..} =
  (,)
    <$> decodeName TypeVarName typeVarWithKindVar
    <*> mayDecode "typeVarWithKindKind" typeVarWithKindKind decodeKind

decodeVarWithType :: LF1.VarWithType -> Decode (ExprVarName, Type)
decodeVarWithType LF1.VarWithType{..} =
  (,)
    <$> decodeName ExprVarName varWithTypeVar
    <*> mayDecode "varWithTypeType" varWithTypeType decodeType

decodePrimLit :: LF1.PrimLit -> Decode BuiltinExpr
decodePrimLit (LF1.PrimLit mbSum) = mayDecode "primLitSum" mbSum $ \case
  LF1.PrimLitSumInt64 sInt -> pure $ BEInt64 sInt
  LF1.PrimLitSumNumericInternedStr strId -> lookupString strId >>= decodeNumericLit . fst
  LF1.PrimLitSumTimestamp sTime -> pure $ BETimestamp sTime
  LF1.PrimLitSumTextStr x -> pure $ BEText $ decodeString x
  LF1.PrimLitSumTextInternedStr strId ->  BEText . fst <$> lookupString strId
  LF1.PrimLitSumPartyStr _ ->
      throwError (ParseError "Party literals are not supported")
  LF1.PrimLitSumPartyInternedStr _ ->
      throwError (ParseError "Party literals are not supported")
  LF1.PrimLitSumDate days -> pure $ BEDate days
  LF1.PrimLitSumRoundingMode enum -> case enum of
    Proto.Enumerated (Right mode) -> pure $ case mode of
       LF1.PrimLit_RoundingModeUP -> BERoundingMode LitRoundingUp
       LF1.PrimLit_RoundingModeDOWN -> BERoundingMode LitRoundingDown
       LF1.PrimLit_RoundingModeCEILING -> BERoundingMode LitRoundingCeiling
       LF1.PrimLit_RoundingModeFLOOR -> BERoundingMode LitRoundingFloor
       LF1.PrimLit_RoundingModeHALF_UP -> BERoundingMode LitRoundingHalfUp
       LF1.PrimLit_RoundingModeHALF_DOWN -> BERoundingMode LitRoundingHalfDown
       LF1.PrimLit_RoundingModeHALF_EVEN -> BERoundingMode LitRoundingHalfEven
       LF1.PrimLit_RoundingModeUNNECESSARY -> BERoundingMode LitRoundingUnnecessary
    Proto.Enumerated (Left idx) -> throwError (UnknownEnum "PrimLitSumRoundingMode" idx)
  LF1.PrimLitSumDecimalStr _ -> unsupportedDecimal

decodeNumericLit :: T.Text -> Decode BuiltinExpr
decodeNumericLit (T.unpack -> str) = case readMaybe str of
    Nothing -> throwError $ ParseError $ "bad Numeric literal: " ++ show str
    Just n -> pure $ BENumeric n


decodeKind :: LF1.Kind -> Decode Kind
decodeKind LF1.Kind{..} = mayDecode "kindSum" kindSum $ \case
  LF1.KindSumStar LF1.Unit -> pure KStar
  LF1.KindSumNat LF1.Unit -> pure KNat
  LF1.KindSumArrow (LF1.Kind_Arrow params mbResult) -> do
    result <- mayDecode "kind_ArrowResult" mbResult decodeKind
    foldr KArrow result <$> traverse decodeKind (V.toList params)

decodePrim :: LF1.PrimType -> Decode BuiltinType
decodePrim = \case
  LF1.PrimTypeINT64 -> pure BTInt64
  LF1.PrimTypeNUMERIC -> pure BTNumeric
  LF1.PrimTypeTEXT    -> pure BTText
  LF1.PrimTypeTIMESTAMP -> pure BTTimestamp
  LF1.PrimTypePARTY   -> pure BTParty
  LF1.PrimTypeUNIT    -> pure BTUnit
  LF1.PrimTypeBOOL    -> pure BTBool
  LF1.PrimTypeLIST    -> pure BTList
  LF1.PrimTypeUPDATE  -> pure BTUpdate
  LF1.PrimTypeSCENARIO -> pure BTScenario
  LF1.PrimTypeDATE -> pure BTDate
  LF1.PrimTypeCONTRACT_ID -> pure BTContractId
  LF1.PrimTypeOPTIONAL -> pure BTOptional
  LF1.PrimTypeTEXTMAP -> pure BTTextMap
  LF1.PrimTypeGENMAP -> pure BTGenMap
  LF1.PrimTypeARROW -> pure BTArrow
  LF1.PrimTypeANY -> pure BTAny
  LF1.PrimTypeTYPE_REP -> pure BTTypeRep
  LF1.PrimTypeROUNDING_MODE -> pure BTRoundingMode
  LF1.PrimTypeBIGNUMERIC -> pure BTBigNumeric
  LF1.PrimTypeANY_EXCEPTION -> pure BTAnyException
  LF1.PrimTypeDECIMAL -> unsupportedDecimal

decodeTypeLevelNat :: Integer -> Decode TypeLevelNat
decodeTypeLevelNat m =
    case typeLevelNatE m of
        Left TLNEOutOfBounds ->
            throwError $ ParseError $ "bad type-level nat: " <> show m <> " is out of bounds"
        Right n ->
            pure n

decodeType :: LF1.Type -> Decode Type
decodeType LF1.Type{..} = mayDecode "typeSum" typeSum $ \case
  LF1.TypeSumVar (LF1.Type_Var var args) ->
    decodeWithArgs args $ TVar <$> decodeName TypeVarName var
  LF1.TypeSumNat n -> TNat <$> decodeTypeLevelNat (fromIntegral n)
  LF1.TypeSumCon (LF1.Type_Con mbCon args) ->
    decodeWithArgs args $ TCon <$> mayDecode "type_ConTycon" mbCon decodeTypeConName
  LF1.TypeSumSyn (LF1.Type_Syn mbSyn args) ->
    TSynApp <$> mayDecode "type_SynTysyn" mbSyn decodeTypeSynName <*> traverse decodeType (V.toList args)
  LF1.TypeSumPrim (LF1.Type_Prim (Proto.Enumerated (Right prim)) args) -> do
    decodeWithArgs args $ TBuiltin <$> decodePrim prim
  LF1.TypeSumPrim (LF1.Type_Prim (Proto.Enumerated (Left idx)) _args) ->
    throwError (UnknownEnum "Prim" idx)
  LF1.TypeSumForall (LF1.Type_Forall binders mbBody) -> do
    body <- mayDecode "type_ForAllBody" mbBody decodeType
    foldr TForall body <$> traverse decodeTypeVarWithKind (V.toList binders)
  LF1.TypeSumStruct (LF1.Type_Struct flds) ->
    TStruct <$> mapM (decodeFieldWithType FieldName) (V.toList flds)
  LF1.TypeSumInterned n -> do
    DecodeEnv{internedTypes} <- ask
    lookupInterned internedTypes BadTypeId n
  where
    decodeWithArgs :: V.Vector LF1.Type -> Decode Type -> Decode Type
    decodeWithArgs args fun = foldl' TApp <$> fun <*> traverse decodeType args


decodeFieldWithType :: (T.Text -> a) -> LF1.FieldWithType -> Decode (a, Type)
decodeFieldWithType wrapName (LF1.FieldWithType name mbType) =
  (,)
    <$> decodeName wrapName name
    <*> mayDecode "fieldWithTypeType" mbType decodeType

decodeFieldWithExpr :: LF1.FieldWithExpr -> Decode (FieldName, Expr)
decodeFieldWithExpr (LF1.FieldWithExpr name mbExpr) =
  (,)
    <$> decodeName FieldName name
    <*> mayDecode "fieldWithExprExpr" mbExpr decodeExpr

decodeTypeConApp :: LF1.Type_Con -> Decode TypeConApp
decodeTypeConApp LF1.Type_Con{..} =
  TypeConApp
    <$> mayDecode "typeConAppTycon" type_ConTycon decodeTypeConName
    <*> mapM decodeType (V.toList type_ConArgs)

decodeTypeSynName :: LF1.TypeSynName -> Decode (Qualified TypeSynName)
decodeTypeSynName LF1.TypeSynName{..} = do
  (pref, mname) <- mayDecode "typeSynNameModule" typeSynNameModule decodeModuleRef
  syn <- decodeDottedName TypeSynName typeSynNameName
  pure $ Qualified pref mname syn

decodeTypeConName :: LF1.TypeConName -> Decode (Qualified TypeConName)
decodeTypeConName LF1.TypeConName{..} = do
  (pref, mname) <- mayDecode "typeConNameModule" typeConNameModule decodeModuleRef
  con <- decodeDottedName TypeConName typeConNameName
  pure $ Qualified pref mname con

decodeModuleRef :: LF1.ModuleRef -> Decode (PackageRef, ModuleName)
decodeModuleRef LF1.ModuleRef{..} =
  (,)
    <$> mayDecode "moduleRefPackageRef" moduleRefPackageRef decodePackageRef
    <*> decodeDottedName ModuleName moduleRefModuleName

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

mayDecode :: String -> Maybe a -> (a -> Decode b) -> Decode b
mayDecode fieldName mb f =
  case mb of
    Nothing -> throwError (MissingField fieldName)
    Just x  -> f x

decodeNM
  :: NM.Named b
  => (NM.Name b -> Error) -> (a -> Decode b) -> V.Vector a -> Decode (NM.NameMap b)
decodeNM mkDuplicateError decode1 xs = do
  ys <- traverse decode1 (V.toList xs)
  either (throwError . mkDuplicateError) pure $ NM.fromListEither ys

decodeSet :: Ord b => (b -> Error) -> (a -> Decode b) -> V.Vector a -> Decode (S.Set b)
decodeSet mkDuplicateError decode1 xs = do
    ys <- traverse decode1 (V.toList xs)
    foldM insertAndCheck S.empty ys
  where
      insertAndCheck !accum item =
        if S.member item accum
          then throwError (mkDuplicateError item)
          else pure (S.insert item accum)

unsupportedDecimal :: Decode a
unsupportedDecimal = throwError (ParseError "Decimal is unsupported in LF >= 1.8")
