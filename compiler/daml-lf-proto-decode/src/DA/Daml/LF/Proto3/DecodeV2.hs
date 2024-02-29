-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE TypeFamilies #-}

module DA.Daml.LF.Proto3.DecodeV2
    ( decodePackage
    , decodeScenarioModule
    , Error(..)
    ) where

import           DA.Daml.LF.Ast as LF
import           DA.Daml.LF.Proto3.Error
import Data.Coerce
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Data.Int
import Text.Read
import           Data.List
import           DA.Daml.LF.Mangling
import qualified Com.Daml.DamlLfDev.DamlLf2 as LF2
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
-- constructor. All compononents are mangled. Dotted names will be interned.
decodeDottedName :: ([T.Text] -> a) -> Int32 -> Decode a
decodeDottedName wrapDottedName dNameId = do
    (_, unmangledOrErr) <- lookupDottedName dNameId
    case unmangledOrErr of
      Left err -> throwError $ ParseError err
      Right unmangled -> pure $ wrapDottedName (coerce unmangled)

decodeDottedNameId :: ([T.Text] -> a) -> Int32 -> Decode a
decodeDottedNameId wrapDottedName dnId = do
  (_, unmangledOrErr) <- lookupDottedName dnId
  case unmangledOrErr of
    Left err -> throwError $ ParseError err
    Right unmangled -> pure $ wrapDottedName (coerce unmangled)

-- | Decode the name of a top-level value. The name is mangled and Iwill be
-- interned in Daml-LF 1.7 and onwards.
decodeValueName :: String -> Int32 -> Decode ExprValName
decodeValueName ident dnId = do
    (mangled, unmangledOrErr) <- lookupDottedName dnId
    case unmangledOrErr of
      Left err -> throwError $ ParseError err
      Right [UnmangledIdentifier unmangled] -> pure $ ExprValName unmangled
      Right [] -> throwError $ MissingField ident
      Right _ ->
          throwError $ ParseError $ "Unexpected multi-segment def name: " ++ show mangled

-- | Decode a reference to a top-level value. The name is mangled and will be
-- interned in Daml-LF 1.7 and onwards.
decodeValName :: LF2.ValName -> Decode (Qualified ExprValName)
decodeValName LF2.ValName{..} = do
  (pref, mname) <- mayDecode "valNameModule" valNameModule decodeModuleRef
  name <- decodeValueName "valNameName" valNameNameInternedDname
  pure $ Qualified pref mname name

-- | Decode a reference to a package. Package names are not mangled. Package
-- name are interned since Daml-LF 1.6.
decodePackageRef :: LF2.PackageRef -> Decode PackageRef
decodePackageRef (LF2.PackageRef pref) =
    mayDecode "packageRefSum" pref $ \case
        LF2.PackageRefSumSelf _ -> asks selfPackageRef
        LF2.PackageRefSumPackageIdInternedStr strId -> PRImport . PackageId . fst <$> lookupString strId

------------------------------------------------------------------------
-- Decodings of everything else
------------------------------------------------------------------------

decodeInternedDottedName :: LF2.InternedDottedName -> Decode ([T.Text], Either String [UnmangledIdentifier])
decodeInternedDottedName (LF2.InternedDottedName ids) = do
    (mangled, unmangledOrErr) <- mapAndUnzipM lookupString (V.toList ids)
    pure (mangled, sequence unmangledOrErr)

decodePackage :: LF.Version -> LF.PackageRef -> LF2.Package -> Either Error Package
decodePackage version selfPackageRef (LF2.Package mods internedStringsV internedDottedNamesV mMetadata internedTypesV)
  | Nothing <- mMetadata  =
      throwError (ParseError "missing package metadata")
  | Just metadata <- mMetadata = do
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
        Package version <$> decodeNM DuplicateModule decodeModule mods <*> decodePackageMetadata metadata

decodeUpgradedPackageId :: LF2.UpgradedPackageId -> Decode PackageId
decodeUpgradedPackageId LF2.UpgradedPackageId {..} =
  PackageId . fst <$> lookupString upgradedPackageIdUpgradedPackageIdInternedStr

decodePackageMetadata :: LF2.PackageMetadata -> Decode PackageMetadata
decodePackageMetadata LF2.PackageMetadata{..} = do
    pkgName <- PackageName . fst <$> lookupString packageMetadataNameInternedStr
    pkgVersion <- PackageVersion . fst <$> lookupString packageMetadataVersionInternedStr
    upgradedPackageId <- traverse decodeUpgradedPackageId packageMetadataUpgradedPackageId
    pure (PackageMetadata pkgName pkgVersion upgradedPackageId)

decodeScenarioModule :: LF.Version -> LF2.Package -> Either Error Module
decodeScenarioModule version protoPkg = do
    Package { packageModules = modules } <- decodePackage version PRSelf protoPkg
    pure $ head $ NM.toList modules

decodeModule :: LF2.Module -> Decode Module
decodeModule (LF2.Module name flags synonyms dataTypes values templates exceptions interfaces) =
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

decodeDefInterface :: LF2.DefInterface -> Decode DefInterface
decodeDefInterface LF2.DefInterface {..} = do
  intLocation <- traverse decodeLocation defInterfaceLocation
  intName <- decodeDottedNameId TypeConName defInterfaceTyconInternedDname
  intRequires <- decodeSet DuplicateRequires decodeTypeConName defInterfaceRequires
  intParam <- decodeNameId ExprVarName defInterfaceParamInternedStr
  intChoices <- decodeNM DuplicateChoice decodeChoice defInterfaceChoices
  intMethods <- decodeNM DuplicateMethod decodeInterfaceMethod defInterfaceMethods
  intView <- mayDecode "defInterfaceView" defInterfaceView decodeType
  pure DefInterface {..}

decodeInterfaceMethod :: LF2.InterfaceMethod -> Decode InterfaceMethod
decodeInterfaceMethod LF2.InterfaceMethod {..} = InterfaceMethod
  <$> traverse decodeLocation interfaceMethodLocation
  <*> decodeMethodName interfaceMethodMethodInternedName
  <*> mayDecode "interfaceMethodType" interfaceMethodType decodeType

decodeMethodName :: Int32 -> Decode MethodName
decodeMethodName = decodeNameId MethodName

decodeFeatureFlags :: LF2.FeatureFlags -> Decode FeatureFlags
decodeFeatureFlags LF2.FeatureFlags{..} =
  if not featureFlagsDontDivulgeContractIdsInCreateArguments || not featureFlagsDontDiscloseNonConsumingChoicesToObservers || not featureFlagsForbidPartyLiterals
    -- We do not support these anymore -- see #157
    then throwError (ParseError "Package uses unsupported flags dontDivulgeContractIdsInCreateArguments, dontDiscloseNonConsumingChoicesToObservers or featureFlagsForbidPartyLiterals")
    else pure FeatureFlags

decodeDefTypeSyn :: LF2.DefTypeSyn -> Decode DefTypeSyn
decodeDefTypeSyn LF2.DefTypeSyn{..} =
  DefTypeSyn
    <$> traverse decodeLocation defTypeSynLocation
    <*> decodeDottedName TypeSynName defTypeSynNameInternedDname
    <*> traverse decodeTypeVarWithKind (V.toList defTypeSynParams)
    <*> mayDecode "typeSynType" defTypeSynType decodeType

decodeDefException :: LF2.DefException -> Decode DefException
decodeDefException LF2.DefException{..} =
  DefException
    <$> traverse decodeLocation defExceptionLocation
    <*> decodeDottedNameId TypeConName defExceptionNameInternedDname
    <*> mayDecode "exceptionMessage" defExceptionMessage decodeExpr

decodeDefDataType :: LF2.DefDataType -> Decode DefDataType
decodeDefDataType LF2.DefDataType{..} =
  DefDataType
    <$> traverse decodeLocation defDataTypeLocation
    <*> decodeDottedName TypeConName defDataTypeNameInternedDname
    <*> pure (IsSerializable defDataTypeSerializable)
    <*> traverse decodeTypeVarWithKind (V.toList defDataTypeParams)
    <*> mayDecode "dataTypeDataCons" defDataTypeDataCons decodeDataCons

decodeDataCons :: LF2.DefDataTypeDataCons -> Decode DataCons
decodeDataCons = \case
  LF2.DefDataTypeDataConsRecord (LF2.DefDataType_Fields fs) ->
    DataRecord <$> mapM (decodeFieldWithType FieldName) (V.toList fs)
  LF2.DefDataTypeDataConsVariant (LF2.DefDataType_Fields fs) ->
    DataVariant <$> mapM (decodeFieldWithType VariantConName) (V.toList fs)
  LF2.DefDataTypeDataConsEnum (LF2.DefDataType_EnumConstructors cIds) -> do
    unmangledOrErr <- mapM (fmap snd . lookupString) (V.toList cIds)
    DataEnum <$> mapM (decodeNameString VariantConName) unmangledOrErr
  LF2.DefDataTypeDataConsInterface LF2.Unit -> pure DataInterface

decodeDefValueNameWithType :: LF2.DefValue_NameWithType -> Decode (ExprValName, Type)
decodeDefValueNameWithType LF2.DefValue_NameWithType{..} = (,)
  <$> decodeValueName "defValueName" defValue_NameWithTypeNameInternedDname
  <*> mayDecode "defValueType" defValue_NameWithTypeType decodeType

decodeDefValue :: LF2.DefValue -> Decode DefValue
decodeDefValue (LF2.DefValue mbBinder mbBody noParties isTest mbLoc) = do
  when (not noParties) $
    throwError (ParseError "DefValue uses unsupported no_party_literals flag")
  DefValue
    <$> traverse decodeLocation mbLoc
    <*> mayDecode "defValueName" mbBinder decodeDefValueNameWithType
    <*> pure (IsTest isTest)
    <*> mayDecode "defValueExpr" mbBody decodeExpr

decodeDefTemplate :: LF2.DefTemplate -> Decode Template
decodeDefTemplate LF2.DefTemplate{..} = do
  tplParam <- decodeNameId ExprVarName defTemplateParamInternedStr
  Template
    <$> traverse decodeLocation defTemplateLocation
    <*> decodeDottedName TypeConName defTemplateTyconInternedDname
    <*> pure tplParam
    <*> mayDecode "defTemplatePrecond" defTemplatePrecond decodeExpr
    <*> mayDecode "defTemplateSignatories" defTemplateSignatories decodeExpr
    <*> mayDecode "defTemplateObservers" defTemplateObservers decodeExpr
    <*> decodeNM DuplicateChoice decodeChoice defTemplateChoices
    <*> mapM decodeDefTemplateKey defTemplateKey
    <*> decodeNM DuplicateImplements decodeDefTemplateImplements defTemplateImplements

decodeDefTemplateImplements :: LF2.DefTemplate_Implements -> Decode TemplateImplements
decodeDefTemplateImplements LF2.DefTemplate_Implements{..} = TemplateImplements
  <$> mayDecode "defTemplate_ImplementsInterface" defTemplate_ImplementsInterface decodeTypeConName
  <*> mayDecode "defTemplate_ImplementsBody" defTemplate_ImplementsBody decodeInterfaceInstanceBody
  <*> traverse decodeLocation defTemplate_ImplementsLocation

decodeInterfaceInstanceBody :: LF2.InterfaceInstanceBody -> Decode InterfaceInstanceBody
decodeInterfaceInstanceBody LF2.InterfaceInstanceBody{..} = InterfaceInstanceBody
  <$> decodeNM DuplicateMethod decodeInterfaceInstanceMethod interfaceInstanceBodyMethods
  <*> mayDecode "defTemplate_ImplementsView" interfaceInstanceBodyView decodeExpr

decodeInterfaceInstanceMethod :: LF2.InterfaceInstanceBody_InterfaceInstanceMethod -> Decode InterfaceInstanceMethod
decodeInterfaceInstanceMethod LF2.InterfaceInstanceBody_InterfaceInstanceMethod{..} = InterfaceInstanceMethod
  <$> decodeMethodName interfaceInstanceBody_InterfaceInstanceMethodMethodInternedName
  <*> mayDecode "interfaceInstanceBody_InterfaceInstanceMethodValue" interfaceInstanceBody_InterfaceInstanceMethodValue decodeExpr

decodeDefTemplateKey :: LF2.DefTemplate_DefKey -> Decode TemplateKey
decodeDefTemplateKey LF2.DefTemplate_DefKey{..} = do
  typ <- mayDecode "defTemplate_DefKeyType" defTemplate_DefKeyType decodeType
  key <- mayDecode "defTemplate_DefKeyKeyExpr" defTemplate_DefKeyKeyExpr decodeExpr
  maintainers <- mayDecode "defTemplate_DefKeyMaintainers" defTemplate_DefKeyMaintainers decodeExpr
  return (TemplateKey typ key maintainers)

decodeChoice :: LF2.TemplateChoice -> Decode TemplateChoice
decodeChoice LF2.TemplateChoice{..} =
  TemplateChoice
    <$> traverse decodeLocation templateChoiceLocation
    <*> decodeNameId ChoiceName templateChoiceNameInternedStr
    <*> pure templateChoiceConsuming
    <*> mayDecode "templateChoiceControllers" templateChoiceControllers decodeExpr
    <*> traverse decodeExpr templateChoiceObservers
    <*> traverse decodeExpr templateChoiceAuthorizers
    <*> decodeNameId ExprVarName templateChoiceSelfBinderInternedStr
    <*> mayDecode "templateChoiceArgBinder" templateChoiceArgBinder decodeVarWithType
    <*> mayDecode "templateChoiceRetType" templateChoiceRetType decodeType
    <*> mayDecode "templateChoiceUpdate" templateChoiceUpdate decodeExpr

decodeBuiltinFunction :: LF2.BuiltinFunction -> Decode BuiltinExpr
decodeBuiltinFunction = \case
  LF2.BuiltinFunctionEQUAL -> pure BEEqualGeneric
  LF2.BuiltinFunctionLESS -> pure BELessGeneric
  LF2.BuiltinFunctionLESS_EQ -> pure BELessEqGeneric
  LF2.BuiltinFunctionGREATER -> pure BEGreaterGeneric
  LF2.BuiltinFunctionGREATER_EQ -> pure BEGreaterEqGeneric

  LF2.BuiltinFunctionEQUAL_INT64 -> pure (BEEqual BTInt64)
  LF2.BuiltinFunctionEQUAL_NUMERIC -> pure BEEqualNumeric
  LF2.BuiltinFunctionEQUAL_TEXT -> pure (BEEqual BTText)
  LF2.BuiltinFunctionEQUAL_TIMESTAMP -> pure (BEEqual BTTimestamp)
  LF2.BuiltinFunctionEQUAL_DATE -> pure (BEEqual BTDate)
  LF2.BuiltinFunctionEQUAL_PARTY -> pure (BEEqual BTParty)
  LF2.BuiltinFunctionEQUAL_BOOL -> pure (BEEqual BTBool)
  LF2.BuiltinFunctionEQUAL_TYPE_REP -> pure (BEEqual BTTypeRep)

  LF2.BuiltinFunctionLEQ_INT64 -> pure (BELessEq BTInt64)
  LF2.BuiltinFunctionLEQ_NUMERIC -> pure BELessEqNumeric
  LF2.BuiltinFunctionLEQ_TEXT -> pure (BELessEq BTText)
  LF2.BuiltinFunctionLEQ_TIMESTAMP -> pure (BELessEq BTTimestamp)
  LF2.BuiltinFunctionLEQ_DATE -> pure (BELessEq BTDate)
  LF2.BuiltinFunctionLEQ_PARTY -> pure (BELessEq BTParty)

  LF2.BuiltinFunctionLESS_INT64 -> pure (BELess BTInt64)
  LF2.BuiltinFunctionLESS_NUMERIC -> pure BELessNumeric
  LF2.BuiltinFunctionLESS_TEXT -> pure (BELess BTText)
  LF2.BuiltinFunctionLESS_TIMESTAMP -> pure (BELess BTTimestamp)
  LF2.BuiltinFunctionLESS_DATE -> pure (BELess BTDate)
  LF2.BuiltinFunctionLESS_PARTY -> pure (BELess BTParty)

  LF2.BuiltinFunctionGEQ_INT64 -> pure (BEGreaterEq BTInt64)
  LF2.BuiltinFunctionGEQ_NUMERIC -> pure BEGreaterEqNumeric
  LF2.BuiltinFunctionGEQ_TEXT -> pure (BEGreaterEq BTText)
  LF2.BuiltinFunctionGEQ_TIMESTAMP -> pure (BEGreaterEq BTTimestamp)
  LF2.BuiltinFunctionGEQ_DATE -> pure (BEGreaterEq BTDate)
  LF2.BuiltinFunctionGEQ_PARTY -> pure (BEGreaterEq BTParty)

  LF2.BuiltinFunctionGREATER_INT64 -> pure (BEGreater BTInt64)
  LF2.BuiltinFunctionGREATER_NUMERIC -> pure BEGreaterNumeric
  LF2.BuiltinFunctionGREATER_TEXT -> pure (BEGreater BTText)
  LF2.BuiltinFunctionGREATER_TIMESTAMP -> pure (BEGreater BTTimestamp)
  LF2.BuiltinFunctionGREATER_DATE -> pure (BEGreater BTDate)
  LF2.BuiltinFunctionGREATER_PARTY -> pure (BEGreater BTParty)

  LF2.BuiltinFunctionINT64_TO_TEXT -> pure (BEToText BTInt64)
  LF2.BuiltinFunctionNUMERIC_TO_TEXT -> pure BENumericToText
  LF2.BuiltinFunctionTEXT_TO_TEXT -> pure (BEToText BTText)
  LF2.BuiltinFunctionTIMESTAMP_TO_TEXT -> pure (BEToText BTTimestamp)
  LF2.BuiltinFunctionPARTY_TO_TEXT -> pure (BEToText BTParty)
  LF2.BuiltinFunctionDATE_TO_TEXT -> pure (BEToText BTDate)
  LF2.BuiltinFunctionCONTRACT_ID_TO_TEXT -> pure BEContractIdToText
  LF2.BuiltinFunctionBIGNUMERIC_TO_TEXT -> pure (BEToText BTBigNumeric)
  LF2.BuiltinFunctionCODE_POINTS_TO_TEXT -> pure BECodePointsToText
  LF2.BuiltinFunctionTEXT_TO_PARTY -> pure BETextToParty
  LF2.BuiltinFunctionTEXT_TO_INT64 -> pure BETextToInt64
  LF2.BuiltinFunctionTEXT_TO_NUMERIC -> pure BETextToNumeric
  LF2.BuiltinFunctionTEXT_TO_CODE_POINTS -> pure BETextToCodePoints
  LF2.BuiltinFunctionPARTY_TO_QUOTED_TEXT -> pure BEPartyToQuotedText

  LF2.BuiltinFunctionADD_NUMERIC   -> pure BEAddNumeric
  LF2.BuiltinFunctionSUB_NUMERIC   -> pure BESubNumeric
  LF2.BuiltinFunctionMUL_NUMERIC   -> pure  BEMulNumeric
  LF2.BuiltinFunctionDIV_NUMERIC   -> pure BEDivNumeric
  LF2.BuiltinFunctionROUND_NUMERIC -> pure BERoundNumeric
  LF2.BuiltinFunctionCAST_NUMERIC  -> pure BECastNumeric
  LF2.BuiltinFunctionSHIFT_NUMERIC -> pure BEShiftNumeric

  LF2.BuiltinFunctionADD_INT64 -> pure BEAddInt64
  LF2.BuiltinFunctionSUB_INT64 -> pure BESubInt64
  LF2.BuiltinFunctionMUL_INT64 -> pure BEMulInt64
  LF2.BuiltinFunctionDIV_INT64 -> pure BEDivInt64
  LF2.BuiltinFunctionMOD_INT64 -> pure BEModInt64
  LF2.BuiltinFunctionEXP_INT64 -> pure BEExpInt64

  LF2.BuiltinFunctionFOLDL          -> pure BEFoldl
  LF2.BuiltinFunctionFOLDR          -> pure BEFoldr
  LF2.BuiltinFunctionEQUAL_LIST     -> pure BEEqualList
  LF2.BuiltinFunctionAPPEND_TEXT    -> pure BEAppendText

  LF2.BuiltinFunctionERROR          -> pure BEError
  LF2.BuiltinFunctionANY_EXCEPTION_MESSAGE -> pure BEAnyExceptionMessage

  LF2.BuiltinFunctionTEXTMAP_EMPTY      -> pure BETextMapEmpty
  LF2.BuiltinFunctionTEXTMAP_INSERT     -> pure BETextMapInsert
  LF2.BuiltinFunctionTEXTMAP_LOOKUP     -> pure BETextMapLookup
  LF2.BuiltinFunctionTEXTMAP_DELETE     -> pure BETextMapDelete
  LF2.BuiltinFunctionTEXTMAP_TO_LIST    -> pure BETextMapToList
  LF2.BuiltinFunctionTEXTMAP_SIZE       -> pure BETextMapSize

  LF2.BuiltinFunctionGENMAP_EMPTY      -> pure BEGenMapEmpty
  LF2.BuiltinFunctionGENMAP_INSERT     -> pure BEGenMapInsert
  LF2.BuiltinFunctionGENMAP_LOOKUP     -> pure BEGenMapLookup
  LF2.BuiltinFunctionGENMAP_DELETE     -> pure BEGenMapDelete
  LF2.BuiltinFunctionGENMAP_KEYS       -> pure BEGenMapKeys
  LF2.BuiltinFunctionGENMAP_VALUES     -> pure BEGenMapValues
  LF2.BuiltinFunctionGENMAP_SIZE       -> pure BEGenMapSize

  LF2.BuiltinFunctionEXPLODE_TEXT -> pure BEExplodeText
  LF2.BuiltinFunctionIMPLODE_TEXT -> pure BEImplodeText
  LF2.BuiltinFunctionSHA256_TEXT  -> pure BESha256Text

  LF2.BuiltinFunctionDATE_TO_UNIX_DAYS -> pure BEDateToUnixDays
  LF2.BuiltinFunctionUNIX_DAYS_TO_DATE -> pure BEUnixDaysToDate
  LF2.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS -> pure BETimestampToUnixMicroseconds
  LF2.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP -> pure BEUnixMicrosecondsToTimestamp

  LF2.BuiltinFunctionINT64_TO_NUMERIC -> pure BEInt64ToNumeric
  LF2.BuiltinFunctionNUMERIC_TO_INT64 -> pure BENumericToInt64

  LF2.BuiltinFunctionTRACE -> pure BETrace
  LF2.BuiltinFunctionEQUAL_CONTRACT_ID -> pure BEEqualContractId
  LF2.BuiltinFunctionCOERCE_CONTRACT_ID -> pure BECoerceContractId

  LF2.BuiltinFunctionTYPE_REP_TYCON_NAME -> pure BETypeRepTyConName

  LF2.BuiltinFunctionSCALE_BIGNUMERIC -> pure BEScaleBigNumeric
  LF2.BuiltinFunctionPRECISION_BIGNUMERIC -> pure BEPrecisionBigNumeric
  LF2.BuiltinFunctionADD_BIGNUMERIC -> pure BEAddBigNumeric
  LF2.BuiltinFunctionSUB_BIGNUMERIC -> pure BESubBigNumeric
  LF2.BuiltinFunctionMUL_BIGNUMERIC -> pure BEMulBigNumeric
  LF2.BuiltinFunctionDIV_BIGNUMERIC -> pure BEDivBigNumeric
  LF2.BuiltinFunctionSHIFT_RIGHT_BIGNUMERIC -> pure BEShiftRightBigNumeric
  LF2.BuiltinFunctionBIGNUMERIC_TO_NUMERIC -> pure BEBigNumericToNumeric
  LF2.BuiltinFunctionNUMERIC_TO_BIGNUMERIC -> pure BENumericToBigNumeric

  LF2.BuiltinFunctionADD_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionSUB_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionMUL_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionDIV_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionROUND_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionLEQ_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionLESS_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionGEQ_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionGREATER_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionDECIMAL_TO_TEXT -> unsupportedDecimal
  LF2.BuiltinFunctionINT64_TO_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionDECIMAL_TO_INT64 -> unsupportedDecimal
  LF2.BuiltinFunctionEQUAL_DECIMAL -> unsupportedDecimal
  LF2.BuiltinFunctionTEXT_TO_DECIMAL -> unsupportedDecimal

  LF2.BuiltinFunctionTEXT_TO_NUMERIC_LEGACY -> error "The builin TEXT_TO_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionMUL_NUMERIC_LEGACY -> error "The builin MUL_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionDIV_NUMERIC_LEGACY -> error "The builin DIV_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionCAST_NUMERIC_LEGACY -> error "The builin CAST_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionSHIFT_NUMERIC_LEGACY -> error "The builin SHIFT_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionINT64_TO_NUMERIC_LEGACY -> error "The builin INT64_TO_NUMERIC_LEGACY is not supported by LF 2.x"
  LF2.BuiltinFunctionBIGNUMERIC_TO_NUMERIC_LEGACY -> error "The builin BIGNUMERIC_TO_NUMERIC_LEGACY is not supported by LF 2.x"

decodeLocation :: LF2.Location -> Decode SourceLoc
decodeLocation (LF2.Location mbModRef mbRange) = do
  mbModRef' <- traverse decodeModuleRef mbModRef
  LF2.Location_Range sline scol eline ecol <- mayDecode "Location_Range" mbRange pure
  pure $ SourceLoc
    mbModRef'
    (fromIntegral sline) (fromIntegral scol)
    (fromIntegral eline) (fromIntegral ecol)

decodeExpr :: LF2.Expr -> Decode Expr
decodeExpr (LF2.Expr mbLoc exprSum) = case mbLoc of
  Nothing -> decodeExprSum exprSum
  Just loc -> ELocation <$> decodeLocation loc <*> decodeExprSum exprSum

decodeExprSum :: Maybe LF2.ExprSum -> Decode Expr
decodeExprSum exprSum = mayDecode "exprSum" exprSum $ \case
  LF2.ExprSumVarInternedStr strId -> EVar <$> decodeNameId ExprVarName strId
  LF2.ExprSumVal val -> EVal <$> decodeValName val
  LF2.ExprSumBuiltin (Proto.Enumerated (Right bi)) -> EBuiltin <$> decodeBuiltinFunction bi
  LF2.ExprSumBuiltin (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumBuiltin" num)
  LF2.ExprSumBuiltinCon (Proto.Enumerated (Right con)) -> pure $ EBuiltin $ case con of
    LF2.BuiltinConCON_UNIT -> BEUnit
    LF2.BuiltinConCON_TRUE -> BEBool True
    LF2.BuiltinConCON_FALSE -> BEBool False

  LF2.ExprSumBuiltinCon (Proto.Enumerated (Left num)) -> throwError (UnknownEnum "ExprSumBuiltinCon" num)
  LF2.ExprSumBuiltinLit lit ->
    EBuiltin <$> decodeBuiltinLit lit
  LF2.ExprSumRecCon (LF2.Expr_RecCon mbTycon fields) ->
    ERecCon
      <$> mayDecode "Expr_RecConTycon" mbTycon decodeTypeConApp
      <*> mapM decodeFieldWithExpr (V.toList fields)
  LF2.ExprSumRecProj (LF2.Expr_RecProj mbTycon field mbRecord) ->
    ERecProj
      <$> mayDecode "Expr_RecProjTycon" mbTycon decodeTypeConApp
      <*> decodeNameId FieldName field
      <*> mayDecode "Expr_RecProjRecord" mbRecord decodeExpr
  LF2.ExprSumRecUpd (LF2.Expr_RecUpd mbTycon field mbRecord mbUpdate) ->
    ERecUpd
      <$> mayDecode "Expr_RecUpdTycon" mbTycon decodeTypeConApp
      <*> decodeNameId FieldName field
      <*> mayDecode "Expr_RecUpdRecord" mbRecord decodeExpr
      <*> mayDecode "Expr_RecUpdUpdate" mbUpdate decodeExpr
  LF2.ExprSumVariantCon (LF2.Expr_VariantCon mbTycon variant mbArg) ->
    EVariantCon
      <$> mayDecode "Expr_VariantConTycon" mbTycon decodeTypeConApp
      <*> decodeNameId VariantConName variant
      <*> mayDecode "Expr_VariantConVariantArg" mbArg decodeExpr
  LF2.ExprSumEnumCon (LF2.Expr_EnumCon mbTypeCon dataCon) ->
    EEnumCon
      <$> mayDecode "Expr_EnumConTycon" mbTypeCon decodeTypeConName
      <*> decodeNameId VariantConName dataCon
  LF2.ExprSumStructCon (LF2.Expr_StructCon fields) ->
    EStructCon
      <$> mapM decodeFieldWithExpr (V.toList fields)
  LF2.ExprSumStructProj (LF2.Expr_StructProj field mbStruct) ->
    EStructProj
      <$> decodeNameId FieldName field
      <*> mayDecode "Expr_StructProjStruct" mbStruct decodeExpr
  LF2.ExprSumStructUpd (LF2.Expr_StructUpd field mbStruct mbUpdate) ->
    EStructUpd
      <$> decodeNameId FieldName field
      <*> mayDecode "Expr_StructUpdStruct" mbStruct decodeExpr
      <*> mayDecode "Expr_StructUpdUpdate" mbUpdate decodeExpr
  LF2.ExprSumApp (LF2.Expr_App mbFun args) -> do
    fun <- mayDecode "Expr_AppFun" mbFun decodeExpr
    foldl' ETmApp fun <$> mapM decodeExpr (V.toList args)
  LF2.ExprSumTyApp (LF2.Expr_TyApp mbFun args) -> do
    fun <- mayDecode "Expr_TyAppFun" mbFun decodeExpr
    foldl' ETyApp fun <$> mapM decodeType (V.toList args)
  LF2.ExprSumAbs (LF2.Expr_Abs params mbBody) -> do
    body <- mayDecode "Expr_AbsBody" mbBody decodeExpr
    foldr ETmLam body <$> mapM decodeVarWithType (V.toList params)
  LF2.ExprSumTyAbs (LF2.Expr_TyAbs params mbBody) -> do
    body <- mayDecode "Expr_TyAbsBody" mbBody decodeExpr
    foldr ETyLam body <$> traverse decodeTypeVarWithKind (V.toList params)
  LF2.ExprSumCase (LF2.Case mbScrut alts) ->
    ECase
      <$> mayDecode "Case_caseScrut" mbScrut decodeExpr
      <*> mapM decodeCaseAlt (V.toList alts)
  LF2.ExprSumLet (LF2.Block lets mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr ELet body <$> mapM decodeBinding (V.toList lets)
  LF2.ExprSumNil (LF2.Expr_Nil mbType) ->
    ENil <$> mayDecode "expr_NilType" mbType decodeType
  LF2.ExprSumCons (LF2.Expr_Cons mbType front mbTail) -> do
    ctype <- mayDecode "expr_ConsType" mbType decodeType
    ctail <- mayDecode "expr_ConsTail" mbTail decodeExpr
    foldr (ECons ctype) ctail <$> mapM decodeExpr (V.toList front)
  LF2.ExprSumUpdate upd ->
    decodeUpdate upd
  LF2.ExprSumScenario scen ->
    decodeScenario scen
  LF2.ExprSumOptionalNone (LF2.Expr_OptionalNone mbType) -> do
    bodyType <- mayDecode "expr_OptionalNoneType" mbType decodeType
    return (ENone bodyType)
  LF2.ExprSumOptionalSome (LF2.Expr_OptionalSome mbType mbBody) -> do
    bodyType <- mayDecode "expr_OptionalSomeType" mbType decodeType
    bodyExpr <- mayDecode "expr_OptionalSomeBody" mbBody decodeExpr
    return (ESome bodyType bodyExpr)
  LF2.ExprSumToAny (LF2.Expr_ToAny mbType mbExpr) -> do
    type' <- mayDecode "expr_ToAnyType" mbType decodeType
    body <- mayDecode "expr_toAnyExpr" mbExpr decodeExpr
    return (EToAny type' body)
  LF2.ExprSumFromAny (LF2.Expr_FromAny mbType mbExpr) -> do
    type' <- mayDecode "expr_FromAnyType" mbType decodeType
    expr <- mayDecode "expr_FromAnyExpr" mbExpr decodeExpr
    return (EFromAny type' expr)
  LF2.ExprSumTypeRep typ ->
    ETypeRep <$> decodeType typ
  LF2.ExprSumToAnyException LF2.Expr_ToAnyException {..} -> EToAnyException
    <$> mayDecode "expr_ToAnyExceptionType" expr_ToAnyExceptionType decodeType
    <*> mayDecode "expr_ToAnyExceptionExpr" expr_ToAnyExceptionExpr decodeExpr
  LF2.ExprSumFromAnyException LF2.Expr_FromAnyException {..} -> EFromAnyException
    <$> mayDecode "expr_FromAnyExceptionType" expr_FromAnyExceptionType decodeType
    <*> mayDecode "expr_FromAnyExceptionExpr" expr_FromAnyExceptionExpr decodeExpr
  LF2.ExprSumThrow LF2.Expr_Throw {..} -> EThrow
    <$> mayDecode "expr_ThrowReturnType" expr_ThrowReturnType decodeType
    <*> mayDecode "expr_ThrowExceptionType" expr_ThrowExceptionType decodeType
    <*> mayDecode "expr_ThrowExceptionExpr" expr_ThrowExceptionExpr decodeExpr
  LF2.ExprSumToInterface LF2.Expr_ToInterface {..} -> EToInterface
    <$> mayDecode "expr_ToInterfaceInterfaceType" expr_ToInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_ToInterfaceTemplateType" expr_ToInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_ToInterfaceTemplateExpr" expr_ToInterfaceTemplateExpr decodeExpr
  LF2.ExprSumFromInterface LF2.Expr_FromInterface {..} -> EFromInterface
    <$> mayDecode "expr_FromInterfaceInterfaceType" expr_FromInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_FromInterfaceTemplateType" expr_FromInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_FromInterfaceInterfaceExpr" expr_FromInterfaceInterfaceExpr decodeExpr
  LF2.ExprSumUnsafeFromInterface LF2.Expr_UnsafeFromInterface {..} -> EUnsafeFromInterface
    <$> mayDecode "expr_UnsafeFromInterfaceInterfaceType" expr_UnsafeFromInterfaceInterfaceType decodeTypeConName
    <*> mayDecode "expr_UnsafeFromInterfaceTemplateType" expr_UnsafeFromInterfaceTemplateType decodeTypeConName
    <*> mayDecode "expr_UnsafeFromInterfaceContractIdExpr" expr_UnsafeFromInterfaceContractIdExpr decodeExpr
    <*> mayDecode "expr_UnsafeFromInterfaceInterfaceExpr" expr_UnsafeFromInterfaceInterfaceExpr decodeExpr
  LF2.ExprSumCallInterface LF2.Expr_CallInterface {..} -> ECallInterface
    <$> mayDecode "expr_CallInterfaceInterfaceType" expr_CallInterfaceInterfaceType decodeTypeConName
    <*> decodeMethodName expr_CallInterfaceMethodInternedName
    <*> mayDecode "expr_CallInterfaceInterfaceExpr" expr_CallInterfaceInterfaceExpr decodeExpr
  LF2.ExprSumToRequiredInterface LF2.Expr_ToRequiredInterface {..} -> EToRequiredInterface
    <$> mayDecode "expr_ToRequiredInterfaceRequiredInterface" expr_ToRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_ToRequiredInterfaceRequiringInterface" expr_ToRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_ToRequiredInterfaceExpr" expr_ToRequiredInterfaceExpr decodeExpr
  LF2.ExprSumFromRequiredInterface LF2.Expr_FromRequiredInterface {..} -> EFromRequiredInterface
    <$> mayDecode "expr_FromRequiredInterfaceRequiredInterface" expr_FromRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_FromRequiredInterfaceRequiringInterface" expr_FromRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_FromRequiredInterfaceExpr" expr_FromRequiredInterfaceExpr decodeExpr
  LF2.ExprSumUnsafeFromRequiredInterface LF2.Expr_UnsafeFromRequiredInterface {..} -> EUnsafeFromRequiredInterface
    <$> mayDecode "expr_UnsafeFromRequiredInterfaceRequiredInterface" expr_UnsafeFromRequiredInterfaceRequiredInterface decodeTypeConName
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceRequiringInterface" expr_UnsafeFromRequiredInterfaceRequiringInterface decodeTypeConName
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceContractIdExpr" expr_UnsafeFromRequiredInterfaceContractIdExpr decodeExpr
    <*> mayDecode "expr_UnsafeFromRequiredInterfaceInterfaceExpr" expr_UnsafeFromRequiredInterfaceInterfaceExpr decodeExpr
  LF2.ExprSumInterfaceTemplateTypeRep LF2.Expr_InterfaceTemplateTypeRep {..} -> EInterfaceTemplateTypeRep
    <$> mayDecode "expr_InterfaceTemplateTypeRepInterface" expr_InterfaceTemplateTypeRepInterface decodeTypeConName
    <*> mayDecode "expr_InterfaceTemplateTypeRepExpr" expr_InterfaceTemplateTypeRepExpr decodeExpr
  LF2.ExprSumSignatoryInterface LF2.Expr_SignatoryInterface {..} -> ESignatoryInterface
    <$> mayDecode "expr_SignatoryInterfaceInterface" expr_SignatoryInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_SignatoryInterfaceExpr" expr_SignatoryInterfaceExpr decodeExpr
  LF2.ExprSumObserverInterface LF2.Expr_ObserverInterface {..} -> EObserverInterface
    <$> mayDecode "expr_ObserverInterfaceInterface" expr_ObserverInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_ObserverInterfaceExpr" expr_ObserverInterfaceExpr decodeExpr
  LF2.ExprSumViewInterface LF2.Expr_ViewInterface {..} -> EViewInterface
    <$> mayDecode "expr_ViewInterfaceInterface" expr_ViewInterfaceInterface decodeTypeConName
    <*> mayDecode "expr_ViewInterfaceExpr" expr_ViewInterfaceExpr decodeExpr
  LF2.ExprSumChoiceController LF2.Expr_ChoiceController {..} -> EChoiceController
    <$> mayDecode "expr_ChoiceControllerTemplate" expr_ChoiceControllerTemplate decodeTypeConName
    <*> decodeNameId ChoiceName expr_ChoiceControllerChoiceInternedStr
    <*> mayDecode "expr_ChoiceControllerContractExpr" expr_ChoiceControllerContractExpr decodeExpr
    <*> mayDecode "expr_ChoiceControllerChoiceArgExpr" expr_ChoiceControllerChoiceArgExpr decodeExpr
  LF2.ExprSumChoiceObserver LF2.Expr_ChoiceObserver {..} -> EChoiceObserver
    <$> mayDecode "expr_ChoiceObserverTemplate" expr_ChoiceObserverTemplate decodeTypeConName
    <*> decodeNameId ChoiceName expr_ChoiceObserverChoiceInternedStr
    <*> mayDecode "expr_ChoiceObserverContractExpr" expr_ChoiceObserverContractExpr decodeExpr
    <*> mayDecode "expr_ChoiceObserverChoiceArgExpr" expr_ChoiceObserverChoiceArgExpr decodeExpr
  LF2.ExprSumExperimental (LF2.Expr_Experimental name mbType) -> do
    ty <- mayDecode "expr_Experimental" mbType decodeType
    pure $ EExperimental (decodeString name) ty

decodeUpdate :: LF2.Update -> Decode Expr
decodeUpdate LF2.Update{..} = mayDecode "updateSum" updateSum $ \case
  LF2.UpdateSumPure (LF2.Pure mbType mbExpr) ->
    fmap EUpdate $ UPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF2.UpdateSumBlock (LF2.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EUpdate $ UBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF2.UpdateSumCreate (LF2.Update_Create mbTycon mbExpr) ->
    fmap EUpdate $ UCreate
      <$> mayDecode "update_CreateTemplate" mbTycon decodeTypeConName
      <*> mayDecode "update_CreateExpr" mbExpr decodeExpr
  LF2.UpdateSumCreateInterface (LF2.Update_CreateInterface mbTycon mbExpr) ->
    fmap EUpdate $ UCreateInterface
      <$> mayDecode "update_CreateInterfaceInterface" mbTycon decodeTypeConName
      <*> mayDecode "update_CreateInterfaceExpr" mbExpr decodeExpr
  LF2.UpdateSumExercise LF2.Update_Exercise{..} ->
    fmap EUpdate $ UExercise
      <$> mayDecode "update_ExerciseTemplate" update_ExerciseTemplate decodeTypeConName
      <*> decodeNameId ChoiceName update_ExerciseChoiceInternedStr
      <*> mayDecode "update_ExerciseCid" update_ExerciseCid decodeExpr
      <*> mayDecode "update_ExerciseArg" update_ExerciseArg decodeExpr
  LF2.UpdateSumSoftExercise LF2.Update_SoftExercise{} ->
    throwError (ParseError "Update.Sum.soft_exercise is no longer supported")
  LF2.UpdateSumDynamicExercise LF2.Update_DynamicExercise{..} ->
    fmap EUpdate $ UDynamicExercise
      <$> mayDecode "update_DynamicExerciseTemplate" update_DynamicExerciseTemplate decodeTypeConName
      <*> decodeNameId ChoiceName update_DynamicExerciseChoiceInternedStr
      <*> mayDecode "update_DynamicExerciseCid" update_DynamicExerciseCid decodeExpr
      <*> mayDecode "update_DynamicExerciseArg" update_DynamicExerciseArg decodeExpr
  LF2.UpdateSumExerciseInterface LF2.Update_ExerciseInterface{..} ->
    fmap EUpdate $ UExerciseInterface
      <$> mayDecode "update_ExerciseInterfaceInterface" update_ExerciseInterfaceInterface decodeTypeConName
      <*> decodeNameId ChoiceName update_ExerciseInterfaceChoiceInternedStr
      <*> mayDecode "update_ExerciseInterfaceCid" update_ExerciseInterfaceCid decodeExpr
      <*> mayDecode "update_ExerciseInterfaceArg" update_ExerciseInterfaceArg decodeExpr
      <*> traverse decodeExpr update_ExerciseInterfaceGuard
  LF2.UpdateSumExerciseByKey LF2.Update_ExerciseByKey{..} ->
    fmap EUpdate $ UExerciseByKey
      <$> mayDecode "update_ExerciseByKeyTemplate" update_ExerciseByKeyTemplate decodeTypeConName
      <*> decodeNameId ChoiceName update_ExerciseByKeyChoiceInternedStr
      <*> mayDecode "update_ExerciseByKeyKey" update_ExerciseByKeyKey decodeExpr
      <*> mayDecode "update_ExerciseByKeyArg" update_ExerciseByKeyArg decodeExpr
  LF2.UpdateSumFetch LF2.Update_Fetch{..} ->
    fmap EUpdate $ UFetch
      <$> mayDecode "update_FetchTemplate" update_FetchTemplate decodeTypeConName
      <*> mayDecode "update_FetchCid" update_FetchCid decodeExpr
  LF2.UpdateSumSoftFetch LF2.Update_SoftFetch{} ->
    throwError (ParseError "Update.Sum.soft_fetch is no longer supported")
  LF2.UpdateSumFetchInterface LF2.Update_FetchInterface{..} ->
    fmap EUpdate $ UFetchInterface
      <$> mayDecode "update_FetchInterfaceInterface" update_FetchInterfaceInterface decodeTypeConName
      <*> mayDecode "update_FetchInterfaceCid" update_FetchInterfaceCid decodeExpr
  LF2.UpdateSumGetTime LF2.Unit ->
    pure (EUpdate UGetTime)
  LF2.UpdateSumEmbedExpr LF2.Update_EmbedExpr{..} ->
    fmap EUpdate $ UEmbedExpr
      <$> mayDecode "update_EmbedExprType" update_EmbedExprType decodeType
      <*> mayDecode "update_EmbedExprBody" update_EmbedExprBody decodeExpr
  LF2.UpdateSumLookupByKey retrieveByKey ->
    fmap (EUpdate . ULookupByKey) (decodeRetrieveByKey retrieveByKey)
  LF2.UpdateSumFetchByKey retrieveByKey ->
    fmap (EUpdate . UFetchByKey) (decodeRetrieveByKey retrieveByKey)
  LF2.UpdateSumTryCatch LF2.Update_TryCatch{..} ->
    fmap EUpdate $ UTryCatch
      <$> mayDecode "update_TryCatchReturnType" update_TryCatchReturnType decodeType
      <*> mayDecode "update_TryCatchTryExpr" update_TryCatchTryExpr decodeExpr
      <*> decodeNameId ExprVarName update_TryCatchVarInternedStr
      <*> mayDecode "update_TryCatchCatchExpr" update_TryCatchCatchExpr decodeExpr

decodeRetrieveByKey :: LF2.Update_RetrieveByKey -> Decode RetrieveByKey
decodeRetrieveByKey LF2.Update_RetrieveByKey{..} = RetrieveByKey
  <$> mayDecode "update_RetrieveByKeyTemplate" update_RetrieveByKeyTemplate decodeTypeConName
  <*> mayDecode "update_RetrieveByKeyKey" update_RetrieveByKeyKey decodeExpr

decodeScenario :: LF2.Scenario -> Decode Expr
decodeScenario LF2.Scenario{..} = mayDecode "scenarioSum" scenarioSum $ \case
  LF2.ScenarioSumPure (LF2.Pure mbType mbExpr) ->
    fmap EScenario $ SPure
      <$> mayDecode "pureType" mbType decodeType
      <*> mayDecode "pureExpr" mbExpr decodeExpr
  LF2.ScenarioSumBlock (LF2.Block binds mbBody) -> do
    body <- mayDecode "blockBody" mbBody decodeExpr
    foldr (\b e -> EScenario $ SBind b e) body <$> mapM decodeBinding (V.toList binds)
  LF2.ScenarioSumCommit LF2.Scenario_Commit{..} ->
    fmap EScenario $ SCommit
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF2.ScenarioSumMustFailAt LF2.Scenario_Commit{..} ->
    fmap EScenario $ SMustFailAt
      <$> mayDecode "scenario_CommitRetType" scenario_CommitRetType decodeType
      <*> mayDecode "scenario_CommitParty" scenario_CommitParty decodeExpr
      <*> mayDecode "scenario_CommitExpr" scenario_CommitExpr decodeExpr
  LF2.ScenarioSumPass delta ->
    EScenario . SPass <$> decodeExpr delta
  LF2.ScenarioSumGetTime LF2.Unit ->
    pure (EScenario SGetTime)
  LF2.ScenarioSumGetParty name ->
    EScenario . SGetParty <$> decodeExpr name
  LF2.ScenarioSumEmbedExpr LF2.Scenario_EmbedExpr{..} ->
    fmap EScenario $ SEmbedExpr
      <$> mayDecode "scenario_EmbedExprType" scenario_EmbedExprType decodeType
      <*> mayDecode "scenario_EmbedExprBody" scenario_EmbedExprBody decodeExpr

decodeCaseAlt :: LF2.CaseAlt -> Decode CaseAlternative
decodeCaseAlt LF2.CaseAlt{..} = do
  pat <- mayDecode "caseAltSum" caseAltSum $ \case
    LF2.CaseAltSumDefault LF2.Unit -> pure CPDefault
    LF2.CaseAltSumVariant LF2.CaseAlt_Variant{..} ->
      CPVariant
        <$> mayDecode "caseAlt_VariantCon" caseAlt_VariantCon decodeTypeConName
        <*> decodeNameId VariantConName caseAlt_VariantVariantInternedStr
        <*> decodeNameId ExprVarName caseAlt_VariantBinderInternedStr
    LF2.CaseAltSumEnum LF2.CaseAlt_Enum{..} ->
      CPEnum
        <$> mayDecode "caseAlt_DataCon" caseAlt_EnumCon decodeTypeConName
        <*> decodeNameId VariantConName caseAlt_EnumConstructorInternedStr
    LF2.CaseAltSumBuiltinCon (Proto.Enumerated (Right pcon)) -> pure $ case pcon of
      LF2.BuiltinConCON_UNIT -> CPUnit
      LF2.BuiltinConCON_TRUE -> CPBool True
      LF2.BuiltinConCON_FALSE -> CPBool False
    LF2.CaseAltSumBuiltinCon (Proto.Enumerated (Left idx)) ->
      throwError (UnknownEnum "CaseAltSumBuiltinCon" idx)
    LF2.CaseAltSumNil LF2.Unit -> pure CPNil
    LF2.CaseAltSumCons LF2.CaseAlt_Cons{..} ->
      CPCons <$> decodeNameId ExprVarName caseAlt_ConsVarHeadInternedStr <*> decodeNameId ExprVarName caseAlt_ConsVarTailInternedStr
    LF2.CaseAltSumOptionalNone LF2.Unit -> pure CPNone
    LF2.CaseAltSumOptionalSome LF2.CaseAlt_OptionalSome{..} ->
      CPSome <$> decodeNameId ExprVarName caseAlt_OptionalSomeVarBodyInternedStr
  body <- mayDecode "caseAltBody" caseAltBody decodeExpr
  pure $ CaseAlternative pat body

decodeBinding :: LF2.Binding -> Decode Binding
decodeBinding (LF2.Binding mbBinder mbBound) =
  Binding
    <$> mayDecode "bindingBinder" mbBinder decodeVarWithType
    <*> mayDecode "bindingBound" mbBound decodeExpr

decodeTypeVarWithKind :: LF2.TypeVarWithKind -> Decode (TypeVarName, Kind)
decodeTypeVarWithKind LF2.TypeVarWithKind{..} =
  (,)
    <$> decodeNameId TypeVarName typeVarWithKindVarInternedStr
    <*> mayDecode "typeVarWithKindKind" typeVarWithKindKind decodeKind

decodeVarWithType :: LF2.VarWithType -> Decode (ExprVarName, Type)
decodeVarWithType LF2.VarWithType{..} =
  (,)
    <$> decodeNameId ExprVarName varWithTypeVarInternedStr
    <*> mayDecode "varWithTypeType" varWithTypeType decodeType

decodeBuiltinLit :: LF2.BuiltinLit -> Decode BuiltinExpr
decodeBuiltinLit (LF2.BuiltinLit mbSum) = mayDecode "builtinLitSum" mbSum $ \case
  LF2.BuiltinLitSumInt64 sInt -> pure $ BEInt64 sInt
  LF2.BuiltinLitSumNumericInternedStr strId -> lookupString strId >>= decodeNumericLit . fst
  LF2.BuiltinLitSumTimestamp sTime -> pure $ BETimestamp sTime
  LF2.BuiltinLitSumTextInternedStr strId ->  BEText . fst <$> lookupString strId
  LF2.BuiltinLitSumDate days -> pure $ BEDate days
  LF2.BuiltinLitSumRoundingMode enum -> case enum of
    Proto.Enumerated (Right mode) -> pure $ case mode of
       LF2.BuiltinLit_RoundingModeUP -> BERoundingMode LitRoundingUp
       LF2.BuiltinLit_RoundingModeDOWN -> BERoundingMode LitRoundingDown
       LF2.BuiltinLit_RoundingModeCEILING -> BERoundingMode LitRoundingCeiling
       LF2.BuiltinLit_RoundingModeFLOOR -> BERoundingMode LitRoundingFloor
       LF2.BuiltinLit_RoundingModeHALF_UP -> BERoundingMode LitRoundingHalfUp
       LF2.BuiltinLit_RoundingModeHALF_DOWN -> BERoundingMode LitRoundingHalfDown
       LF2.BuiltinLit_RoundingModeHALF_EVEN -> BERoundingMode LitRoundingHalfEven
       LF2.BuiltinLit_RoundingModeUNNECESSARY -> BERoundingMode LitRoundingUnnecessary
    Proto.Enumerated (Left idx) -> throwError (UnknownEnum "BuiltinLitSumRoundingMode" idx)

decodeNumericLit :: T.Text -> Decode BuiltinExpr
decodeNumericLit (T.unpack -> str) = case readMaybe str of
    Nothing -> throwError $ ParseError $ "bad Numeric literal: " ++ show str
    Just n -> pure $ BENumeric n


decodeKind :: LF2.Kind -> Decode Kind
decodeKind LF2.Kind{..} = mayDecode "kindSum" kindSum $ \case
  LF2.KindSumStar LF2.Unit -> pure KStar
  LF2.KindSumNat LF2.Unit -> pure KNat
  LF2.KindSumArrow (LF2.Kind_Arrow params mbResult) -> do
    result <- mayDecode "kind_ArrowResult" mbResult decodeKind
    foldr KArrow result <$> traverse decodeKind (V.toList params)

decodeBuiltin :: LF2.BuiltinType -> Decode BuiltinType
decodeBuiltin = \case
  LF2.BuiltinTypeINT64 -> pure BTInt64
  LF2.BuiltinTypeNUMERIC -> pure BTNumeric
  LF2.BuiltinTypeTEXT    -> pure BTText
  LF2.BuiltinTypeTIMESTAMP -> pure BTTimestamp
  LF2.BuiltinTypePARTY   -> pure BTParty
  LF2.BuiltinTypeUNIT    -> pure BTUnit
  LF2.BuiltinTypeBOOL    -> pure BTBool
  LF2.BuiltinTypeLIST    -> pure BTList
  LF2.BuiltinTypeUPDATE  -> pure BTUpdate
  LF2.BuiltinTypeSCENARIO -> pure BTScenario
  LF2.BuiltinTypeDATE -> pure BTDate
  LF2.BuiltinTypeCONTRACT_ID -> pure BTContractId
  LF2.BuiltinTypeOPTIONAL -> pure BTOptional
  LF2.BuiltinTypeTEXTMAP -> pure BTTextMap
  LF2.BuiltinTypeGENMAP -> pure BTGenMap
  LF2.BuiltinTypeARROW -> pure BTArrow
  LF2.BuiltinTypeANY -> pure BTAny
  LF2.BuiltinTypeTYPE_REP -> pure BTTypeRep
  LF2.BuiltinTypeROUNDING_MODE -> pure BTRoundingMode
  LF2.BuiltinTypeBIGNUMERIC -> pure BTBigNumeric
  LF2.BuiltinTypeANY_EXCEPTION -> pure BTAnyException

decodeTypeLevelNat :: Integer -> Decode TypeLevelNat
decodeTypeLevelNat m =
    case typeLevelNatE m of
        Left TLNEOutOfBounds ->
            throwError $ ParseError $ "bad type-level nat: " <> show m <> " is out of bounds"
        Right n ->
            pure n

decodeType :: LF2.Type -> Decode Type
decodeType LF2.Type{..} = mayDecode "typeSum" typeSum $ \case
  LF2.TypeSumVar (LF2.Type_Var var args) ->
    decodeWithArgs args $ TVar <$> decodeNameId TypeVarName var
  LF2.TypeSumNat n -> TNat <$> decodeTypeLevelNat (fromIntegral n)
  LF2.TypeSumCon (LF2.Type_Con mbCon args) ->
    decodeWithArgs args $ TCon <$> mayDecode "type_ConTycon" mbCon decodeTypeConName
  LF2.TypeSumSyn (LF2.Type_Syn mbSyn args) ->
    TSynApp <$> mayDecode "type_SynTysyn" mbSyn decodeTypeSynName <*> traverse decodeType (V.toList args)
  LF2.TypeSumBuiltin (LF2.Type_Builtin (Proto.Enumerated (Right prim)) args) -> do
    decodeWithArgs args $ TBuiltin <$> decodeBuiltin prim
  LF2.TypeSumBuiltin (LF2.Type_Builtin (Proto.Enumerated (Left idx)) _args) ->
    throwError (UnknownEnum "Builtin" idx)
  LF2.TypeSumForall (LF2.Type_Forall binders mbBody) -> do
    body <- mayDecode "type_ForAllBody" mbBody decodeType
    foldr TForall body <$> traverse decodeTypeVarWithKind (V.toList binders)
  LF2.TypeSumStruct (LF2.Type_Struct flds) ->
    TStruct <$> mapM (decodeFieldWithType FieldName) (V.toList flds)
  LF2.TypeSumInterned n -> do
    DecodeEnv{internedTypes} <- ask
    lookupInterned internedTypes BadTypeId n
  where
    decodeWithArgs :: V.Vector LF2.Type -> Decode Type -> Decode Type
    decodeWithArgs args fun = foldl' TApp <$> fun <*> traverse decodeType args


decodeFieldWithType :: (T.Text -> a) -> LF2.FieldWithType -> Decode (a, Type)
decodeFieldWithType wrapName (LF2.FieldWithType name mbType) =
  (,)
    <$> decodeNameId wrapName name
    <*> mayDecode "fieldWithTypeType" mbType decodeType

decodeFieldWithExpr :: LF2.FieldWithExpr -> Decode (FieldName, Expr)
decodeFieldWithExpr (LF2.FieldWithExpr name mbExpr) =
  (,)
    <$> decodeNameId FieldName name
    <*> mayDecode "fieldWithExprExpr" mbExpr decodeExpr

decodeTypeConApp :: LF2.Type_Con -> Decode TypeConApp
decodeTypeConApp LF2.Type_Con{..} =
  TypeConApp
    <$> mayDecode "typeConAppTycon" type_ConTycon decodeTypeConName
    <*> mapM decodeType (V.toList type_ConArgs)

decodeTypeSynName :: LF2.TypeSynName -> Decode (Qualified TypeSynName)
decodeTypeSynName LF2.TypeSynName{..} = do
  (pref, mname) <- mayDecode "typeSynNameModule" typeSynNameModule decodeModuleRef
  syn <- decodeDottedName TypeSynName typeSynNameNameInternedDname
  pure $ Qualified pref mname syn

decodeTypeConName :: LF2.TypeConName -> Decode (Qualified TypeConName)
decodeTypeConName LF2.TypeConName{..} = do
  (pref, mname) <- mayDecode "typeConNameModule" typeConNameModule decodeModuleRef
  con <- decodeDottedName TypeConName typeConNameNameInternedDname
  pure $ Qualified pref mname con

decodeModuleRef :: LF2.ModuleRef -> Decode (PackageRef, ModuleName)
decodeModuleRef LF2.ModuleRef{..} =
  (,)
    <$> mayDecode "moduleRefPackageRef" moduleRefPackageRef decodePackageRef
    <*> decodeDottedName ModuleName moduleRefModuleNameInternedDname

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
