-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Encoding of the LF package into LF version 1 format.
module DA.Daml.LF.Proto3.EncodeV1
  ( encodeModule
  , encodePackage
  ) where

import           Control.Lens ((^.), matching)
import           Control.Lens.Ast (rightSpine)

import qualified Data.NameMap as NM
import qualified Data.Text           as T
import qualified Data.Text.Lazy      as TL
import qualified Data.Vector         as V
import qualified Data.Scientific     as Scientific
import           GHC.Stack (HasCallStack)

import           DA.Prelude
import           DA.Pretty
import qualified DA.Daml.LF.Decimal  as Decimal
import           DA.Daml.LF.Ast
import           DA.Daml.LF.Mangling
import qualified Da.DamlLf1 as P

import qualified Proto3.Suite as P (Enumerated (..))

-- | Encoding 'from' to type 'to'
class Encode from to | from -> to where
  encode :: Version -> from -> to

------------------------------------------------------------------------
-- Simple encodings
------------------------------------------------------------------------

encodeV :: Encode a b => Version -> [a] -> V.Vector b
encodeV version = V.fromList . map (encode version)

encodeNM :: (NM.Named a, Encode a b) => Version -> NM.NameMap a -> V.Vector b
encodeNM version = V.fromList . map (encode version) . NM.toList

encodeE :: Encode a b => Version -> a -> P.Enumerated b
encodeE version = P.Enumerated . Right . encode version

encode' :: Encode a b => Version -> a -> Maybe b
encode' version = Just . encode version

instance Encode PackageId TL.Text where
   encode _version = TL.fromStrict . unTagged

instance Encode PartyLiteral TL.Text where
   encode _version = TL.fromStrict . unTagged

instance Encode FieldName TL.Text where
   encode version = encodeIdentifier version . unTagged

instance Encode VariantConName TL.Text where
   encode version = encodeIdentifier version . unTagged

instance Encode TypeVarName TL.Text where
   encode version = encodeIdentifier version . unTagged

instance Encode ExprVarName TL.Text where
   encode version = encodeIdentifier version . unTagged

instance Encode ChoiceName TL.Text where
   encode version = encodeIdentifier version . unTagged

encodeIdentifier :: Version -> T.Text -> TL.Text
encodeIdentifier _version unmangled = case mangleIdentifier unmangled of
   Left err -> error ("IMPOSSIBLE: could not mangle name " ++ show unmangled ++ ": " ++ err)
   Right x -> TL.fromStrict x

instance Encode T.Text TL.Text where
  encode _version = TL.fromStrict

instance Encode Decimal.Decimal TL.Text where
  encode _version d = enc (Decimal.normalize d)
    where
      enc (Decimal.Decimal n k0) =
        TL.pack
          $ Scientific.formatScientific Scientific.Fixed Nothing
          $ Scientific.scientific n (fromIntegral (negate k0))

instance Encode (Qualified TypeConName) P.TypeConName where
  encode version (Qualified pref mname con) = P.TypeConName (Just $ P.ModuleRef (encode' version pref) (encode' version mname)) (encode' version con)

instance Encode (Qualified ExprValName) P.ValName where
  encode version (Qualified pref mname con) = P.ValName (Just $ P.ModuleRef (encode' version pref) (encode' version mname)) (encodeDefName version (unTagged con))

instance Encode SourceLoc P.Location where
  encode version SourceLoc{..} =
    P.Location
      (fmap
        (\(p, m) -> P.ModuleRef (encode' version p) (encode' version m))
        slocModuleRef)
      (Just (P.Location_Range
        (fromIntegral slocStartLine)
        (fromIntegral slocStartCol)
        (fromIntegral slocEndLine)
        (fromIntegral slocEndCol)))

instance Encode PackageRef P.PackageRef where
  encode version = \case
      PRSelf -> P.PackageRef (Just (P.PackageRefSumSelf P.Unit))
      PRImport pkgid -> P.PackageRef (Just (P.PackageRefSumPackageId $ encode version pkgid))

encodeDottedName :: Version -> [T.Text] -> P.DottedName
encodeDottedName _version unmangled = case mangled of
  Left (which, err) -> error ("IMPOSSIBLE: could not mangle name " ++ show which ++ ": " ++ err)
  Right xs -> P.DottedName (V.fromList xs)
  where
    mangled = forM unmangled $ \part -> case mangleIdentifier part of
      Left err -> Left (part, err)
      Right x -> Right (TL.fromStrict x)

instance Encode ModuleName P.DottedName where
  encode version = encodeDottedName version . unTagged

instance Encode TypeConName P.DottedName where
  encode version = encodeDottedName version . unTagged

instance Encode (FieldName, Type) P.FieldWithType where
  encode version (name, typ) = P.FieldWithType (encode version name) (encode' version typ)

instance Encode (FieldName, Expr) P.FieldWithExpr where
  encode version (name, expr) = P.FieldWithExpr (encode version name) (encode' version expr)

instance Encode (VariantConName, Type) P.FieldWithType where
  encode version (name, typ) = P.FieldWithType (encode version name) (encode' version typ)

instance Encode (TypeVarName, Kind) P.TypeVarWithKind where
  encode version (name, kind)  = P.TypeVarWithKind (encode version name) (encode' version kind)

instance Encode (ExprVarName, Type) P.VarWithType where
  encode version (name, typ) = P.VarWithType (encode version name) (encode' version typ)

------------------------------------------------------------------------
-- Encoding of kinds
------------------------------------------------------------------------

instance Encode Kind P.Kind where
  encode version = P.Kind . Just . \case
    KStar -> P.KindSumStar P.Unit
    k@KArrow{} ->
      let (params, result) = k ^. rightSpine _KArrow
      in P.KindSumArrow (P.Kind_Arrow (encodeV version params) (encode' version result))

------------------------------------------------------------------------
-- Encoding of types
------------------------------------------------------------------------

instance Encode BuiltinType P.PrimType where
  encode version = \case
    BTInt64 -> P.PrimTypeINT64
    BTDecimal -> P.PrimTypeDECIMAL
    BTText -> P.PrimTypeTEXT
    BTTimestamp -> P.PrimTypeTIMESTAMP
    BTParty -> P.PrimTypePARTY
    BTEnum et -> case et of
      ETUnit -> P.PrimTypeUNIT
      ETBool -> P.PrimTypeBOOL
    BTList -> P.PrimTypeLIST
    BTUpdate -> P.PrimTypeUPDATE
    BTScenario -> P.PrimTypeSCENARIO
    BTDate -> P.PrimTypeDATE
    BTContractId -> P.PrimTypeCONTRACT_ID
    BTOptional -> checkOptional version P.PrimTypeOPTIONAL
    BTMap -> checkTextMap version P.PrimTypeMAP
    BTArrow -> checkArrowType version P.PrimTypeARROW

instance Encode Type P.Type where
  encode version typ = P.Type . Just $
    case typ ^. _TApps of
      (TVar var, args) ->
        P.TypeSumVar (P.Type_Var (encode version var) (encodeV version args))
      (TCon con, args) ->
        P.TypeSumCon (P.Type_Con (encode' version con) (encodeV version args))
      (TBuiltin BTArrow, args)
        | not (supportsArrowType version) -> case args of
            [param, result] ->
              P.TypeSumFun (P.Type_Fun (encodeV version [param]) (encode' version result))
            _ -> error "TArrow must be used with exactly two arguments"
      (TBuiltin bltn, args) ->
        P.TypeSumPrim (P.Type_Prim (encodeE version bltn) (encodeV version args))
      (t@TForall{}, []) ->
          let (binders, body) = t ^. _TForalls
          in P.TypeSumForall (P.Type_Forall (encodeV version binders) (encode' version body))
      (TTuple flds, []) -> P.TypeSumTuple (P.Type_Tuple (encodeV version flds))

      (TApp{}, _) -> error "TApp after unwinding TApp"
      -- NOTE(MH): The following case is ill-kinded.
      (TTuple{}, _:_) -> error "Application of TTuple"
      -- NOTE(MH): The following case requires impredicative polymorphism,
      -- which we don't support.
      (TForall{}, _:_) -> error "Application of TForall"

------------------------------------------------------------------------
-- Encoding of expressions
------------------------------------------------------------------------

instance Encode EnumCon P.PrimCon where
  encode _version = \case
    ECUnit -> P.PrimConCON_UNIT
    ECFalse -> P.PrimConCON_FALSE
    ECTrue -> P.PrimConCON_TRUE

instance Encode TypeConApp P.Type_Con where
  encode version (TypeConApp tycon args) = P.Type_Con (encode' version tycon) (encodeV version args)

instance Encode BuiltinExpr P.ExprSum where
  encode version = \case
    BEInt64 x -> lit $ P.PrimLitSumInt64 x
    BEDecimal dec -> lit $ P.PrimLitSumDecimal (TL.pack (show dec))
    BEText x -> lit $ P.PrimLitSumText (TL.fromStrict x)
    BETimestamp x -> lit $ P.PrimLitSumTimestamp x
    BEParty x -> lit $ P.PrimLitSumParty (encode version x)
    BEDate x -> lit $ P.PrimLitSumDate x

    BEEnumCon con -> P.ExprSumPrimCon (encodeE version con)

    BEEqual typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionEQUAL_INT64
      BTDecimal -> builtin P.BuiltinFunctionEQUAL_DECIMAL
      BTText -> builtin P.BuiltinFunctionEQUAL_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionEQUAL_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionEQUAL_DATE
      BTParty -> builtin P.BuiltinFunctionEQUAL_PARTY
      BTEnum ETBool -> builtin P.BuiltinFunctionEQUAL_BOOL
      other -> error $ "BEEqual unexpected type " <> show other

    BELessEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionLEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionLEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLEQ_DATE
      BTParty | supportsPartyOrd version -> builtin P.BuiltinFunctionLEQ_PARTY
      other -> error $ "BELessEq unexpected type " <> show other

    BELess typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionLESS_INT64
      BTDecimal -> builtin P.BuiltinFunctionLESS_DECIMAL
      BTText -> builtin P.BuiltinFunctionLESS_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionLESS_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionLESS_DATE
      BTParty | supportsPartyOrd version -> builtin P.BuiltinFunctionLESS_PARTY
      other -> error $ "BELess unexpected type " <> show other

    BEGreaterEq typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGEQ_INT64
      BTDecimal -> builtin P.BuiltinFunctionGEQ_DECIMAL
      BTText -> builtin P.BuiltinFunctionGEQ_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGEQ_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGEQ_DATE
      BTParty | supportsPartyOrd version -> builtin P.BuiltinFunctionGEQ_PARTY
      other -> error $ "BEGreaterEq unexpected type " <> show other

    BEGreater typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionGREATER_INT64
      BTDecimal -> builtin P.BuiltinFunctionGREATER_DECIMAL
      BTText -> builtin P.BuiltinFunctionGREATER_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionGREATER_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionGREATER_DATE
      BTParty | supportsPartyOrd version -> builtin P.BuiltinFunctionGREATER_PARTY
      other -> error $ "BEGreater unexpected type " <> show other

    BEToText typ -> case typ of
      BTInt64 -> builtin P.BuiltinFunctionTO_TEXT_INT64
      BTDecimal -> builtin P.BuiltinFunctionTO_TEXT_DECIMAL
      BTText -> builtin P.BuiltinFunctionTO_TEXT_TEXT
      BTTimestamp -> builtin P.BuiltinFunctionTO_TEXT_TIMESTAMP
      BTDate -> builtin P.BuiltinFunctionTO_TEXT_DATE
      BTParty -> builtin P.BuiltinFunctionTO_TEXT_PARTY
      other -> error $ "BEToText unexpected type " <> show other
    BEPartyFromText -> builtin P.BuiltinFunctionFROM_TEXT_PARTY
    BEPartyToQuotedText -> builtin P.BuiltinFunctionTO_QUOTED_TEXT_PARTY

    BEAddDecimal -> builtin P.BuiltinFunctionADD_DECIMAL
    BESubDecimal -> builtin P.BuiltinFunctionSUB_DECIMAL
    BEMulDecimal -> builtin P.BuiltinFunctionMUL_DECIMAL
    BEDivDecimal -> builtin P.BuiltinFunctionDIV_DECIMAL
    BERoundDecimal -> builtin P.BuiltinFunctionROUND_DECIMAL

    BEAddInt64 -> builtin P.BuiltinFunctionADD_INT64
    BESubInt64 -> builtin P.BuiltinFunctionSUB_INT64
    BEMulInt64 -> builtin P.BuiltinFunctionMUL_INT64
    BEDivInt64 -> builtin P.BuiltinFunctionDIV_INT64
    BEModInt64 -> builtin P.BuiltinFunctionMOD_INT64
    BEExpInt64 -> builtin P.BuiltinFunctionEXP_INT64

    BEInt64ToDecimal -> builtin P.BuiltinFunctionINT64_TO_DECIMAL
    BEDecimalToInt64 -> builtin P.BuiltinFunctionDECIMAL_TO_INT64

    BEFoldl -> builtin P.BuiltinFunctionFOLDL
    BEFoldr -> builtin P.BuiltinFunctionFOLDR
    BEEqualList -> builtin P.BuiltinFunctionEQUAL_LIST
    BEExplodeText -> builtin P.BuiltinFunctionEXPLODE_TEXT
    BEAppendText -> builtin P.BuiltinFunctionAPPEND_TEXT
    BEImplodeText -> builtin P.BuiltinFunctionIMPLODE_TEXT
    BESha256Text -> builtin P.BuiltinFunctionSHA256_TEXT
    BEError -> builtin P.BuiltinFunctionERROR

    BEMapEmpty -> builtin P.BuiltinFunctionMAP_EMPTY
    BEMapInsert -> builtin P.BuiltinFunctionMAP_INSERT
    BEMapLookup -> builtin P.BuiltinFunctionMAP_LOOKUP
    BEMapDelete -> builtin P.BuiltinFunctionMAP_DELETE
    BEMapSize -> builtin P.BuiltinFunctionMAP_SIZE
    BEMapToList -> builtin P.BuiltinFunctionMAP_TO_LIST

    BETimestampToUnixMicroseconds -> builtin P.BuiltinFunctionTIMESTAMP_TO_UNIX_MICROSECONDS
    BEUnixMicrosecondsToTimestamp -> builtin P.BuiltinFunctionUNIX_MICROSECONDS_TO_TIMESTAMP

    BEDateToUnixDays -> builtin P.BuiltinFunctionDATE_TO_UNIX_DAYS
    BEUnixDaysToDate -> builtin P.BuiltinFunctionUNIX_DAYS_TO_DATE

    BETrace -> builtin P.BuiltinFunctionTRACE
    BEEqualContractId -> builtin P.BuiltinFunctionEQUAL_CONTRACT_ID

    where
      builtin = P.ExprSumBuiltin . P.Enumerated . Right
      lit = P.ExprSumPrimLit . P.PrimLit . Just

instance Encode Expr P.Expr where
  encode = encodeExpr

encodeExpr :: Version -> Expr -> P.Expr
encodeExpr version = \case
  EVar v -> expr $ P.ExprSumVar (encode version v)
  EVal v -> expr $ P.ExprSumVal (encode version v)
  EBuiltin bi -> expr $ encode version bi
  ERecCon{..} -> expr $ P.ExprSumRecCon $ P.Expr_RecCon (encode' version recTypeCon) (encodeV version recFields)
  ERecProj{..} -> expr $ P.ExprSumRecProj $ P.Expr_RecProj (encode' version recTypeCon) (encode version recField) (encode' version recExpr)
  ERecUpd{..} -> expr $ P.ExprSumRecUpd $ P.Expr_RecUpd (encode' version recTypeCon) (encode version recField) (encode' version recExpr) (encode' version recUpdate)
  EVariantCon{..} -> expr $ P.ExprSumVariantCon $ P.Expr_VariantCon (encode' version varTypeCon) (encode version varVariant) (encode' version varArg)
  ETupleCon{..} -> expr $ P.ExprSumTupleCon $ P.Expr_TupleCon (encodeV version tupFields)
  ETupleProj{..} -> expr $ P.ExprSumTupleProj $ P.Expr_TupleProj (encode version tupField) (encode' version tupExpr)
  ETupleUpd{..} -> expr $ P.ExprSumTupleUpd $ P.Expr_TupleUpd (encode version tupField) (encode' version tupExpr) (encode' version tupUpdate)
  e@ETmApp{} ->
      let (fun, args) = e ^. _ETmApps
      in expr $ P.ExprSumApp $ P.Expr_App (encode' version fun) (encodeV version args)
  e@ETyApp{} ->
      let (fun, args) = e ^. _ETyApps
      in expr $ P.ExprSumTyApp $ P.Expr_TyApp (encode' version fun) (encodeV version args)
  e@ETmLam{} ->
      let (params, body) = e ^. _ETmLams
      in expr $ P.ExprSumAbs $ P.Expr_Abs (encodeV version params) (encode' version body)
  e@ETyLam{} ->
      let (params, body) = e ^. _ETyLams
      in expr $ P.ExprSumTyAbs $ P.Expr_TyAbs (encodeV version params) (encode' version body)
  ECase{..} -> expr $ P.ExprSumCase $ P.Case (encode' version casScrutinee) (encodeV version casAlternatives)
  e@ELet{} ->
      let (lets, body) = e ^. _ELets
      in expr $ P.ExprSumLet $ P.Block (encodeV version lets) (encode' version body)
  ENil{..} -> expr $ P.ExprSumNil $ P.Expr_Nil (encode' version nilType)
  ECons{..} ->
      let unwind e0 as = case matching _ECons e0 of
            Left e1 -> (e1, as)
            Right (typ, hd, tl)
              | typ /= consType -> error "internal error: unexpected mismatch in cons cell type"
              | otherwise -> unwind tl (hd:as)
          (ctail, cfront) = unwind consTail [consHead]
      in expr $ P.ExprSumCons $ P.Expr_Cons (encode' version consType) (encodeV version $ reverse cfront) (encode' version ctail)
  EUpdate u -> expr $ P.ExprSumUpdate $ encode version u
  EScenario s -> expr $ P.ExprSumScenario $ encode version s
  ELocation loc e ->
    let (P.Expr _ esum) = encodeExpr version e
    in P.Expr (Just (encode version loc)) esum
  ENone typ -> checkOptional version $
    expr (P.ExprSumNone (P.Expr_None (encode' version typ)))
  ESome typ body -> checkOptional version $
    expr (P.ExprSumSome (P.Expr_Some (encode' version typ) (encode' version body)))
  where
    expr = P.Expr Nothing . Just

instance Encode Update P.Update where
  encode version = P.Update . Just . \case
    UPure{..} -> P.UpdateSumPure $ P.Pure (encode' version pureType) (encode' version pureExpr)
    e@UBind{} ->
      let (bindings, body) = EUpdate e ^. rightSpine (_EUpdate . _UBind)
      in P.UpdateSumBlock $ P.Block (encodeV version bindings) (encode' version body)
    UCreate{..} -> P.UpdateSumCreate $ P.Update_Create (encode' version creTemplate) (encode' version creArg)
    UExercise{..} -> P.UpdateSumExercise $ P.Update_Exercise (encode' version exeTemplate) (encode version exeChoice) (encode' version exeContractId) (encode' version exeActors) (encode' version exeArg)
    UFetch{..} -> P.UpdateSumFetch $ P.Update_Fetch (encode' version fetTemplate) (encode' version fetContractId)
    UGetTime -> P.UpdateSumGetTime P.Unit
    UEmbedExpr typ e -> P.UpdateSumEmbedExpr $ P.Update_EmbedExpr (encode' version typ) (encode' version e)
    UFetchByKey rbk -> checkContractKeys version $
       P.UpdateSumFetchByKey (encode version rbk)
    ULookupByKey rbk -> checkContractKeys version $
       P.UpdateSumLookupByKey (encode version rbk)

instance Encode RetrieveByKey P.Update_RetrieveByKey where
  encode version RetrieveByKey{..} = P.Update_RetrieveByKey
    (encode' version retrieveByKeyTemplate)
    (encode' version retrieveByKeyKey)

instance Encode Scenario P.Scenario where
  encode version = P.Scenario . Just . \case
    SPure{..} -> P.ScenarioSumPure $ P.Pure (encode' version spureType) (encode' version spureExpr)
    e@SBind{} ->
      let (bindings, body) = EScenario e ^. rightSpine (_EScenario . _SBind)
      in P.ScenarioSumBlock $ P.Block (encodeV version bindings) (encode' version body)
    SCommit{..} ->
      P.ScenarioSumCommit $ P.Scenario_Commit
        (encode' version scommitParty)
        (encode' version scommitExpr)
        (encode' version scommitType)
    SMustFailAt{..} ->
      P.ScenarioSumMustFailAt $ P.Scenario_Commit
        (encode' version smustFailAtParty)
        (encode' version smustFailAtExpr)
        (encode' version smustFailAtType)
    SPass{..} ->
      P.ScenarioSumPass (encode version spassDelta)
    SGetTime -> P.ScenarioSumGetTime P.Unit
    SGetParty{..} ->
      P.ScenarioSumGetParty (encode version sgetPartyName)
    SEmbedExpr typ e -> P.ScenarioSumEmbedExpr $ P.Scenario_EmbedExpr (encode' version typ) (encode' version e)

instance Encode Binding P.Binding where
  encode version (Binding binder bound) = P.Binding (encode' version binder) (encode' version bound)

instance Encode CaseAlternative P.CaseAlt where
  encode version CaseAlternative{..} =
    let pat = case altPattern of
          CPDefault     -> P.CaseAltSumDefault P.Unit
          CPVariant{..} -> P.CaseAltSumVariant $ P.CaseAlt_Variant (encode' version patTypeCon) (encode version patVariant) (encode version patBinder)
          CPEnumCon con -> P.CaseAltSumPrimCon (encodeE version con)
          CPNil         -> P.CaseAltSumNil P.Unit
          CPCons{..}    -> P.CaseAltSumCons $ P.CaseAlt_Cons (encode version patHeadBinder) (encode version patTailBinder)
          CPNone        -> checkOptional version $
            P.CaseAltSumNone P.Unit
          CPSome{..}    -> checkOptional version $
            P.CaseAltSumSome $ P.CaseAlt_Some (encode version patBodyBinder)
    in P.CaseAlt (Just pat) (encode' version altExpr)

instance Encode DefDataType P.DefDataType where
  encode version DefDataType{..} =
      P.DefDataType (encode' version dataTypeCon) (encodeV version dataParams)
      (Just $ case dataCons of
        DataRecord fs -> P.DefDataTypeDataConsRecord $ P.DefDataType_Fields (encodeV version fs)
        DataVariant fs -> P.DefDataTypeDataConsVariant $ P.DefDataType_Fields (encodeV version fs))
      (getIsSerializable dataSerializable)
      (encode version <$> dataLocation)

instance Encode DefValue P.DefValue where
  encode version DefValue{..} =
    P.DefValue
      (Just (P.DefValue_NameWithType (encodeDefName version (unTagged (fst dvalBinder))) (encode' version (snd dvalBinder))))
      (encode' version dvalBody)
      (getHasNoPartyLiterals dvalNoPartyLiterals)
      (getIsTest dvalIsTest)
      (encode version <$> dvalLocation)

instance Encode Template P.DefTemplate where
  encode version Template{..} =
    P.DefTemplate
    { P.defTemplateTycon = encode' version tplTypeCon
    , P.defTemplateParam = encode version tplParam
    , P.defTemplatePrecond = encode' version tplPrecondition
    , P.defTemplateSignatories = encode' version tplSignatories
    , P.defTemplateObservers = encode' version tplObservers
    , P.defTemplateAgreement = encode' version tplAgreement
    , P.defTemplateChoices = encodeNM version tplChoices
    , P.defTemplateLocation = encode version <$> tplLocation
    , P.defTemplateKey = fmap (encodeTemplateKey version tplParam) tplKey
    }

encodeTemplateKey :: Version -> ExprVarName -> TemplateKey -> P.DefTemplate_DefKey
encodeTemplateKey version templateVar TemplateKey{..} = checkContractKeys version $ P.DefTemplate_DefKey
  { P.defTemplate_DefKeyType = encode' version tplKeyType
  , P.defTemplate_DefKeyKey = case encodeKeyExpr version templateVar tplKeyBody of
      Left err -> error err
      Right x -> Just (P.KeyExpr (encode' version x))
  , P.defTemplate_DefKeyMaintainers = encode' version tplKeyMaintainers
  }


data KeyExpr =
    KeyExprProjections ![(TypeConApp, FieldName)]
  | KeyExprRecord !TypeConApp ![(FieldName, KeyExpr)]

instance Encode KeyExpr P.KeyExprSum where
  encode version = \case
    KeyExprProjections projs -> P.KeyExprSumProjections $ P.KeyExpr_Projections $ V.fromList $ do
      (tyCon, fld) <- projs
      return (P.KeyExpr_Projection (encode' version tyCon) (encode version fld))
    KeyExprRecord tyCon flds -> P.KeyExprSumRecord $ P.KeyExpr_Record (encode' version tyCon) $ V.fromList $ do
      (fldName, ke) <- flds
      return (P.KeyExpr_RecordField (encode version fldName) (Just (P.KeyExpr (encode' version ke))))

encodeKeyExpr ::  Version -> ExprVarName -> Expr -> Either String KeyExpr
encodeKeyExpr version tplParameter = \case
  ELocation _loc expr ->
    encodeKeyExpr version tplParameter expr
  EVar var -> if var == tplParameter
    then Right (KeyExprProjections [])
    else Left ("Expecting variable " ++ show tplParameter ++ " in key expression, got " ++ show var)
  ERecProj tyCon fld e -> do
    keyExpr <- encodeKeyExpr version tplParameter e
    case keyExpr of
      KeyExprProjections projs -> return (KeyExprProjections (projs ++ [(tyCon, fld)]))
      KeyExprRecord{} -> Left "Trying to project out of a record in key expression"
  ERecCon tyCon flds -> do
    keyFlds <- mapM (\(lbl, e) -> (lbl, ) <$> encodeKeyExpr version tplParameter e) flds
    return (KeyExprRecord tyCon keyFlds)
  e -> Left ("Bad key expression " ++ show e)

instance Encode TemplateChoice P.TemplateChoice where
  encode version TemplateChoice{..} =
    P.TemplateChoice
    { P.templateChoiceName = encode version chcName
    , P.templateChoiceConsuming = chcConsuming
    , P.templateChoiceControllers = encode' version chcControllers
    , P.templateChoiceSelfBinder = encode version chcSelfBinder
    , P.templateChoiceArgBinder = encode' version chcArgBinder
    , P.templateChoiceRetType = encode' version chcReturnType
    , P.templateChoiceUpdate = encode' version chcUpdate
    , P.templateChoiceLocation = encode version <$> chcLocation
    }

instance Encode FeatureFlags P.FeatureFlags where
  encode _version FeatureFlags{..} =  P.FeatureFlags
    { P.featureFlagsForbidPartyLiterals = forbidPartyLiterals
    -- We only support packages with these enabled -- see #157
    , P.featureFlagsDontDivulgeContractIdsInCreateArguments = True
    , P.featureFlagsDontDiscloseNonConsumingChoicesToObservers = True
    }


instance Encode Module P.Module where
  encode version Module{..} =
    P.Module (encode' version moduleName)
      (encode' version moduleFeatureFlags)
      (encodeNM version moduleDataTypes)
      (encodeNM version moduleValues)
      (encodeNM version moduleTemplates)

encodeModule :: Version -> Module -> P.Module
encodeModule = encode

-- | NOTE(MH): Assumes the DAML-LF version of the 'Package' is 'V1'.
encodePackage :: Package -> P.Package
encodePackage (Package version mods) = P.Package (encodeNM version mods)


-- | For now, value names are always encoded version using a single segment.
--
-- This is to keep backwards compat with older .dalf files, but also
-- because currently GenDALF generates weird names like `.` that we'd
-- have to handle separatedly. So for now, considering that we do not
-- use values in codegen, just mangle the entire thing.
encodeDefName :: (HasCallStack, Encode T.Text b) => Version -> T.Text -> V.Vector b
encodeDefName version txt = case mangleIdentifier txt of
  Left err -> error ("IMPOSSIBLE: could not mangle def name " ++ show txt ++ ": " ++ err)
  Right mangled -> encodeV version [mangled]

-- | NOTE(MH): This functions is used for sanity checking. The actual checks
-- are done in the conversion to DAML-LF.
checkFeature :: (Version -> Bool) -> String -> Version -> a -> a
checkFeature hasFeature name version x
    | hasFeature version = x
    | otherwise = error (error "DAML-LF " ++ renderPretty version ++ " cannot encode feature: " ++ name)

checkOptional :: Version -> a -> a
checkOptional = checkFeature supportsOptional "Optional"

checkArrowType :: Version -> a -> a
checkArrowType = checkFeature supportsArrowType "Partial application of (->)"

checkContractKeys :: Version -> a -> a
checkContractKeys = checkFeature supportsContractKeys "Contract keys"

checkTextMap :: Version -> a -> a
checkTextMap = checkFeature supportsTextMap "TextMap"
