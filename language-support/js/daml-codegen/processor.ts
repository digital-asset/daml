// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as ts from 'typescript';
import { lf } from 'daml-grpc';
import { ModuleTree, treeify } from './module_tree';

export class DalfProcessor {
    private readonly src: ts.SourceFile;
    private readonly printer: ts.Printer;
    constructor(outputPath: string, printer: ts.Printer) {
        this.src = ts.createSourceFile(outputPath, '', ts.ScriptTarget.ES2015);
        this.printer = printer;
    }

    process(dalf: Buffer): ts.SourceFile {
        const payload = lf.Archive.deserializeBinary(dalf).getPayload_asU8();
        const modules = lf.ArchivePayload.deserializeBinary(payload).getDamlLf1().getModulesList();
        this.src.text += generate(treeify(modules)).map(decl => this.printer.printNode(ts.EmitHint.Unspecified, decl, this.src)).join('\n\n');
        return this.src;
    }

}

function generate(tree: ModuleTree): ts.ModuleDeclaration[] {
    const declarations: ts.ModuleDeclaration[] = [];
    for (const lfModuleName in tree.children) {
        const child = tree.children[lfModuleName];
        const tsModuleName = ts.createIdentifier(lfModuleName);
        declarations.push(ts.createModuleDeclaration([], [], tsModuleName, ts.createModuleBlock(generate(child))));
    }
    for (const lfModule of tree.modules) {
        const lfModuleName = lfModule.getName().getSegmentsList();
        const tsModuleName = ts.createIdentifier(lfModuleName[lfModuleName.length - 1]);
        const templates = lfModule.getTemplatesList().map(t => generateTemplate(t));
        const dataTypes = lfModule.getDataTypesList().map(t => generateDataType(t));
        declarations.push(ts.createModuleDeclaration([], [], tsModuleName, ts.createModuleBlock([...templates, ...dataTypes])));
    }
    return declarations;
}

function generateTemplate(template: lf.v1.DefTemplate): ts.ClassDeclaration {
    const templateNameSegments = template.getTycon().getSegmentsList();
    const templateName = templateNameSegments[templateNameSegments.length - 1];
    const choices = template.getChoicesList().map(c => ts.createMethod([], [], undefined, c.getName(), undefined, [], [], undefined, undefined));
    return ts.createClassDeclaration([], [], templateName, [], [], choices);
}

function generateDataType(dataType: lf.v1.DefDataType): ts.ClassDeclaration {
    const typeNameSegments = dataType.getName().getSegmentsList();
    const typeName = typeNameSegments[typeNameSegments.length - 1];
    const members: ts.ClassElement[] = [];
    if (dataType.hasRecord()) {
        const fields = dataType.getRecord().getFieldsList();
        for (const field of fields) {
            const lfType = field.getType();
            const tsType = generateType(lfType);
            members.push(ts.createProperty([], [], field.getField(), undefined, tsType, undefined));
        }
    }
    return ts.createClassDeclaration([], [], typeName, [], [], members);
}

function generateType(lfType: lf.v1.Type): ts.TypeNode {
    if (lfType.hasPrim()) {
        switch (lfType.getPrim().getPrim()) {
            case lf.v1.PrimType.UNIT:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.BOOL:
                return ts.createKeywordTypeNode(ts.SyntaxKind.BooleanKeyword);
                break;
            case lf.v1.PrimType.INT64:
                return ts.createKeywordTypeNode(ts.SyntaxKind.NumberKeyword);
                break;
            case lf.v1.PrimType.DECIMAL:
                return ts.createKeywordTypeNode(ts.SyntaxKind.StringKeyword);
                break;
            case lf.v1.PrimType.TEXT:
                return ts.createKeywordTypeNode(ts.SyntaxKind.StringKeyword);
                break;
            case lf.v1.PrimType.TIMESTAMP:
                return ts.createTypeReferenceNode('Date', []);
                break;
            case lf.v1.PrimType.PARTY:
                return ts.createKeywordTypeNode(ts.SyntaxKind.StringKeyword);
                break;
            case lf.v1.PrimType.LIST:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.UPDATE:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.SCENARIO:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.DATE:
                return ts.createTypeReferenceNode('Date', []);
                break;
            case lf.v1.PrimType.CONTRACT_ID:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.OPTIONAL:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            case lf.v1.PrimType.ARROW:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
            default:
                return ts.createTypeReferenceNode('MISSING', []);
                throw new Error(`unsupported type ${lfType}`);
                break;
        }
    } else if (lfType.hasCon()) {
        return ts.createTypeReferenceNode('MISSING', []);
        throw new Error(`unsupported type ${lfType}`);
    } else if (lfType.hasTuple()) {
        return ts.createTypeReferenceNode('MISSING', []);
        throw new Error(`unsupported type ${lfType}`);
    } else {
        return ts.createTypeReferenceNode('MISSING', []);
        throw new Error(`unsupported type ${lfType}`);
    }
}