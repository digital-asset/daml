// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as fs from 'fs';
import * as path from 'path';

interface OptionsBuffer {
    inputDalf?: string
    outputTs?: string
}

export interface Options {
    inputDalf: string
    outputTs: string
}

export enum OptionsReadError {
    PARSE_MISSING_INPUT,
    PARSE_MISSING_OUTPUT,
    PARSE_UNRECOGNIZED,
    READ_INPUT_IS_MISSING,
    READ_INPUT_DOES_NOT_EXIST,
    READ_OUTPUT_DIRECTORY_DOES_NOT_EXIST
}

function parse(argv: string[]): OptionsBuffer | OptionsReadError {
    const buffer: OptionsBuffer = {};
    for (let i = 0; i < argv.length; i++) {
        switch (argv[i]) {
            case '-i':
                if (i + 1 >= argv.length) {
                    return OptionsReadError.PARSE_MISSING_INPUT;
                }
                buffer.inputDalf = argv[++i];
                break;
            case '-o':
                if (i + 1 >= argv.length) {
                    return OptionsReadError.PARSE_MISSING_OUTPUT;
                }
                buffer.outputTs = argv[++i];
                break;
            default:
                return OptionsReadError.PARSE_UNRECOGNIZED;
                break;
        }
    }
    return buffer;
}

function interpret(buffer: OptionsBuffer): Options | OptionsReadError {
    if (!buffer.inputDalf) {
        return OptionsReadError.READ_INPUT_IS_MISSING;
    } else if (!fs.existsSync(buffer.inputDalf)) {
        return OptionsReadError.READ_INPUT_DOES_NOT_EXIST;
    } else if (!buffer.outputTs) {
        const { root, dir, name } = path.parse(buffer.inputDalf)
        buffer.outputTs = path.resolve(root, dir, `${name}.ts`);
        return {
            inputDalf: buffer.inputDalf,
            outputTs: buffer.outputTs
        };
    } else if (!fs.statSync(path.dirname(buffer.outputTs)).isDirectory()) {
        return OptionsReadError.READ_OUTPUT_DIRECTORY_DOES_NOT_EXIST;
    } else {
        return {
            inputDalf: buffer.inputDalf,
            outputTs: buffer.outputTs
        };
    }
}

export function read(argv: string[]): Options | OptionsReadError {
    const parseResult = parse(argv);
    if (typeof parseResult === 'number') {
        return parseResult;
    } else {
        return interpret(parseResult);
    }
}