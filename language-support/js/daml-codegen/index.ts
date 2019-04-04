// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as opts from './options';
import { OptionsReadError } from './options';
import * as fs from 'fs';
import * as ts from 'typescript';
import { DalfProcessor } from './processor';

const options = opts.read(process.argv.slice(2));

if (typeof options === 'number') {
    switch (options) {
        case OptionsReadError.PARSE_MISSING_INPUT:
            console.log('no parameter for -i flag');
            break;
        case OptionsReadError.PARSE_MISSING_OUTPUT:
            console.log('no parameter for -o flag');
            break;
        case OptionsReadError.PARSE_UNRECOGNIZED:
            console.log('unrecognized option');
            break;
        case OptionsReadError.READ_INPUT_IS_MISSING:
            console.log('input file is missing');
            break;
        case OptionsReadError.READ_INPUT_DOES_NOT_EXIST:
            console.log('input file does not exist');
            break;
        case OptionsReadError.READ_OUTPUT_DIRECTORY_DOES_NOT_EXIST:
            console.log('output directory does not exist');
            break;
    }
    console.log(`usage ${process.argv[0]} ${process.argv[1]} -i <input.dalf> [-o output.ts]`)
    process.exit(1);
} else {
    fs.readFile(options.inputDalf, (error, data) => {
        if (error) throw error;
        const printer = ts.createPrinter({ newLine: ts.NewLineKind.LineFeed });
        const processor = new DalfProcessor(options.outputTs, printer);
        const source = processor.process(data);
        console.log(source.text);
        printer.printFile(source);
    });
}