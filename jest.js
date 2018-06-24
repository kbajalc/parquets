"use strict";
process.argv[1] = "./node_modules/jest/bin/jest";
process.argv[process.argv.length - 1] = process.argv[process.argv.length - 1].replace(".ts", ".js");
console.log("-----------"); 
require(process.argv[1]);
// const importLocal = require('import-local');
// if (!importLocal(__filename)) {
//   require('jest/node_modules/jest-cli/bin/jest');
// }
