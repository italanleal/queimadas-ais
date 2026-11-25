const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync('frontend/data.js', 'utf8');
const context = {window: {}};
vm.runInNewContext(source, context);
const aggregate = features => JSON.parse(JSON.stringify(context.window.INPE_DATA.aggregateCells(features)));
const cell = (h3, count) => ({properties: {h3, count, resolution: 5}});
// Páginas diferentes podem trazer o mesmo hash: as contagens são somadas.
const features = [cell('abc', 2), cell('abc', 3), cell('def', 4)];
const cells = aggregate(features);
assert.equal(cells.length, 2);
assert.deepEqual(cells.map(c => [c.h3, c.count]), [['abc', 5], ['def', 4]]);
assert.equal(cells[0].feature.properties.h3, 'abc');
// Cada atualização substitui os dados: não acumula a mesma resposta duas vezes.
assert.equal(aggregate(features)[0].count, 5);
assert.equal(aggregate(features.slice(1))[0].count, 3);
assert.deepEqual(aggregate([]), []);
// Sem dimensão de tempo: data.js não lê propriedades de observação.
assert.ok(!/first_observed_at|last_observed_at/.test(source));
console.log('Agregação H3: soma por hash e substituição entre atualizações validadas.');
