/* Agregação compartilhada pela visualização e pelos testes, sem dependência de D3. */
'use strict';
window.INPE_DATA = {
  // Soma as contagens por hash H3 (páginas diferentes podem repetir uma célula).
  aggregateCells(features) {
    const cells = new Map();
    for (const feature of features) {
      const props = feature.properties;
      let cell = cells.get(props.h3);
      if (!cell) {
        cell = {h3: props.h3, count: 0, feature};
        cells.set(props.h3, cell);
      }
      cell.count += props.count;
    }
    return [...cells.values()];
  }
};
