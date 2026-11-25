/* O navegador usa somente a API HTTP; nenhuma credencial AWS chega ao cliente. */
'use strict';
const config = window.INPE_CONFIG || {};
const apiBaseUrl = (config.apiBaseUrl || '').replace(/\/+$/, '');
const refreshIntervalMs = Math.max(1000, Number(config.refreshIntervalMs) || 5000);
const $ = id => document.getElementById(id);
const state = {features: [], satellites: [], satellite: '', busy: false, ready: false, generation: 0, request: null};
const svg = d3.select('svg');
const scene = d3.select('#scene');
const zoom = d3.zoom().scaleExtent([1, 12]).on('zoom', event => scene.attr('transform', event.transform));
svg.call(zoom);
$('zoom-in').onclick = () => svg.transition().call(zoom.scaleBy, 1.5);
$('zoom-out').onclick = () => svg.transition().call(zoom.scaleBy, 1 / 1.5);
$('reset').onclick = () => svg.transition().call(zoom.transform, d3.zoomIdentity);
let path;

// GeoJSON usa anéis anti-horários; D3 usa sentido horário para polígonos pequenos.
function forD3(feature) {
  const result = structuredClone(feature);
  if (d3.geoArea(result) > 2 * Math.PI && result.geometry.type === 'Polygon') {
    result.geometry.coordinates = result.geometry.coordinates.map(ring => ring.reverse());
  }
  return result;
}

function draw() {
  if (!state.ready) return;
  const cells = window.INPE_DATA.aggregateCells(state.features)
    .map(cell => ({...cell, feature: forD3(cell.feature)}));
  const max = d3.max(cells, d => d.count) || 1;
  const color = d3.scaleSequentialLog(d3.interpolateYlOrRd).domain([1, Math.max(2, max)]);
  const label = state.satellite || 'todos os satélites';
  $('total').textContent = d3.sum(cells, d => d.count).toLocaleString('pt-BR');
  $('cells').textContent = cells.length.toLocaleString('pt-BR');
  $('satellites').textContent = state.satellites.length.toLocaleString('pt-BR');
  $('range').textContent = cells.length ? `1 – ${max.toLocaleString('pt-BR')}` : 'Sem detecções';
  d3.select('#hexagons').selectAll('path').data(cells, d => d.h3).join('path')
    .attr('class', 'cell').attr('d', d => path(d.feature)).attr('fill', d => color(d.count))
    .attr('tabindex', 0).attr('aria-label', d => `${d.count} focos, H3 ${d.h3}, ${label}`)
    .on('pointerenter focus', (_, d) => {
      $('tooltip').hidden = false;
      $('tooltip').textContent = `${d.count.toLocaleString('pt-BR')} focos de calor\nSatélite: ${label}\nH3: ${d.h3}`;
    }).on('pointerleave blur', () => { $('tooltip').hidden = true; });
}

async function getJSON(url, signal) {
  const timeout = AbortSignal.timeout(20000);
  const response = await fetch(url, {signal: signal ? AbortSignal.any([signal, timeout]) : timeout, cache: 'no-store'});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

// Opções do select: agregado + satélites conhecidos pela API (preserva a seleção atual).
function updateSatelliteOptions(satellites) {
  const names = satellites.map(s => s.satellite).sort();
  if (state.satellite && !names.includes(state.satellite)) names.push(state.satellite);
  const select = $('satellite');
  const current = [...select.options].map(o => o.value).slice(1);
  if (current.join('|') !== names.join('|')) {
    select.replaceChildren(new Option('Todos (agregado)', ''), ...names.map(s => new Option(s, s)));
  }
  select.value = state.satellite;
}

async function refresh() {
  if (state.busy || !state.ready) return;
  state.busy = true;
  const generation = state.generation;
  const satellite = state.satellite;
  state.request = new AbortController();
  $('status').textContent = 'Consultando DynamoDB…';
  try {
    let cursor = null;
    let satellites = null;
    const features = [];
    do {
      const params = new URLSearchParams({limit: '1000'});
      if (satellite) params.set('satellite', satellite);
      if (cursor) params.set('cursor', cursor);
      const page = await getJSON(`${apiBaseUrl}/api/clusters?${params}`, state.request.signal);
      if (!Array.isArray(page.features)) throw new Error('Resposta inválida da API');
      if (Array.isArray(page.satellites)) satellites = page.satellites;
      features.push(...page.features);
      cursor = page.next_cursor;
      if (generation !== state.generation) return;
    } while (cursor);
    state.features = features;
    state.satellites = satellites || [];
    updateSatelliteOptions(state.satellites);
    draw();
    $('status').textContent = features.length ? `● Dados atualizados · atualização a cada ${refreshIntervalMs / 1000} s` : 'Sem focos registrados.';
    $('updated').textContent = `Consulta: ${new Date().toLocaleTimeString('pt-BR')}`;
  } catch (error) {
    if (generation === state.generation) {
      $('status').textContent = `Falha ao atualizar (${error.message}). Nova tentativa automática; os dados exibidos podem estar desatualizados.`;
    }
  } finally {
    state.busy = false;
    state.request = null;
    if (generation !== state.generation) refresh();
  }
}

// Trocar o satélite refaz a consulta na partição correspondente (?satellite=).
function changeSatellite() {
  state.satellite = $('satellite').value;
  state.generation += 1;
  if (state.request) state.request.abort();
  state.features = [];
  draw();
  $('updated').textContent = 'Aguardando consulta';
  refresh();
}
$('satellite').onchange = changeSatellite;
$('refresh').onclick = refresh;
async function poll() {
  await refresh();
  setTimeout(poll, refreshIntervalMs);
}
async function start() {
  try {
    const world = await getJSON('https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json');
    const countries = topojson.feature(world, world.objects.countries);
    const brazil = countries.features.find(f => Number(f.id) === 76);
    if (!brazil) throw new Error('Limite do Brasil ausente');
    path = d3.geoPath(d3.geoMercator().fitExtent([[45, 30], [855, 725]], brazil));
    d3.select('#land').selectAll('path').data(countries.features).join('path')
      .attr('class', f => Number(f.id) === 76 ? 'country brazil' : 'country').attr('d', path);
    d3.select('#brazil-clip path').attr('d', path(brazil));
    state.ready = true;
    poll();
  } catch (error) {
    $('status').textContent = `Erro ao carregar o mapa: ${error.message}. Verifique o acesso ao CDN e recarregue a página.`;
  }
}
start();
