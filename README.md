# Observatório INPE · H3

> **Modelo atual:** o banco guarda apenas **hash H3 + contagem**, sem dimensão de tempo. O mapa mostra o total acumulado por célula, no agregado de todos os satélites ou de um satélite.

Pipeline de focos de calor com produtor na sua máquina, dados brutos do INPE, consolidação cronológica, S3, SQS FIFO, Lambda consumidora, agregação H3, DynamoDB e mapa em D3.

## Arquitetura e fluxo dos dados

A arquitetura separa a **ingestão**, que começa na sua máquina e grava na AWS, da **visualização**, em que o navegador consulta uma Lambda pública. O mapa pode ser hospedado na Vercel ou em qualquer serviço de arquivos estáticos; ele não depende de um servidor local nem recebe credenciais AWS.

[![Diagrama da arquitetura: produtor local, serviços AWS e mapa D3](docs/architecture.svg)](docs/architecture.svg)

O diagrama separa o produtor local (verde), os serviços AWS (roxo) e a hospedagem/navegador (cinza). Clique na imagem para abrir em tamanho completo.

### 1. Fonte de dados: INPE

O produtor acessa a listagem pública do INPE e identifica os CSVs disponíveis. Esses arquivos representam intervalos de publicação de dez minutos e contêm latitude, longitude, satélite e data/hora da observação. A periodicidade de publicação é independente da frequência com que o mapa consulta a API. A fonte possui uma janela de retenção: baixar todos os arquivos disponíveis não significa baixar todo o histórico de queimadas do INPE.

### 2. Produtor local: download, consolidação e S3

Com `--scrape`, o produtor baixa os arquivos para `data/raw/`, normaliza datas em UTC e registra as observações no SQLite. O banco local remove duplicatas e permite ordenar os registros em disco, sem carregar todo o dataset na memória. Em seguida, gera `data/consolidated.csv` em ordem cronológica e envia um snapshot ao S3 privado.

O caminho do snapshot inclui o SHA-256 do conteúdo: `consolidated/<sha256>.csv`. A referência do último upload concluído fica em `data/s3-dataset.json`; é ela que conecta a etapa de preparação à etapa de streaming. O SQLite também guarda os arquivos já processados e os checkpoints de envio por fila.

### 3. Streaming local: S3 para SQS

Com `--start`, o produtor abre o snapshot indicado no manifesto e lê o CSV diretamente do S3 como stream. Ele não faz novo scraping nem novo upload nessa etapa. Cada mensagem SQS contém de 1 a 10 observações, conforme `RECORDS_PER_MESSAGE`. A pausa entre mensagens é definida por `READ_INTERVAL_SECONDS`.

Após um envio confirmado, o produtor grava o checkpoint no SQLite. Se houver interrupção entre o envio e o checkpoint, a observação pode ser reenviada; a proteção adicional no consumidor impede que isso duplique sua contagem no DynamoDB.

### 4. Fila FIFO, consumidor e tratamento de falhas

A fila SQS desacopla a velocidade de leitura do produtor da velocidade de processamento da Lambda. Todas as mensagens usam o mesmo `MessageGroupId`, `inpe-chronological`, para preservar a ordem de envio e processar esse grupo sequencialmente. A Lambda recebe uma mensagem por invocação; essa mensagem pode conter até dez observações.

A configuração desejada da atividade é concorrência reservada igual a 1, declarada no template SAM. Na configuração manual desta conta, a reserva ficou como **unreserved** devido à quota disponível; o único grupo FIFO mantém o consumo desse grupo sequencial. Isso não equivale a configurar a reserva da função como 1.

Para cada observação, a Lambda converte latitude/longitude para uma célula H3 na resolução configurada. Mensagens que falham voltam a ser tentadas; após cinco recebimentos sem sucesso, seguem para a DLQ para investigação e eventual reenvio. O consumidor usa `ReportBatchItemFailures` para sinalizar falhas ao SQS.

### 5. Persistência: hash H3 + contagem

O DynamoDB não guarda data, latitude nem longitude. Para cada observação, a Lambda grava em **uma única transação**:

- um marcador de deduplicação `EVENT#<id>` (condição `attribute_not_exists(pk)`);
- `ADD count 1` na célula do agregado de todos os satélites;
- `ADD count 1` na célula do satélite da observação;
- `ADD count 1` no total do satélite, usado como catálogo do filtro.

Se o marcador já existe, a transação inteira é cancelada e nada é incrementado: reentregas do SQS não contam duas vezes.

### 6. Lambda pública de consulta

A Lambda de consulta faz `Query` com leitura consistente em uma única partição: a do agregado ou a do satélite escolhido. Não há índice secundário nem `Scan`. A API devolve GeoJSON com `{h3, count, resolution}` por célula e `next_cursor` para paginação; a primeira página também traz a lista `satellites`. O cursor só é aceito na mesma partição em que foi emitido.

A Function URL fornece o endpoint HTTPS público, sem API Gateway. A Lambda utiliza uma IAM role com permissão `dynamodb:Query` somente na tabela, além de logs. CORS autoriza as origens do frontend; ele não torna a API privada nem substitui autenticação.

### 7. Website estático e mapa D3

A hospedagem entrega os arquivos da pasta `frontend/` ao navegador. O JavaScript lê a URL pública em `config.js`, consulta a API e percorre todas as páginas. Depois soma as contagens pelo mesmo hash H3 e desenha uma célula por hash.

Não há filtro de tempo: o mapa mostra **o total acumulado**. O único filtro é o de satélite: **Todos (agregado)** ou um satélite da lista devolvida pela API. Trocar a opção refaz a consulta com `?satellite=`.

Uma nova consulta começa cinco segundos após terminar a anterior. Cada rodada substitui os dados exibidos, em vez de somar novamente a resposta anterior. Em falhas de consulta, o mapa preserva o último resultado e sinaliza que ele pode estar desatualizado.

### Comandos relacionados ao fluxo

| Comando | Etapa executada |
|---|---|
| `python -m producer.main --reset` | Apaga downloads, dataset, registros e checkpoints locais; preserva AWS |
| `python -m producer.main --scrape --max-files 6` | Processa os seis arquivos mais recentes da listagem e envia o consolidado ao S3 |
| `python -m producer.main --start` | Lê o snapshot preparado no S3 e envia observações ainda não enviadas ao SQS |
| `python -m producer.main --scrape --start` | Executa preparação/upload e depois envio à fila |

Os arquivos e registros locais são cumulativos entre rodadas. `--max-files 6` limita a seleção de novos arquivos da listagem, mas não remove registros anteriores do consolidado. Para um teste local do zero, use `--reset --scrape --max-files 6`; o reset local não limpa o DynamoDB ou o S3. Cada execução roda a rotina uma vez; para repetir, agende execuções externas (cron, Agendador de Tarefas).

## Aplicações isoladas

| Diretório | Função | Execução |
|---|---|---|
| `producer/` | Download, consolidação, upload, streaming e envio | Sua máquina, uma execução por chamada |
| `consumer/` | H3 e persistência idempotente | Lambda acionada pelo SQS |
| `backend/` | Consulta paginada de células H3 e GeoJSON | Lambda com Function URL pública; servidor local opcional |
| `frontend/` | Mapa Brasil, totais por H3, filtro de satélite e zoom | Website estático, hospedagem independente |
| `common/` | Contrato e normalização das observações | Compartilhado no build |
| `infra/` | Infraestrutura declarativa AWS SAM | CloudFormation |
| `tests/` | Testes de rotina e integração simulada | pytest / Moto |

Para os recursos já criados manualmente: **[Publicar o mapa estático e a Lambda de consulta](docs/MAPA_PUBLICO.md)**.

Para uma instalação nova do pipeline completo: **[Configuração e implantação na AWS](docs/AWS.md)**.

## Decisões do processamento

**Fonte:** [diretório CSV de dez minutos do INPE](https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/10min/). Na primeira execução, baixa todos os arquivos que estiverem na listagem. A fonte tem uma janela de retenção: não equivale a todo o acervo histórico do INPE. `--max-files 6` permite um teste com os seis arquivos mais recentes. Quando existir a coluna `pais`/`country`, o produtor filtra Brasil por padrão. O formato real de dez minutos (`lat,lon,satelite,data`) não informa país: nesse caso todas as observações são preservadas e um aviso é registrado, pois não é possível determinar o país somente pelo cabeçalho.

**Rotina:** descobre arquivos → baixa para `data/raw/` → normaliza → deduplica no SQLite → exporta `data/consolidated.csv` em ordem crescente de data UTC → envia para `s3://BUCKET/consolidated/SHA256.csv` → abre o objeto no S3 como stream → envia os registros ainda não enviados para SQS. A ordenação usa `(date, id)` para desempate determinístico. O CSV consolidado é único localmente; o S3 guarda snapshots identificados por conteúdo. Os CSVs brutos permanecem locais.

**Memória:** downloads e leitura do S3 usam blocos; a ordenação fica a cargo de um índice SQLite em disco. O produtor não precisa carregar todos os CSVs em um DataFrame. `RECORDS_PER_MESSAGE` determina os registros por mensagem (1–10); `READ_INTERVAL_SECONDS` determina a pausa entre mensagens. São conceitos diferentes do tamanho do lote de mensagens entregue à Lambda, configurado como **1 mensagem por invocação**.

**Retomada:** o SQLite guarda arquivos processados e IDs enviados por URL de fila. Após cada envio confirmado, registra o checkpoint. Se cair entre o envio e o checkpoint, pode reenviar; a Lambda evita a duplicação no banco. O bloqueio local impede duas instâncias com o mesmo `DATA_DIR` (Linux/macOS). Use apenas um produtor por fila para preservar a sequência. Os arquivos publicados são tratados como imutáveis; correções de arquivos já baixados exigem revisão do cache/estado.

**Datas:** datas INPE sem fuso são interpretadas como GMT/UTC, conforme a [descrição de atributos do INPE](https://www.terrabrasilis.dpi.inpe.br/queimadas/portal/pages/secao_informacoes/faq/index.html). Todas são normalizadas para ISO 8601 UTC. O dataset é ordenado por instante, não pelo nome do CSV. Cada snapshot é ordenado; arquivos atrasados podem trazer observações anteriores às já enviadas em execuções anteriores. A fila preserva a ordem de envio, não corrige atrasos da fonte.

**H3:** `latlng_to_cell(latitude, longitude, resolution)` agrupa pontos em uma grade geográfica, conforme a [API H3](https://h3geo.org/docs/api/indexing/). Resolução padrão 5. Não é DBSCAN nem identificação de incêndios: cada contador é **célula H3 + resolução**, no agregado ou por satélite. Observações em células adjacentes continuam sendo contadores distintos. A data/hora é usada apenas para ordenar o envio e compor o ID de deduplicação; não é gravada no banco.

**DynamoDB:** uma tabela (`pk`/`sk`, sem índice secundário), quatro tipos de item:

| Item | `pk` | `sk` | Atributos |
|---|---|---|---|
| Célula, todos os satélites | `RES#5#H3COUNT#ALL` | `H3#<hash>` | `count` |
| Célula, um satélite | `RES#5#H3COUNT#SAT#AQUA` | `H3#<hash>` | `count` |
| Total por satélite | `RES#5#H3COUNT#SATELLITES` | `SAT#AQUA` | `count` |
| Deduplicação | `EVENT#<sha256>` | `RES#5#H3COUNT` | — |

O ID é SHA-256 de data/hora, satélite e coordenadas com oito casas decimais. Duas linhas iguais nesses campos contam como uma observação, mesmo vindas de arquivos diferentes. O marcador e os três incrementos são uma transação atômica. Marcadores não expiram, para que replays antigos continuem idempotentes; o armazenamento cresce com as observações. Não apague marcadores isoladamente.

**Consulta:** `Query` com `ConsistentRead` na partição `RES#<res>#H3COUNT#ALL` ou `RES#<res>#H3COUNT#SAT#<satélite>`. O backend converte bordas H3 para GeoJSON; o frontend soma as contagens entre páginas. Sempre siga `next_cursor`, mesmo se uma página estiver vazia.

**Mapa:** usa D3, projeção Mercator e limite nacional do [World Atlas](https://github.com/topojson/world-atlas). Células são recortadas visualmente pelo Brasil; os contadores incluem todas as observações recebidas, inclusive fora do Brasil quando o CSV não informa país. O recorte visual não é um filtro geográfico das observações. A consulta é refeita a cada cinco segundos após concluir a anterior; a fonte é atualizada a cada dez minutos. A paginação durante novas escritas não é um snapshot transacional e converge na próxima atualização. D3, TopoJSON e a malha são carregados de CDN e precisam de internet.

## Executar o produtor

Python **3.12** recomendado (mesma versão das imagens Lambda).

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r producer/requirements.txt

# Apenas baixar e consolidar; não requer credenciais AWS.
python -m producer.main --prepare-only --max-files 6

# Depois da implantação e da configuração das variáveis AWS:
export S3_BUCKET='OUTPUT-BucketName'
export SQS_QUEUE_URL='OUTPUT-QueueUrl'
export RECORDS_PER_MESSAGE=10
export READ_INTERVAL_SECONDS=1
python -m producer.main --scrape --start
```

Cada execução roda a rotina uma única vez e termina. Para repetir periodicamente, agende o comando (cron, Agendador de Tarefas) com diretório de trabalho e ambiente configurados; não rode duas instâncias ao mesmo tempo. Linhas inválidas interrompem a ingestão do arquivo, registram nome/linha e preservam os demais checkpoints; não são descartadas silenciosamente. O formato `lat,lon,satelite,data` é aceito sem exigir país. Cabeçalhos aceitos também incluem `data_hora_gmt`, `satelite`, `lat`, `lon`, `pais` e variantes documentadas como `DataHora`, `Latitude`, `Longitude`.

## Executar o backend local

```bash
python -m pip install -r backend/requirements.txt
export TABLE_NAME='OUTPUT-TableName'
export AWS_DEFAULT_REGION=sa-east-1
export AWS_PROFILE=inpe-local
export H3_RESOLUTION=5
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
```

Abra **http://127.0.0.1:8000**. Somente para executar o backend local contra o banco, o perfil precisa de `dynamodb:Query` na tabela; o perfil mínimo do produtor não inclui essa permissão. O servidor local é opcional. A publicação usa `backend.handler.handler` em uma Lambda com Function URL pública e o frontend em hospedagem estática. O browser consulta a URL em `frontend/config.js`, sem usuário AWS local. O SAM empacota a Lambda de consulta separadamente, sem FastAPI ou frontend na imagem.

API:

```text
GET /api/health
GET /api/clusters                       # agregado de todos os satélites
GET /api/clusters?satellite=AQUA        # um satélite
GET /api/clusters?limit=500&cursor=<next_cursor>
GET /docs  # somente no servidor FastAPI opcional
```

`/api/clusters` aceita apenas `satellite`, `cursor` e `limit` (1–1000). Os parâmetros antigos `day`, `window`, `view`, `minutes` e `as_of` retornam **400**.

## Testes

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q tests
node --check frontend/app.js
```

Os testes de integração usam Moto e H3 real: verificam que os itens guardam só hash + contagem, transações idempotentes, reentrega após falha parcial, comportamento FIFO, paginação, filtro de satélite e GeoJSON. Um teste do produtor prova que o envio S3 → SQS é streaming: a primeira mensagem sai antes de o objeto S3 ser lido por inteiro. Quando essas dependências não estão instaladas, o módulo de integração é explicitamente pulado. Os testes do produtor usam streams e clientes falsos, sem acesso à AWS/INPE.

## Limites e operação

- A configuração AWS deve ser aplicada na sua conta; este repositório não inclui credenciais.
- `infra/template.yaml` cria uma Function URL pública para leitura dos agregados, com CORS controlado por `FrontendOrigin` e output `ApiUrl`. O website é publicado separadamente. CORS não autentica chamadas; dados são públicos e invocações geram cobrança.
- O número de itens lidos por rodada é o número de células ocupadas (não de observações), o que mantém a consulta barata mesmo com histórico longo. Cada partição de contagem recebe uma escrita por observação; volumes muito maiores podem exigir sharding das partições quentes.
- Para reconstruir em outra resolução, altere `H3Resolution` em ambas as Lambdas e reenvie o dataset usando um novo `DATA_DIR` ou limpando somente os checkpoints `sent`, com o produtor parado. A chave de deduplicação inclui resolução; isso permite reprocessar sem misturar as contagens antigas.
- A DLQ recebe mensagens que falharem cinco vezes. Isso permite avançar a fila após a falha, mas perde a ordem estrita em relação à mensagem que foi isolada. Corrija a causa antes de fazer redrive.

## Atualizar a partir das versões com tempo

Reimplante consumidor, backend e o template juntos. O template remove o índice `ObservationsByTime` e o atributo `date`. As novas chaves usam o namespace `H3COUNT`, isoladas dos itens antigos (`DAY#...`, `WINDOW10M`); o backend lê apenas as novas partições. Os dados antigos não são convertidos automaticamente; use [o reset](docs/RESET.md) se quiser removê-los.

Para popular as contagens com o dataset já baixado, pare o produtor e limpe **somente os checkpoints de envio**:

```bash
python3 - <<'PYTHON'
import os
import sqlite3
from pathlib import Path
path = Path(os.getenv('DATA_DIR', 'data')) / 'state.sqlite3'
if not path.is_file():
    raise SystemExit(f'Estado não encontrado: {path}')
with sqlite3.connect(path) as db:
    db.execute('DELETE FROM sent WHERE destination = ?', (os.environ['SQS_QUEUE_URL'],))
PYTHON
python -m producer.main --scrape --start
```

Faça uma cópia de segurança do SQLite antes da limpeza. Aguarde a fila esvaziar antes de trocar o consumidor. A deduplicação por observação/resolução/`H3COUNT` impede contagem repetida durante o replay.

## Website estático + Function URL

A Lambda de consulta tem somente permissão `dynamodb:Query`, além dos logs. O consumidor e o produtor permanecem independentes. O frontend chama `GET /api/clusters` na URL pública e não usa SDK AWS ou credenciais. No console, crie a Function URL com autenticação `NONE` e CORS para a origem do frontend. Detalhes, pacotes e permissões públicas estão no [guia do mapa](docs/MAPA_PUBLICO.md).

```bash
# No CloudShell, após enviar e extrair artifacts/inpe-api-source.zip:
bash scripts/build-backend-zip.sh
# Gera dist/inpe-api.zip; handler backend.handler.handler.
python3 scripts/build-frontend.py --api-url 'https://ID.lambda-url.sa-east-1.on.aws/'
# Gera dist/inpe-mapa.zip para upload manual no Amplify Hosting.
```

O pacote de fontes pode ser montado offline, mas o ZIP final da Lambda requer download das dependências. O frontend usa caminhos relativos para os assets e pode ser servido por qualquer hospedagem estática. `apiBaseUrl` vazio preserva o uso com o servidor local; na hospedagem estática configure a URL real ou use o script de empacotamento acima.

## Reset test data

Use [`scripts/reset_aws_data.py`](scripts/reset_aws_data.py) to empty DynamoDB and the versioned S3 bucket while preserving resources. It previews by default; `--execute` permanently deletes the data. Stop the producer and drain SQS first. Run with administrative/cleanup credentials, not the producer's restricted profile. See [reset instructions and permissions](docs/RESET.md), including optional local checkpoint reset for replay.

## Producer actions

Flags run in this order: **reset → scrape/consolidate/upload → SQS streaming**. They can be run separately:

```bash
python -m producer.main --reset
python -m producer.main --scrape --max-files 6
python -m producer.main --start
```

`--reset` removes local downloads, consolidated records, SQLite/checkpoints and the S3 dataset manifest; it does not delete AWS data. `--scrape` downloads the currently available INPE CSVs (or the latest N with `--max-files`), consolidates them chronologically and uploads the CSV to S3. It does not send SQS messages. It saves the uploaded object reference in `data/s3-dataset.json`.

`--start` reads the uploaded object referenced by that manifest and sends unsent observations to SQS, without scraping or uploading again. Run `--scrape` first. Existing send checkpoints remain effective. `--reset --start` requires `--scrape` as well, because reset removes the prepared dataset reference.

```bash
# Complete fresh local run, including AWS streaming:
python -m producer.main --reset --scrape --start

# Download/consolidate only, without AWS access:
python -m producer.main --prepare-only --max-files 6
```

`--scrape` needs S3_BUCKET; `--start` needs S3_BUCKET and SQS_QUEUE_URL. Credentials are initialized only for actions that use AWS. Running without action flags displays help and does no work. Each invocation runs the routine once; schedule it externally to repeat. Cached files and records remain cumulative unless `--reset` is used: `--max-files 6` limits newly discovered input files, not the total historical contents of an existing dataset.
