# Passo a passo: montar o projeto do zero, na mão

Guia de estudo para entender **e repetir sozinho** o que foi feito. Leia na ordem.

---

## 0. O que o projeto faz, em uma frase

> Baixa os focos de queimada do INPE, coloca no **S3**, lê esse arquivo **em stream** e manda aos poucos para uma fila **SQS**; uma **Lambda** pega cada mensagem, agrupa os pontos em **hexágonos H3**, soma em uma tabela **DynamoDB**; outra **Lambda** pública lê a tabela e o **mapa** desenha os hexágonos.

```
INPE ─► produtor (seu PC) ─► S3 ─► produtor lê em stream ─► SQS FIFO ─► Lambda consumidora ─► DynamoDB ─► Lambda API ─► mapa D3
```

| Pedido do professor | Onde está |
|---|---|
| 1. Base geolocalizada | Focos de calor do INPE (data, satélite, latitude, longitude) |
| 2. Dados no S3 | `producer/main.py` → `upload` do `consolidated.csv` |
| 3. Produtor SQS em stream | `stream_dataset()` em `producer/main.py`: abre o objeto do S3 e lê linha a linha |
| 3a. Velocidade do loop | `READ_INTERVAL_SECONDS` (o `time.sleep(delay)` no loop) |
| 3b. N registros por mensagem (máx 10) | `RECORDS_PER_MESSAGE` (valida 1..10) |
| 4. Lambda consumidora | `consumer/handler.py`, gatilho SQS no `infra/template.yaml` |
| 5. Clusterizar, concorrência 1 | Célula H3 (`h3.latlng_to_cell`), `ReservedConcurrentExecutions: 1` |
| 6. Salvar no DynamoDB | Transação: marcador de deduplicação + 3 contadores `ADD count 1` |
| 7. Backend + mapa | `backend/` (Lambda com Function URL) + `frontend/` (D3) |

---

## 1. Ferramentas (uma vez só)

Tudo roda no **Ubuntu do WSL** (o produtor usa `fcntl`, que não existe no Windows).

```powershell
wsl --install -d Ubuntu      # no PowerShell, se ainda não tiver
wsl -d Ubuntu                # entra no Ubuntu
```

Dentro do Ubuntu:

```bash
sudo apt update && sudo apt install -y python3-venv python3-pip unzip curl

# AWS CLI (fala com a AWS pelo terminal)
curl -sSL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip
unzip -q awscliv2.zip && sudo ./aws/install && aws --version

# SAM CLI (lê o template e cria tudo na AWS)
curl -sSL https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip -o sam.zip
unzip -q sam.zip -d sam-inst && sudo ./sam-inst/install && sam --version
```

**Docker**: o SAM monta as Lambdas como imagens Docker. No Docker Desktop → *Settings → Resources → WSL integration* → ligue **Ubuntu** → *Apply & restart*. Teste com `docker ps` dentro do Ubuntu.

Ambiente Python do projeto:

```bash
cd "/mnt/c/Users/carlo/OneDrive/Área de Trabalho/queimadas-ais"
python3 -m venv ~/venv-queimadas
source ~/venv-queimadas/bin/activate
pip install -r requirements-dev.txt -r producer/requirements.txt -r backend/requirements.txt
python -m pytest -q tests     # deve dar 81 passed
```

---

## 2. Conta AWS e credenciais

1. Conta no **Free plan** (sem cobrança no cartão; usa créditos).
2. **IAM → Users → Create user** (`inpe-dev`) → *Attach policies directly* → `AdministratorAccess`.
3. Usuário → **Security credentials → Create access key** → *CLI*.
4. No Ubuntu (digite você mesmo, nunca cole as chaves em chat):

```bash
aws configure --profile inpe-local
#   Access key / Secret / região: us-east-2 / formato: json
aws sts get-caller-identity --profile inpe-local   # mostra a conta = credenciais OK
```

> ⚠️ **Sua conta só permite a região `us-east-2` (Ohio).** As outras são bloqueadas por uma política da organização. Use sempre `us-east-2`.

---

## 3. Criar a infraestrutura (deploy)

O arquivo `infra/template.yaml` **descreve** tudo (bucket, filas, tabela, Lambdas, permissões). O SAM transforma isso em uma *stack* do CloudFormation.

```bash
export AWS_PROFILE=inpe-local AWS_DEFAULT_REGION=us-east-2
cd "/mnt/c/Users/carlo/OneDrive/Área de Trabalho/queimadas-ais"

sam validate --lint -t infra/template.yaml        # confere o template
sam build --template-file infra/template.yaml     # monta as 2 imagens Docker (no seu PC)
sam deploy --stack-name inpe-h3 --region us-east-2 \
  --resolve-s3 --resolve-image-repos \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides H3Resolution=5 FrontendOrigin=http://localhost:5500 ReserveConsumerConcurrency=false
```

- `sam build` → cria as imagens a partir de `consumer/Dockerfile` e `backend/Dockerfile`.
- `sam deploy` → envia as imagens para o **ECR** e cria os recursos.
- `ReserveConsumerConcurrency=false` → **só enquanto** a AWS não aprova o aumento do limite de Lambdas (a conta nova tem limite 10, e reservar 1 exige sobrar 10). Quando aprovar, rode de novo com `=true`.

Ver os endereços criados:

```bash
aws cloudformation describe-stacks --stack-name inpe-h3 --query 'Stacks[0].Outputs' --output table
```

Anote `BucketName`, `QueueUrl` e `ApiUrl`.

---

## 4. Rodar o produtor (manda os dados)

```bash
source ~/venv-queimadas/bin/activate
export AWS_PROFILE=inpe-local AWS_DEFAULT_REGION=us-east-2
export S3_BUCKET=<BucketName>
export SQS_QUEUE_URL=<QueueUrl>
export RECORDS_PER_MESSAGE=10        # N registros por mensagem (1..10)
export READ_INTERVAL_SECONDS=0.2     # pausa entre mensagens = velocidade do loop

python -m producer.main --scrape --start --max-files 2
```

- `--scrape` → baixa do INPE, junta em `data/consolidated.csv`, envia ao S3.
- `--start` → abre o arquivo **no S3 em stream** e manda para a fila.
- O log mostra `Enviados 10 registros, total desta rodada=...`.
- Rodar de novo **não reenvia** o que já foi (checkpoint em `data/state.sqlite3`).

---

## 5. Ver o mapa

No PowerShell:

```powershell
python -m http.server 5500 --directory "C:\Users\carlo\OneDrive\Área de Trabalho\queimadas-ais\frontend"
```

Abra **http://localhost:5500**. O endereço da API está em `frontend/config.js` (`apiBaseUrl` = `ApiUrl`).

Teste direto da API no navegador: `<ApiUrl>/api/clusters` e `<ApiUrl>/api/clusters?satellite=GOES-19`.

---

## 6. Onde ver cada peça no console AWS (região **Ohio**)

Bom para mostrar na apresentação:

| Serviço | O que mostrar |
|---|---|
| **CloudFormation** → `inpe-h3` | Aba *Resources*: tudo que o template criou |
| **S3** → `inpe-h3-datasetbucket-...` | Pasta `consolidated/` com o CSV |
| **SQS** → `inpe-h3-InputQueue-...fifo` | *Monitoring*: mensagens enviadas/recebidas; DLQ vazia = sem erro |
| **Lambda** → `inpe-h3-consumer` | *Configuration → Triggers*: SQS, batch size 1; *Concurrency*: reservada (1) |
| **Lambda** → `inpe-h3-consumer` → *Monitor → View CloudWatch logs* | Uma execução por mensagem |
| **DynamoDB** → tabela `inpe-h3-DataTable-...` → *Explore items* | `pk = RES#5#H3COUNT#ALL`, `sk = H3#<hash>`, `count` |
| **Lambda** → backend → *Configuration → Function URL* | O endereço público da API |

---

## 7. Criar pelo console, clicando (para entender cada peça)

O SAM faz isso automaticamente, mas é assim que seria manualmente:

1. **S3** → *Create bucket* → nome único → bloquear acesso público (padrão) → *Create*.
2. **SQS** → *Create queue* → tipo **FIFO** → nome terminando em `.fifo` → *Visibility timeout* 360 s → em *Dead-letter queue* aponte para outra fila FIFO (máx. 5 tentativas).
3. **DynamoDB** → *Create table* → *Partition key* `pk` (String), *Sort key* `sk` (String) → capacidade *On-demand*.
4. **ECR** + **Lambda consumidora**:
   - Monte e envie a imagem (no Ubuntu):
     ```bash
     aws ecr create-repository --repository-name inpe-consumer
     aws ecr get-login-password | docker login --username AWS --password-stdin <conta>.dkr.ecr.us-east-2.amazonaws.com
     docker build -f consumer/Dockerfile -t <conta>.dkr.ecr.us-east-2.amazonaws.com/inpe-consumer:latest .
     docker push <conta>.dkr.ecr.us-east-2.amazonaws.com/inpe-consumer:latest
     ```
   - **Lambda** → *Create function* → *Container image* → escolha a imagem.
   - *Configuration → Environment variables*: `TABLE_NAME`, `H3_RESOLUTION=5`.
   - *Configuration → Permissions* → role: permissão de ler a fila (`AWSLambdaSQSQueueExecutionRole`) e `dynamodb:PutItem/UpdateItem` na tabela.
   - *Configuration → Concurrency* → **Reserve concurrency = 1**.
   - *Add trigger* → SQS → a fila → *Batch size* 1 → marcar *Report batch item failures*.
5. **Lambda da API**: mesma coisa com `backend/Dockerfile`, permissão só `dynamodb:Query` na tabela → *Configuration → Function URL* → *Auth type* **NONE** → CORS com a origem do site.

---

## 8. Apagar tudo (quando acabar)

```bash
sam delete --stack-name inpe-h3 --region us-east-2
```

A tabela DynamoDB tem `DeletionPolicy: Retain` (não é apagada junto, para não perder dados por engano); apague no console se quiser.

---

## 9. Perguntas que o professor pode fazer

**Por que uma fila no meio?** Desacopla: o produtor não precisa esperar o banco; se a Lambda falhar, a mensagem volta para a fila e é reprocessada; depois de 5 falhas vai para a **DLQ**.

**Por que FIFO?** Mantém a ordem cronológica. Todas as mensagens usam o mesmo `MessageGroupId`, então a fila entrega **uma de cada vez**, na ordem.

**Onde está o "stream"?** O produtor não baixa o arquivo inteiro do S3: abre o `Body` do `get_object` e lê linha a linha com `csv.DictReader`. Há um teste que prova que a 1ª mensagem sai antes de ler o arquivo todo.

**Como calibra a velocidade?** `READ_INTERVAL_SECONDS` — pausa depois de cada mensagem. `RECORDS_PER_MESSAGE` — quantos registros por mensagem (o limite 10 é validado).

**Que clusterização é essa?** Agrupamento espacial por grade **H3** (hexágonos da Uber). Cada ponto cai em um hexágono (`latlng_to_cell`) e soma 1 no contador dele. Não é K-means/DBSCAN: esses precisam de todos os pontos juntos; a grade funciona ponto a ponto, que é o que um consumidor de stream com concorrência 1 consegue fazer.

**Por que concorrência 1?** Para processar uma mensagem por vez, sem duas Lambdas escrevendo o mesmo contador ao mesmo tempo e mantendo a ordem.

**E se a mesma mensagem chegar duas vezes?** Cada observação tem um ID (SHA-256 de data, satélite e coordenadas). A Lambda grava um marcador `EVENT#<id>` com a condição "só se não existir" na **mesma transação** dos contadores. Se já existe, a transação inteira é cancelada: nada conta duas vezes.

**O que fica no DynamoDB?** Só hash H3 + contagem: total geral (`...#ALL`), por satélite (`...#SAT#<nome>`), total por satélite (`...#SATELLITES`) e os marcadores de deduplicação.

**Como o mapa recebe os dados?** O navegador chama a Function URL (`/api/clusters`), recebe GeoJSON com o polígono de cada hexágono e a contagem, pagina com `next_cursor`, e o D3 pinta.
