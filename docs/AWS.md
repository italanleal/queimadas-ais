# Configurar e publicar na AWS

Para a conta com recursos já criados manualmente, siga o [guia de publicação do mapa estático com Function URL](MAPA_PUBLICO.md). O deploy completo abaixo cria um pipeline novo.

## 1. Preparar ferramentas e conta

Instale [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html), [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html), Docker e Python 3.12. O Docker deve estar ativo; as dependências nativas do H3 são instaladas dentro das imagens Linux, evitando copiar bibliotecas do seu computador para a Lambda.

Configure um perfil com permissão de implantação CloudFormation, S3, SQS, DynamoDB, Lambda, IAM, CloudWatch Logs e ECR. Prefira credenciais temporárias de IAM Identity Center:

```bash
aws configure sso --profile inpe-deploy
aws sso login --profile inpe-deploy
export AWS_PROFILE=inpe-deploy
export AWS_DEFAULT_REGION=sa-east-1
aws sts get-caller-identity
```

As políticas de criação de recursos dependem das restrições da sua conta. `ProducerPolicy`, criada pela stack, é somente para a aplicação local e não concede permissões de implantação. A região precisa suportar os serviços e sua conta deve permitir reservar uma execução Lambda. Contas com quota pequena podem precisar de aumento da quota de concorrência antes do deploy.

## 2. Construir e implantar

Na raiz do projeto:

```bash
sam validate --lint --template-file infra/template.yaml
sam build --template-file infra/template.yaml
sam deploy --guided --resolve-image-repos
```

No assistente:

- Stack name: `inpe-h3`.
- AWS Region: `sa-east-1` (ou a região escolhida).
- Parameter H3Resolution: `5`.
- Parameter FrontendOrigin: origem do website; inicialmente `http://localhost:8000`.
- Permitir criação de roles IAM: `Y`.
- Se o SAM solicitar confirmação de acesso público, confira a Function URL com `AuthType: NONE`.
- Salvar configuração: `Y`, para reutilizar em atualizações.

O SAM cria repositórios ECR para as duas imagens. O código não é executado na sua máquina durante o consumo: a Lambda é o consumidor. Depois de alterações, execute novamente `sam build --template-file infra/template.yaml` e `sam deploy`.

Consulte os outputs:

```bash
aws cloudformation describe-stacks --stack-name inpe-h3 \
  --query 'Stacks[0].Outputs' --output table
```

| Output | Uso |
|---|---|
| `BucketName` | `S3_BUCKET` do produtor |
| `QueueUrl` | `SQS_QUEUE_URL` do produtor |
| `TableName` | `TABLE_NAME` do backend local |
| `ProducerPolicyArn` | Política mínima para identidade do produtor |
| `ConsumerFunction` | Nome para logs e verificação da concorrência |
| `DeadLetterQueueUrl` | Diagnóstico e redrive |
| `ApiUrl` | Function URL pública da Lambda de consulta |

## 3. Dar acesso ao produtor local

Crie/use uma identidade dedicada com credenciais temporárias e associe a política do output `ProducerPolicyArn`. Em IAM Identity Center, um administrador pode associar a política gerenciada ao permission set correspondente, conforme a configuração da organização. Para um role IAM convencional, um administrador pode executar:

```bash
aws iam attach-role-policy \
  --role-name SEU_ROLE_DO_PRODUTOR \
  --policy-arn OUTPUT-ProducerPolicyArn
```

Configure o perfil que assume esse role ou o perfil SSO correspondente. O produtor usa a cadeia padrão de credenciais do boto3, sem chaves no código ou no `.env`.

```bash
export AWS_PROFILE=inpe-producer
export AWS_DEFAULT_REGION=sa-east-1
aws sts get-caller-identity
export S3_BUCKET='OUTPUT-BucketName'
export SQS_QUEUE_URL='OUTPUT-QueueUrl'
export RECORDS_PER_MESSAGE=10
export READ_INTERVAL_SECONDS=0.5
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r producer/requirements.txt
python -m producer.main --scrape --start --max-files 6
```

O perfil deve existir antes de executar os comandos. `inpe-producer` é um nome ilustrativo. Para processar todos os arquivos da listagem ainda não processados:

```bash
python -m producer.main --scrape --start
```

Cada execução roda a rotina uma vez; para repetir periodicamente, agende o comando (cron, Agendador de Tarefas). O teste com `--max-files 6` pode retornar zero focos do Brasil se os arquivos recentes estiverem vazios ou contiverem apenas outros países em formatos que informam país. Para uma demonstração com mais dados, amplie a quantidade ou remova o limite.

O arquivo `.env.example` descreve as variáveis, mas não é carregado automaticamente. Exporte-as no shell; `AWS_PROFILE` do produtor deve ter acesso à mesma conta e região da fila e do bucket. Guarde `data/state.sqlite3` para preservar a retomada. Não remova esse diretório no fluxo normal.

## 4. Verificar consumo e persistência

No console, abra a Lambda do output `ConsumerFunction`:

- Trigger: fila FIFO criada pela stack.
- Batch size: **1 mensagem SQS**; dentro dela existem **N observações**, no máximo 10.
- Reserved concurrency: **1**.
- Timeout: 60 segundos; visibilidade SQS: 360 segundos.
- Variáveis: `TABLE_NAME` e `H3_RESOLUTION`.
- Resposta parcial habilitada: `ReportBatchItemFailures`.

A Lambda é invocada pelo poller do serviço quando há mensagens; não é necessário criar um servidor de polling próprio. A [AWS documenta esse acionamento e as reentregas](https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html). A fila usa um único `MessageGroupId`, mantendo o envio sequencial. Não configure `ScalingConfig.MaximumConcurrency=1`: o campo do event source aceita a partir de 2; a limitação desejada fica na concorrência reservada da função e no único grupo FIFO. [Referência de concorrência](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html).

Com o perfil de implantação/observação:

```bash
export AWS_PROFILE=inpe-deploy
sam logs --name Consumer --stack-name inpe-h3 --tail
```

O consumidor registra falhas no CloudWatch. Em DynamoDB → Explore items, procure a partição `RES#5#H3COUNT#ALL` (agregado), `RES#5#H3COUNT#SAT#<satélite>` (um satélite) ou `RES#5#H3COUNT#SATELLITES` (total por satélite). Os itens contêm apenas `pk`, `sk` (`H3#<hash>` ou `SAT#<nome>`) e `count`; não há data nem coordenadas. As partições `EVENT#...` guardam só a chave de deduplicação.

O [uso de transações](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html) impede que uma interrupção grave o contador sem gravar o marcador. A política usa `dynamodb:PutItem` e `dynamodb:UpdateItem`, que autorizam as respectivas operações dentro de `TransactWriteItems`.

## 5. Publicar o mapa estático

O backend é uma Lambda de consulta com Function URL pública, sem Function URL. O frontend é um website estático separado. Configure `frontend/config.js` com o output `ApiUrl` ou execute:

```bash
python3 scripts/build-frontend.py --api-url 'OUTPUT-ApiUrl'
```

Publique `dist/inpe-mapa.zip` no Amplify Hosting e ajuste o parâmetro `FrontendOrigin` da stack para a origem HTTPS do website (sem barra final). Na primeira implantação, o padrão permite `http://localhost:8000` para testes. O guia completo de [publicação do mapa](MAPA_PUBLICO.md) mostra o processo pelo console e a alternativa de preview estático local.

O navegador não utiliza credenciais AWS; a Lambda usa uma execution role com somente `dynamodb:Query` na tabela. O usuário/perfil do produtor permanece exclusivo do processo local de ingestão. Não é necessário criar `inpe-focos-viewer` para acessar o website.

## 6. Diagnóstico e reprocessamento

| Sintoma | Conferir |
|---|---|
| `AccessDenied` no produtor | Perfil ativo, policy anexada, nomes/ARNs dos outputs e região |
| Download falha | DNS/HTTPS do INPE; o script tenta quatro vezes e preserva estado |
| Cabeçalho não reconhecido | CSV real em `data/raw/`, erro de linha nos logs; não descarte o erro sem verificar o formato |
| Fila acumula mensagens | Trigger habilitado, logs Lambda, quota, erros de transação e DLQ |
| Mapa sem dados | Resolução igual à do consumidor e existência de itens `RES#<res>#H3COUNT#ALL` |
| HTTP 503 | Região/tabela/role do backend; logs da função Backend |
| Mapa base não carrega | Acesso do navegador ao CDN jsDelivr |
| Deploy de concorrência falha | Quota da conta e capacidade de reservar 1 execução |

Após cinco tentativas sem sucesso, uma mensagem segue para a DLQ por até 14 dias. Inspecione no console SQS, corrija o consumidor ou a mensagem e use o redrive para a fila original. Mensagens válidas já gravadas não são contadas novamente.

Para reexecutar o dataset sem reenviar normalmente, basta reiniciar a rotina: ela ignora IDs enviados. Para reconstrução intencional, pare o produtor e use outro `DATA_DIR` (isso baixa novamente os arquivos ainda disponíveis), ou limpe os checkpoints `sent` no SQLite com cópia de segurança. Os marcadores no DynamoDB impedem duplicação na mesma resolução; para reconstruir contagens na mesma resolução do zero, use uma nova tabela/stack em vez de apagar somente os agregados.

## 7. Custos e encerramento

Há cobrança potencial de S3, SQS, DynamoDB (incluindo transações e recuperação pontual), Lambda, ECR e hospedagem estática. Polling do mapa gera leituras contínuas enquanto a página está aberta. O bucket é versionado; snapshots e versões podem ser limpos quando não forem mais necessários.

Para encerrar, pare o produtor e remova a stack com `sam delete --stack-name inpe-h3`. O bucket deve estar vazio, inclusive versões e delete markers, para sua remoção. A tabela tem `DeletionPolicy: Retain` e permanece após a stack ser apagada; remova-a manualmente apenas quando não precisar mais dos dados. Verifique também imagens/repositórios ECR e logs restantes.

## Migração das versões com tempo

Reimplante a stack (remove o índice `ObservationsByTime`) e reenvie o dataset seguindo a [rotina de migração no README](../README.md#atualizar-a-partir-das-versões-com-tempo). As chaves novas usam `H3COUNT`; a API ignora os itens antigos (`DAY#...`, `WINDOW10M`). Reiniciar apenas o produtor sem limpar os checkpoints não reenvia observações já enviadas.
