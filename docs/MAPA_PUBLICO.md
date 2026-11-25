# Mapa estático com Lambda Function URL pública

> **Modelo atual:** o banco guarda apenas hash H3 + contagem, sem tempo. O mapa mostra o total acumulado por célula; o único filtro é o de satélite (agregado ou um satélite).

Arquitetura: **website estático → HTTPS Function URL → Lambda de consulta → DynamoDB**. O produtor continua na sua máquina. Não é necessário usuário IAM local para abrir o mapa. A Lambda utiliza uma execution role com leitura somente da tabela. O frontend não contém chaves AWS. Não há API Gateway nem servidor permanente.

A implementação HTTP é `backend/handler.py`; a consulta está em `backend/query.py`. O formato de resposta continua sendo GeoJSON paginado. O mapa atualiza os dados a cada cinco segundos após concluir a consulta anterior. Isso é polling, não WebSocket; novas observações dependem da execução do produtor e da atualização da fonte INPE.

## 1. Criar a role da Lambda de consulta

No console **IAM → Funções → Criar função**, selecione serviço AWS e caso de uso **Lambda**. Associe `AWSLambdaBasicExecutionRole` e nomeie `inpe-focos-api-role`.

Abra a role → Adicionar permissões → Criar política em linha → JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "dynamodb:Query",
    "Resource": "arn:aws:dynamodb:sa-east-1:053109836747:table/inpe-focos-clusters"
  }]
}
```

Nome da política: `inpe-focos-api-read`. Essa role não precisa de S3, SQS, Scan, índices nem gravação no banco. Se a tabela ainda tiver o índice `ObservationsByTime` de versões anteriores, ele pode ser removido no console (DynamoDB → Índices). Não utilize a role do consumidor para esta função.

## 2. Preparar o ZIP no CloudShell

Use o arquivo `artifacts/inpe-api-source.zip` fornecido junto ao projeto. Ele é um **pacote de fontes**, e não o ZIP final da Lambda. Pelo CloudShell → Actions → Upload file, envie esse arquivo. Execute:

```bash
unzip -o ~/inpe-api-source.zip -d ~/inpe-api-build
bash ~/inpe-api-build/scripts/build-backend-zip.sh
```

O script instala boto3 e H3 para **Python 3.12/x86_64**, com wheels Linux, e gera:

```text
/home/cloudshell-user/inpe-api-build/dist/inpe-api.zip
```

Use Actions → Download file para baixar esse ZIP final. A geração precisa de acesso ao PyPI. O pacote final inclui `backend/handler.py`, `backend/query.py`, `common/` e as dependências; não inclui FastAPI, frontend ou servidor Uvicorn.

## 3. Criar a Lambda

Na região **São Paulo / sa-east-1**, crie uma Lambda do zero:

| Campo | Valor |
|---|---|
| Nome | `inpe-focos-api` |
| Runtime | Python 3.12 |
| Arquitetura | x86_64 |
| Role existente | `inpe-focos-api-role` |
| Memória | 256 MB |
| Timeout | 15 segundos |
| Handler | `backend.handler.handler` |
| Variável `TABLE_NAME` | `inpe-focos-clusters` |
| Variável `H3_RESOLUTION` | `5` |

Envie `inpe-api.zip` em Code → Upload from → .zip file e ajuste o handler em Runtime settings. Essa Lambda não tem gatilho SQS.

Você pode testar a consulta no console com:

```json
{
  "version": "2.0",
  "rawPath": "/api/clusters",
  "requestContext": {"http": {"method": "GET"}},
  "queryStringParameters": {"day": "2026-11-13", "limit": "10"}
}
```

O resultado deve ter `statusCode: 200`, mesmo se `features` estiver vazio. `503` indica problema de acesso ao DynamoDB; verifique tabela, região, role e logs. O endpoint `/api/health` confirma que o código responde, mas não testa acesso ao banco.

## 4. Criar a Function URL pública

Lambda → Configuration → Function URL → Create function URL:

- Auth type: **NONE**.
- Invoke mode: **BUFFERED**.
- Habilite CORS.
- Allow origins inicial: `http://localhost:8000`.
- Allow methods: **GET**.
- Allow headers: `Accept`.
- Allow credentials: desabilitado.

Salve e copie a Function URL, semelhante a `https://ID.lambda-url.sa-east-1.on.aws/`. O console normalmente adiciona as permissões públicas ao criar uma URL NONE. Em Configuration → Permissions, confira os dois statements necessários: `lambda:InvokeFunctionUrl` condicionado a `FunctionUrlAuthType=NONE` e `lambda:InvokeFunction` condicionado a `InvokedViaFunctionUrl=true`.

Se a URL responder `403` por falta desses statements, um administrador pode adicioná-los pelo CloudShell (execute uma vez; IDs já existentes devem ser inspecionados antes de repetir):

```bash
aws lambda add-permission --function-name inpe-focos-api \
  --statement-id PublicFunctionUrl --action lambda:InvokeFunctionUrl \
  --principal '*' --function-url-auth-type NONE --region sa-east-1

aws lambda add-permission --function-name inpe-focos-api \
  --statement-id PublicInvocationViaUrl --action lambda:InvokeFunction \
  --principal '*' --invoked-via-function-url --region sa-east-1
```

A API é pública: CORS controla o acesso por navegadores, mas não autentica chamadas. O código responde somente a GET e expõe apenas os agregados, sem endpoints de escrita ou leitura dos itens de observações individuais. Invocações e consultas podem gerar cobrança.

Abra a URL seguida de `/api/health` e depois `/api/clusters`. Parâmetros disponíveis: `satellite=GOES-19`, `limit=500` e `cursor`; `day`, `window`, `view`, `minutes` e `as_of` retornam 400. A paginação limita cada resposta a 1000 itens. O frontend segue `next_cursor` até concluir a consulta.

## 5. Configurar o frontend estático

No CloudShell com os arquivos já extraídos, execute substituindo pela Function URL real:

```bash
cd ~/inpe-api-build
python3 scripts/build-frontend.py \
  --api-url 'https://ID.lambda-url.sa-east-1.on.aws/'
```

Isso gera `dist/inpe-mapa.zip` com `index.html`, `style.css`, `app.js` e `config.js` diretamente na raiz. Baixe-o pelo menu Actions → Download file:

```text
/home/cloudshell-user/inpe-api-build/dist/inpe-mapa.zip
```

O arquivo `config.js` contém apenas a URL pública e a frequência de atualização, nunca credenciais. Para usar os fontes diretamente, configure `frontend/config.js`:

```js
window.INPE_CONFIG = {
  apiBaseUrl: 'https://ID.lambda-url.sa-east-1.on.aws',
  refreshIntervalMs: 5000
};
```

Para uma prévia local, execute `python3 -m http.server 8000 --directory frontend` na sua máquina e abra `http://localhost:8000`. Esse servidor serve somente arquivos e não usa credenciais AWS. O domínio/origem deve corresponder ao CORS: `localhost` e `127.0.0.1` são origens distintas.

## 6. Publicar o website no Amplify Hosting

No console AWS Amplify, crie um app com implantação manual (**Deploy without Git**, ou opção equivalente de upload):

1. Nome do app: `inpe-focos-mapa`.
2. Nome do branch: `main`.
3. Escolha upload de arquivo e envie **`inpe-mapa.zip`**.
4. Salve e implante. Não é necessário executar build de framework.
5. Copie a URL HTTPS do app retornada pelo Amplify.

Volte à Function URL → Edit CORS → adicione essa origem HTTPS **sem barra final nem caminho**, por exemplo `https://main.ID.amplifyapp.com`. Você pode manter também `http://localhost:8000` durante o desenvolvimento. Salve e abra o website. O bucket `inpe-focos-italan-2026-bucket` continua privado e dedicado aos datasets; o Amplify hospeda os arquivos do mapa separadamente.

Por padrão o mapa mostra todo o histórico. Ative o filtro para exibir apenas os últimos N minutos. Se a API retorna dados em outra aba, mas o mapa falha ao consultar, confira a URL em `config.js` e a origem exata configurada no CORS. Se a API retorna `features: []`, confira ingestão, dia UTC e resolução; isso não é falha de hospedagem.

## Infraestrutura automatizada

`infra/template.yaml` também foi adaptado: Backend usa a mesma imagem de consulta, cria Function URL NONE, CORS e as duas permissões públicas. O output é `ApiUrl`; configure o parâmetro `FrontendOrigin` com a origem do website. O frontend estático é publicado separadamente conforme acima.

**Como os recursos desta conta foram criados manualmente, não aplique a stack completa apenas para publicar o mapa**: ela criaria outro pipeline. Siga este guia para reutilizar a tabela existente. A implantação SAM completa serve para novas instalações.

Referências: [Function URLs e CORS](https://docs.aws.amazon.com/lambda/latest/dg/urls-configuration.html), [permissões públicas](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html), [upload manual no Amplify](https://docs.aws.amazon.com/amplify/latest/userguide/manual-deploys.html).
