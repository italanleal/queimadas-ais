from pathlib import Path

import pytest
yaml = pytest.importorskip('yaml')


class CloudFormationLoader(yaml.SafeLoader):
    pass


def tagged(loader, tag, node):
    if isinstance(node, yaml.ScalarNode):
        return {tag: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {tag: loader.construct_sequence(node)}
    return {tag: loader.construct_mapping(node)}


CloudFormationLoader.add_multi_constructor('!', tagged)


def test_queue_consumer_and_iam_configuration():
    template = yaml.load(Path('infra/template.yaml').read_text(), Loader=CloudFormationLoader)
    resources = template['Resources']
    consumer = resources['Consumer']['Properties']
    assert consumer['ReservedConcurrentExecutions'] == 1
    event = consumer['Events']['Queue']['Properties']
    assert event['BatchSize'] == 1
    assert event['FunctionResponseTypes'] == ['ReportBatchItemFailures']
    assert 'ScalingConfig' not in event
    assert resources['InputQueue']['Properties']['FifoQueue'] is True
    assert resources['InputQueue']['Properties']['VisibilityTimeout'] >= 6 * consumer['Timeout']
    actions = consumer['Policies'][-1]['Statement'][0]['Action']
    assert set(actions) == {'dynamodb:PutItem', 'dynamodb:UpdateItem'}
    assert resources['DataTable']['DeletionPolicy'] == 'Retain'


def test_public_function_url_and_read_only_backend():
    resources = yaml.load(Path('infra/template.yaml').read_text(), Loader=CloudFormationLoader)['Resources']
    assert not any(r['Type'] == 'AWS::Serverless::HttpApi' for r in resources.values())
    backend = resources['Backend']['Properties']
    assert 'Events' not in backend
    assert backend['Policies'][-1]['Statement'][0]['Action'] == 'dynamodb:Query'
    assert resources['BackendUrl']['Properties']['AuthType'] == 'NONE'
    assert resources['BackendUrl']['Properties']['Cors']['AllowMethods'] == ['GET']
    assert resources['BackendUrlPermission']['Properties']['Action'] == 'lambda:InvokeFunctionUrl'
    permission = resources['BackendInvokePermission']['Properties']
    assert permission['Action'] == 'lambda:InvokeFunction'
    assert permission['InvokedViaFunctionUrl'] is True


def test_table_without_time_index_and_backend_scoped_to_table():
    text = Path('infra/template.yaml').read_text()
    resources = yaml.load(text, Loader=CloudFormationLoader)['Resources']
    table = resources['DataTable']['Properties']
    assert 'GlobalSecondaryIndexes' not in table
    assert [a['AttributeName'] for a in table['AttributeDefinitions']] == ['pk', 'sk']
    assert 'ObservationsByTime' not in text
    statement = resources['Backend']['Properties']['Policies'][-1]['Statement'][0]
    assert statement['Action'] == 'dynamodb:Query'
    assert statement['Resource'] == {'GetAtt': 'DataTable.Arn'}
