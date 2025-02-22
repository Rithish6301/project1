import time
import boto3

dynamodb = boto3.client('dynamodb', region_name="us-east-1")

def wait_for_table_to_be_active(table_name):
    while True:
        response = dynamodb.describe_table(TableName=table_name)
        table_status = response['Table']['TableStatus']
        print(f"Table {table_name} status: {table_status}")
        if table_status == 'ACTIVE':
            print(f"Table {table_name} is now ACTIVE.")
            break
        time.sleep(5)

def create_acc_master_table():
    try:
        response = dynamodb.create_table(
            TableName="team6-acc_master",
            AttributeDefinitions=[
                {
                    'AttributeName': 'acc_no',
                    'AttributeType': 'N'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'acc_no',
                    'KeyType': 'HASH'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Created team6-acc_master table:", response)
        wait_for_table_to_be_active('team6-acc_master')
    except Exception as e:
        print(f"Error creating team6-acc_master table: {e}")

def insert_acc_master_items():
    items = [
        {"acc_no": {"N": "1"}, "acc_name": {"S": "furniture_sale_amt"}, "acc_desc": {"S": "furniture sales"}, "acc_type": {"S": "income"}},
        {"acc_no": {"N": "4"}, "acc_name": {"S": "vat"}, "acc_desc": {"S": "vat"}, "acc_type": {"S": "expense"}},
        {"acc_no": {"N": "6"}, "acc_name": {"S": "excise_duty"}, "acc_desc": {"S": "excise duties"}, "acc_type": {"S": "expense"}},
        {"acc_no": {"N": "2"}, "acc_name": {"S": "tools_sale_amt"}, "acc_desc": {"S": "tools sales"}, "acc_type": {"S": "income"}},
        {"acc_no": {"N": "3"}, "acc_name": {"S": "gst"}, "acc_desc": {"S": "goods and services tax"}, "acc_type": {"S": "expense"}},
        {"acc_no": {"N": "5"}, "acc_name": {"S": "custom_duty"}, "acc_desc": {"S": "custom duty"}, "acc_type": {"S": "expense"}}
    ]
    for item in items:
        try:
            response = dynamodb.put_item(TableName='team6-acc_master', Item=item)
            print(f"Inserted item: {item['acc_name']['S']}")
        except Exception as e:
            print(f"Error inserting item {item['acc_name']['S']}: {e}")

def create_ledger_txn_table():
    try:
        response = dynamodb.create_table(
            TableName='ledger_txn_t6',
            AttributeDefinitions=[
                {
                    'AttributeName': 'txn_id',
                    'AttributeType': 'N'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'txn_id',
                    'KeyType': 'HASH'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print("Created ledger_txn_t6 table:", response)
        wait_for_table_to_be_active('ledger_txn_t6')
    except Exception as e:
        print(f"Error creating ledger_txn_t6 table: {e}")

def create_src_sys_mst_table():
    try:
        response = dynamodb.create_table(
            TableName='src_sys_mst_t6',
            AttributeDefinitions=[
                {
                    'AttributeName': 'system_id',
                    'AttributeType': 'N'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'system_id',
                    'KeyType': 'HASH'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Created src_sys_mst_t6 table:", response)
        wait_for_table_to_be_active('src_sys_mst_t6')
    except Exception as e:
        print(f"Error creating src_sys_mst_t6 table: {e}")

def insert_src_sys_mst_items():
    items = [
        {"system_id": {"N": "1"}, "system_name": {"S": "Power Tools Shop"}},
        {"system_id": {"N": "2"}, "system_name": {"S": "Furniture Shop"}},
        {"system_id": {"N": "3"}, "system_name": {"S": "Back Office System"}},
        {"system_id": {"N": "4"}, "system_name": {"S": "E-Commerce Website"}}
    ]
    for item in items:
        try:
            response = dynamodb.put_item(TableName='src_sys_mst_t6', Item=item)
            print(f"Inserted item: {item['system_name']['S']}")
        except Exception as e:
            print(f"Error inserting item {item['system_name']['S']}: {e}")

# Main Execution
create_acc_master_table()
create_ledger_txn_table()
insert_acc_master_items()
create_src_sys_mst_table()
insert_src_sys_mst_items()