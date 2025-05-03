import logging
import azure.functions as func
import pyodbc
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def get_db_credentials():
    key_vault_url = os.environ['KEY_VAULT_URL']
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    username = client.get_secret('azuresqlbiagentuser').value
    password = client.get_secret('azuresqlbiagentpass').value
    return username, password

@app.function_name(name='test_db_connection')
@app.route(route='test_db_connection', methods=[func.HttpMethod.GET])
def test_db_connection(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Testing DB connection.')

    try:
        username, password = get_db_credentials()

        connection_string = (
            'DRIVER={ODBC Driver 18 for SQL Server};'
            f'SERVER=tcp:{os.environ["SQL_SERVER_URL"]};'
            'PORT=1433;'
            f'DATABASE={os.environ["SQL_DATABASE_NAME"]};'
            f'UID={username};'
            f'PWD={password};'
            'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        )

        with pyodbc.connect(connection_string) as conn:
            return func.HttpResponse("Database connection successful.", status_code=200)

    except Exception as e:
        logging.error(f'Database connection failed: {e}')
        return func.HttpResponse("Database connection failed.", status_code=500)
