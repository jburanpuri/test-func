# KEY_VAULT_URL
# SQL_SERVER_URL
# SQL_DATABASE_NAME
# CREATE ENV VARIABLES FOR ABOVE

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


@app.function_name(name='loop_leadIds')
@app.route(route='loop_leadIds', methods=[func.HttpMethod.GET])
def loop_lead_ids(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(
        'Manual trigger: looping through SF_Leads_Pending_Conversion.')

    try:
        username, password = get_db_credentials()

        connection_string = (
            'DRIVER={ODBC Driver 18 for SQL Server};'
            f'SERVER=tcp:{os.environ["SQL_SERVER_URL"]};'
            'PORT=1433;'
            f'DATABASE={os.environ["SQL_DATABASE_NAME"]};'
            f'UID={username};PWD={password};'
            'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
        )

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT SF_LeadId FROM JOBS.SF_Leads_Pending_Conversion")
            rows = cursor.fetchall()

            logging.info(f"Found {len(rows)} leads.")
            for (lead_id,) in rows:
                logging.info(f"LeadId: {lead_id}")

        return func.HttpResponse("Lead ID loop completed.", status_code=200)

    except Exception as e:
        logging.error(f"Error looping through leads: {e}")
        return func.HttpResponse("Error during lead loop.", status_code=500)
