import os
import logging
import requests
import json
import time

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def fetch_all_data(api_base_url, resources, templates_dict, out_path, **kwargs):
    """
    Fetch all data for multiple resources from an API and save as JSONL.

    Args:
        api_base_url (str): The base URL of the API.
        resources (list): List of resources to fetch (e.g., ['episodes', 'characters']).
        templates_dict (dict): Dictionary containing template variables like 'ds_nodash'.
        out_path (str): Path to save the output files.
    """
    ds_nodash = templates_dict['ds_nodash']

    for res in resources:
        # Generate output file path
        output_template = os.path.join(out_path, ds_nodash, f'{res}.jsonl')
        os.makedirs(os.path.dirname(output_template), exist_ok=True)

        # Log the resource being fetched
        logging.info(f'Pulling {res} from API')

        # Construct the API URL
        full_url = f"{api_base_url}{res}/"
        os.environ['NO_PROXY'] = full_url

        print(f"Requesting: {full_url}")

        try:
            # Make the API request
            resp = requests.get(full_url, timeout=10)
            if resp.status_code != 200:
                logging.error(f"Failed to fetch {res}: {resp.status_code}")
                continue

            # Parse the response JSON
            out = resp.json()
            if not out:  # Stop if no data is returned
                logging.info(f"No data found for {res}")
                continue

            # Write the data to the output file
            with open(output_template, 'w') as f:
                for row in out:
                    if row is None:
                        logging.warning(f"Empty row encountered for {res}")
                    else:
                        f.write(json.dumps(row) + '\n')

                # Pause to avoid breaking API connection
                time.sleep(1)

        except Exception as e:
            logging.error(f"Error fetching {res}: {e}")

def disk_to_s3(
    s3_conn_id: str,
    local_path: str,
    base_dir: str,
    bucket: str,
    delete_local: bool = False,
):
    """
    Uploads files from a local directory to an S3 bucket.
    """
    s3_hook = S3Hook(s3_conn_id)
    uploaded_keys = []

    for root, _, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)

            if os.path.getsize(full_path) == 0:
                logging.warning(f"Skipping empty file: {full_path}")
                continue

            s3_key = full_path.replace(base_dir + "/", "")

            try:
                s3_hook.load_file(
                    filename=full_path,
                    key=s3_key,
                    bucket_name=bucket,
                    replace=True,
                )
                logging.info(f"Uploaded {full_path} to s3://{bucket}/{s3_key}")
                uploaded_keys.append(s3_key)
            except Exception as e:
                logging.error(f"Failed to upload {full_path} to S3: {e}")
                continue

            if delete_local:
                os.remove(full_path)
                logging.info(f"Deleted local file: {full_path}")

    if not uploaded_keys:
        raise AirflowException("No files were successfully uploaded to S3.")

    return uploaded_keys

def s3_to_snowflake(snowflake_conn_id,
                        schema,
                        table,
                        stage,
                        s3_key=None,
                        **context
                        ):                          
    if s3_key is None:
        s3_key = context['templates_dict']['s3_key']

    # if s3_key still none, skip
    if s3_key is None or s3_key == 'None':
        logging.info('No rows returned')
        raise AirflowSkipException


    copy_stmt = """
    copy into raw.{table}
    from (
    select *
    from @raw.{schema}.{stage}/{s3_key} t)
    force = true,
    file_format = csv_tab_delim,
    on_error = 'continue';
    """.format(schema=schema, table=table, stage=stage, s3_key=s3_key)

    hook = SnowflakeHook(snowflake_conn_id)
    conn = hook.get_conn()

    # make these not prints
    logging.info('Executing COPY command')

    with conn.cursor() as cur:
        cur.execute(copy_stmt)
        result = cur.fetchone()

        logging.info('COPY complete')

    if result.get('errors_seen', 0) > 0:
        # log warning, slack?
        logging.warning('Errors in load')
    
    logging.info(result)