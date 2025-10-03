import os
import subprocess
import logging

logger = logging.getLogger(__name__)

def run_soda_check(schema: str) -> bool:
    env = os.environ.copy()
    env.update({
        'SCHEMA': schema,
        # d’autres variables si besoin (connexion, etc.)
    })
    cmd = [
        'soda', 'scan',
        '-d', "pg_datasource",
        '-c', '/usr/local/airflow/include/soda/configuration.yml',
        '-v', f'SCHEMA={schema}',
        '/usr/local/airflow/include/soda/check.yml'
    ]
    logger.info("Executing Soda command: %s", " ".join(cmd))
    result = subprocess.run(
        cmd, env=env, capture_output=True, text=True
    )
    if result.stdout:
        logger.info("Soda STDOUT: %s", result.stdout)
    if result.stderr:
        logger.error("Soda STDERR: %s", result.stderr)
    if result.returncode != 0:
        raise Exception(f"Soda scan failed with code {result.returncode}")
    logger.info(f"Soda check successful for schema {schema}")
    return True
