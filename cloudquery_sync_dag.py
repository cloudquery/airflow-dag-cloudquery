from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models.param import Param
import os
import os
import platform
import tempfile
import requests
import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the absolute path to the directory containing the DAG
# This will be used as a default location for the sync_spec.yml file
dag_path = os.path.dirname(os.path.abspath(__file__))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Adjust the start date as needed
    'retries': 1,  # Number of retries in case of failure
}

# DAG definition
dag = DAG(
    'run_xkcd_to_sqlite_sync',
    default_args=default_args,
    description='A DAG to run CloudQuery sync with sync_spec.yml to sync XKCD data into SQLite',
    schedule_interval=None,
    params={
        'spec_file_path': Param(default=os.path.join(dag_path, 'sync_spec.yml'), type='string', description='Path to the CloudQuery config file'),
        'cloudquery_version': Param(default='v6.4.1', type='string', description='Version of CloudQuery to download')
    }
)

def _get_cloudquery_download_url(version: str) -> str:
    # Detect the host OS and architecture
    system = platform.system().lower()
    arch = platform.machine().lower()

    # Map to the expected values in the download URL
    if system == "darwin":
        system = "darwin"
    elif system == "linux":
        system = "linux"
    elif system == "windows":
        system = "windows"
    else:
        raise ValueError(f"Unsupported system: {system}")

    if arch in ["x86_64", "amd64"]:
        arch = "amd64"
    elif arch in ["aarch64", "arm64"]:
        arch = "arm64"
    else:
        raise ValueError(f"Unsupported architecture: {arch}")
    
    # Construct the download URL
    extension = ".exe" if system == "windows" else ""
    filename = f"cloudquery_{system}_{arch}{extension}"
    url = f"https://github.com/cloudquery/cloudquery/releases/download/cli-{version}/{filename}"
    
    return url

# Task to download the Cloudquery CLI Binary
@task(dag=dag, task_id='download_cloudquery', doc_md="""### Download Cloudquery
This task handles the download and installation of the CloudQuery binary. It requires the version of the CLI, and returns the path where the binary is stored.
""")
def download_cloudquery(version: str) -> str:
    # Use the system's temp folder to store the binary
    target_folder = tempfile.gettempdir()

    # Determine the file path
    extension = ".exe" if platform.system().lower() == "windows" else ""
    file_name = f"cloudquery{extension}"
    file_path = os.path.join(target_folder, file_name)
    
    # Check if the file already exists
    if os.path.exists(file_path):
        logger.info(f"File already exists at {file_path}, skipping download.")
        return file_path
    
    # Get the download URL
    url = _get_cloudquery_download_url(version)
    
    # Download the file
    logger.info(f"Downloading {url} to {file_path}...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    
    # Write the binary to the file
    with open(file_path, "wb") as f:
        f.write(response.content)
    
    # Set executable permissions on Unix-based systems
    if platform.system().lower() != "windows":
        os.chmod(file_path, 0o755)
        logger.info(f"Set executable permissions on {file_path}")

    # Check the version of the downloaded CloudQuery binary
    logger.info("Checking CloudQuery version...")
    version_check_command = f"{file_path} --version"
    version_output = os.popen(version_check_command).read()
    logger.info(f"CloudQuery version: {version_output.strip()}")
    
    logger.info(f"Downloaded to {file_path}")
    return file_path

# Task to run the CloudQuery sync command
@task(dag=dag, task_id='run_xkcd_to_sqlite_sync', doc_md="""### Run XKCD to SQLite Sync
This task runs the CloudQuery sync command using the provided `spec_file_path` parameter to sync XKCD data into a SQLite database.
It uses the path of the CloudQuery binary from the previous task.
""")
def run_xkcd_to_sqlite_sync(spec_file_path: str, cloudquery_path: str):
    result = subprocess.run([cloudquery_path, 'sync', spec_file_path], capture_output=True)
    if result.returncode != 0:
        raise Exception(f"CloudQuery sync failed with return code {result.returncode}. Output: {result.stdout.decode()}")

# Task dependencies
cloudquery_path = download_cloudquery(dag.params["cloudquery_version"])
run_xkcd_to_sqlite_sync(dag.params["spec_file_path"], cloudquery_path)
