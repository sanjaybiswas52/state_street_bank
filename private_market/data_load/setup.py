from setuptools import setup, find_packages
import pathlib

from pathlib import Path
from setuptools import setup

here = pathlib.Path(__file__).parent.resolve()

setup(
    name="pipeline_package",
    version="0.1.0",
    description="Private Markets Performance data pipeline for Databricks",
    author="PFPM Team",
    packages=find_packages(include=["pipeline_package*"]),
    python_requires=">=3.9, <4",
    include_package_data=True,
    package_data={"pipeline_package": ["**/*"]},
    entry_points={
        'scripts': [
            'Run_Client_Onboarding=pipeline_package.bronze.run_client_onboarding:create_dir',
            'Delete_Folder=pipeline_package.bronze.delete_folder:delete_folder',
            'Load_Lookup_Tables_Bronze=pipeline_package.bronze.load_bronze_lookup_tables:lookup_data_ingestion',
            'Truncate_Tables_Bronze=pipeline_package.bronze.truncate_tables:truncate_tables',
            'Load_Raw_Tables_Bronze=pipeline_package.bronze.load_bronze_raw_tables:lookup_data_ingestion',
            'Metadata_Framework_Source_to_Bronze_Data_Ingestion=pipeline_package.bronze.metadata_framework_source_to_bronze_data_ingestion',
            'Load_Silver_Entity_Master=pipeline_package.silver.load_silver_entity_master:silver_entity_master',
            'Load_Silver_GL_Activity_Investment=pipeline_package.silver.load_silver_gl_activity_investment:silver_gl_activity_investment',
        ]
    }
)
