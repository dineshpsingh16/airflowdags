# Project Directory Structure

    wheel_dag_new/
        ├── util/
        │   ├── __init__.py
        │   ├── tasks.py
        ├── dist/
        ├── dag_with_xcom_csv_pdf_email.py
        ├── setup.py
        ├── requirements.txt
        └── install_dag.py

# Environment Setup

    pip install wheel

# Build Command

    cd wheel_dag_new
    python setup.py bdist_wheel

# Install command

    python setup.py install

