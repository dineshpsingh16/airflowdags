import os
import shutil

def copy_dag_files():
    # Current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Determine the parent directory name
    parent_dir_name = os.path.basename(current_dir)

    # Destination directory
    destination_dir = os.path.abspath(os.path.join(current_dir, '../../dags', parent_dir_name))

    # Ensure the destination directory exists
    os.makedirs(destination_dir, exist_ok=True)

    # Copy the dist folder
    dist_src = os.path.join(current_dir, 'dist')
    dist_dest = os.path.join(destination_dir, 'dist')
    if os.path.exists(dist_src):
        shutil.copytree(dist_src, dist_dest, dirs_exist_ok=True)

    
    # Copy DAG files
    for filename in os.listdir(current_dir):
        if filename.endswith('.py') and filename != 'setup.py' and filename != 'install_dag.py':
            shutil.copy(os.path.join(current_dir, filename), destination_dir)
        elif filename == 'requirements.txt':
            shutil.copy(os.path.join(current_dir, filename), destination_dir)

    print(f"Copied DAG files and dist folder to {destination_dir}")

if __name__ == "__main__":
    copy_dag_files()
