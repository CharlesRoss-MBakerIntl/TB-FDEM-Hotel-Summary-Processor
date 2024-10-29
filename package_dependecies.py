import os
import subprocess
import shutil
import tarfile
import zipfile
import time

def create_dependencies_folder(dependencies_folder):
    if not os.path.exists(dependencies_folder):
        os.makedirs(dependencies_folder)

def download_and_extract_packages(requirements_file, dependencies_folder):
    create_dependencies_folder(dependencies_folder)
    with open(requirements_file, 'r') as file:
        packages = [line.strip() for line in file if line.strip()]

    for package in packages:
        subprocess.run(['pip', 'download', '--no-binary', ':all:', package, '-d', dependencies_folder])

    for item in os.listdir(dependencies_folder):
        if item.endswith('.tar.gz'):
            file_path = os.path.join(dependencies_folder, item)
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(path=dependencies_folder)
            os.remove(file_path)
        elif item.endswith('.zip'):
            file_path = os.path.join(dependencies_folder, item)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(dependencies_folder)
            os.remove(file_path)

def copy_python_files_to_dependencies(dependencies_folder):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for item in os.listdir(current_dir):
        if item.endswith('.py'):
            shutil.copy(item, dependencies_folder)

def zip_dependencies_folder(dependencies_folder):
    zip_file_path = f'{dependencies_folder}.zip'
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(dependencies_folder):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, dependencies_folder)
                info = zipfile.ZipInfo(arcname)
                info.date_time = time.localtime(time.time())[:6]
                with open(file_path, 'rb') as fp:
                    zipf.writestr(info, fp.read())
    return zip_file_path

def delete_dependencies_folder(dependencies_folder):
    shutil.rmtree(dependencies_folder)

if __name__ == "__main__":
    requirements_file = 'requirements.txt'
    lambda_folder = 'FDEM-Hotel-Summaries-Lambda'

    download_and_extract_packages(requirements_file, lambda_folder)
    copy_python_files_to_dependencies(lambda_folder)
    zip_file_path = zip_dependencies_folder(lambda_folder)
    delete_dependencies_folder(lambda_folder)

    print(f'Packages have been downloaded, extracted, and added to {lambda_folder}')
    print(f'The folder has been zipped into {zip_file_path} and deleted.')
