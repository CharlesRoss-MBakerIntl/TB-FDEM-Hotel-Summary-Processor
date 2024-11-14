import os
import subprocess
import shutil
import tarfile
import zipfile
import time

def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

def download_and_extract_packages(requirements_file, dependencies_folder):
    create_folder(dependencies_folder)
    with open(requirements_file, 'r') as file:
        packages = [line.strip() for line in file if line.strip()]

    for package in packages:
        subprocess.run(['pip', 'download', '--no-binary', ':all:', package, '-d', dependencies_folder])

    for item in os.listdir(dependencies_folder):
        file_path = os.path.join(dependencies_folder, item)
        if item.endswith('.tar.gz'):
            with tarfile.open(file_path, 'r:gz') as tar:
                tar.extractall(path=dependencies_folder)
            os.remove(file_path)
        elif item.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(dependencies_folder)
            os.remove(file_path)

def copy_python_files(lambda_folder):
    create_folder(lambda_folder)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    for item in os.listdir(current_dir):
        if item.endswith('.py'):
            shutil.copy(item, lambda_folder)

def zip_folder(folder_path, zip_file_path):
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                info = zipfile.ZipInfo(arcname)
                info.date_time = time.localtime(time.time())[:6]
                with open(file_path, 'rb') as fp:
                    zipf.writestr(info, fp.read())

def main():
    requirements_file = 'requirements.txt'
    lambda_folder = 'FDEM-Hotel-Summaries-Lambda'
    dependencies_folder = 'dependencies'  # Separate dependencies folder

    download_and_extract_packages(requirements_file, dependencies_folder)
    copy_python_files(lambda_folder)
    
    zip_folder(dependencies_folder, f'{dependencies_folder}.zip')
    zip_folder(lambda_folder, f'{lambda_folder}.zip')

    shutil.rmtree(dependencies_folder)
    shutil.rmtree(lambda_folder)

    print(f'Packages have been downloaded, extracted, and added to {dependencies_folder}')
    print(f'The folders have been zipped into {dependencies_folder}.zip and {lambda_folder}.zip and deleted.')

if __name__ == "__main__":
    main()
