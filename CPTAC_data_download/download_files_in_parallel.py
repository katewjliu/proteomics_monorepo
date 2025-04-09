import requests
import json
import csv
import hashlib
import os
from concurrent.futures import ThreadPoolExecutor

# PDC API endpoint
url = "https://pdc.cancer.gov/graphql"

# Fetch studies with version information
def fetch_study_catalog(acceptDUA):
    query = f"""
    {{
        studyCatalog(acceptDUA: {str(acceptDUA).lower()}) {{
            pdc_study_id
            versions {{
                study_id
            }}
        }}
    }}
    """
    response = requests.get(url, params={"query": query})
    return response.json()['data']['studyCatalog']

# Fetch files per study_id
def fetch_files_per_study(study_id):
    query = f"""
    {{
        filesPerStudy(study_id: "{study_id}") {{
            study_id
            pdc_study_id
            file_id
            file_name
            file_size
            md5sum
            signedUrl {{
                url
            }}
        }}
    }}
    """
    response = requests.get(url, params={"query": query})
    return response.json()['data']['filesPerStudy']

# Loop over each study_id and fetch file information
def get_all_files_from_studies(study_ids):
    all_files = []
    for study_id in study_ids:
        files = fetch_files_per_study(study_id)
        print(study_id, len(files)) # print all study IDs and number of files <-- slow step
        all_files.extend(files)
    return all_files

# Function to generate MD5 checksum from raw data
def generate_md5_from_data(data):
    md5_hash = hashlib.md5()
    md5_hash.update(data)
    return md5_hash.hexdigest()

# Function to download a single file and perform necessary operations
def download_and_process_file(file, csv_file):
    study_id = file['study_id'], 
    pdc_study_id = file['pdc_study_id']
    file_id = file['file_id']
    file_name = file['file_name']
    download_url = file['signedUrl']['url']
    file_size = file['file_size']
    md5sum = file['md5sum']

    #print(f"File ID: {file_id}, File Name: {file_name}, File Size: {file_size}, Download URL: {download_url}")
    
    # Download the file
    download_response = requests.get(download_url)

    # Define your subdirectory and file name
    subdirectory = "1000_smallest_folder_in_parallel_rename"
    combined_file_name = f"{pdc_study_id}_{file_name}"  # Combine pdc_study_id and file_name
    file_path = os.path.join(subdirectory, combined_file_name)
    
    # Check if the file already exists, and if so, append an index
    index = 1
    base_name, extension = os.path.splitext(file_name) # split ext e.g. .txt from file name, and append index before ext
    while os.path.exists(file_path):
        combined_file_name = f"{pdc_study_id}_{base_name}_{index}{extension}"
        file_path = os.path.join(subdirectory, combined_file_name)
        index += 1

    # Ensure the subdirectory exists
    os.makedirs(subdirectory, exist_ok=True)

    # Write the file content
    with open(file_path, 'wb') as f:
        file_content = download_response.content
        f.write(file_content)
    print(f"Downloaded {combined_file_name}")

    # Generate md5 checksum from downloaded file content
    generated_md5 = generate_md5_from_data(file_content)

    # Append file info to CSV
    with open(csv_file, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['study_id', 'pdc_study_id', "file_id", "file_name", "file_size", 'md5sum', 'generated_md5sum', 'download_url'])
        writer.writerow({
            'study_id': study_id,
            'pdc_study_id': pdc_study_id,
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "md5sum": md5sum,
            'generated_md5sum': generated_md5,
            "download_url": download_url
        })

# Function to process files in parallel
def download_files_in_parallel(smallest_files, csv_file, max_workers=4):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_and_process_file, file, csv_file) for file in smallest_files]
        for future in futures:
            future.result()  # Ensures that any raised exceptions are handled

# Main execution
if __name__ == "__main__":
    # Step 1: Fetch study catalog
    acceptDUA = True  # Set to True or False based on whether you accept DUA
    study_catalog = fetch_study_catalog(acceptDUA)

    # Step 2: Process the studies and gather study IDs
    study_id_list = []
    for study in study_catalog:
        pdc_study_id = study['pdc_study_id']
        for version in study['versions']:
            study_id_list.append(version['study_id'])
    
    # Step 3: Get all files from the studies
    all_files = get_all_files_from_studies(study_id_list)

    # Step 4: Sort all files by size
    files_sorted = sorted(all_files, key=lambda x: int(x['file_size']))

    # Step 5: Write sorted files to CSV
    csv_all_sorted_files = "all_files_sorted_20241016.csv"
    with open(csv_all_sorted_files, mode='w', newline='') as csv_file:
        fieldnames = ['study_id', 'pdc_study_id', 'file_id', 'file_name', 'file_size', 'md5sum', 'signedUrl']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for file_data in files_sorted:
            writer.writerow(file_data)

    # Step 6: Get the 1000 smallest files
    smallest_files = files_sorted[:1000]

    # Step 7: Prepare CSV for smallest files and initiate parallel download
    csv_file = "1000_smallest_files.csv"
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['study_id', 'pdc_study_id', "file_id", "file_name", "file_size", 'md5sum', 'generated_md5sum', 'download_url'])
        writer.writeheader()

    # Step 8: Download smallest files in parallel
    download_files_in_parallel(smallest_files, csv_file, max_workers=4)
