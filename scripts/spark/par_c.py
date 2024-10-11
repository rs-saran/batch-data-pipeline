import os
import pyarrow.parquet as pq

# Define source and destination top folders
source_folder = '/Users/sais1/Desktop/github_practice/batch-data-pipeline/data/clickstream_data_e'
destination_folder = '/Users/sais1/Desktop/github_practice/batch-data-pipeline/data/clickstream_data'

filesl = []
for (root,dirs,files) in os.walk(source_folder, topdown=True):
        # print (root)
        # print (dirs)
        # print (files)
        filesl.append(files)
filesl2 = []
for (root,dirs,files) in os.walk(destination_folder, topdown=True):
        # print (root)
        # print (dirs)
        # print (files)
        filesl2.append(files)
#         print ('--------------------------------')
print(len(filesl),len(filesl2))
# # Traverse the directory structure
# for root, dirs, files in os.walk(source_folder):
#     print (root)
#     print (dirs)
#     print (files)
#     for file in files:
#         if file.endswith('.parquet'):
#             # Full path to the source file
#             source_file = os.path.join(root, file)
            
#             # Generate the corresponding path in the destination folder
#             relative_path = os.path.relpath(root, source_folder)
#             destination_dir = os.path.join(destination_folder, relative_path)
            
#             # Create the destination directory if it doesn't exist
#             os.makedirs(destination_dir, exist_ok=True)
            
#             # Define the destination file path
#             destination_file = os.path.join(destination_dir, file)
            
#             # Process and write the parquet file
#             try:
#                 table = pq.read_table(source_file)
#                 pq.write_table(table, destination_file, coerce_timestamps='us', allow_truncated_timestamps=True)
#                 print(f"Processed {source_file} to {destination_file}")
#             except:
#                 print(f"Skipped {source_file}")
                
