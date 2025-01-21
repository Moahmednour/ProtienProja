import os
import shutil
import json
import pandas as pd

def safe_save_as_textfile(rdd, path):
    """
    Save an RDD to a directory, overwriting existing files if needed.

    Parameters:
        rdd (RDD): The RDD to save.
        path (str): The directory path to save the RDD.
    """
    if os.path.exists(path):
        print(f"The path {path} exists. Removing...")
        shutil.rmtree(path)
    rdd.map(lambda x: json.dumps(x)).saveAsTextFile(path)
    print(f"Files saved to: {path}")

def merge_spark_output_to_json(input_dir, output_file):
    """
    Merge Spark's output files (part files) from a directory into a single JSON file.

    Parameters:
        input_dir (str): Path to the Spark output directory.
        output_file (str): Path to the output JSON file.
    """
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory {input_dir} does not exist.")
    
    # Collect all part files
    part_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.startswith("part-")]
    if not part_files:
        raise FileNotFoundError(f"No part files found in {input_dir}.")

    # Combine all JSON objects into a single list
    combined_data = []
    for part_file in part_files:
        with open(part_file, "r") as f:
            for line in f:
                combined_data.append(json.loads(line))

    # Write combined data to the output file
    with open(output_file, "w") as f:
        json.dump(combined_data, f, indent=4)

    print(f"Merged Spark output from {input_dir} into {output_file}")


def merge_spark_parts(input_dir, output_file, expected_fields=None, header=None):
    """
    Merge Spark output files (part-*) into a single CSV file.

    Parameters:
        input_dir (str): Directory containing the Spark output files.
        output_file (str): Path to the final merged CSV file.
        expected_fields (int): Expected number of fields in each row.
        header (list): Optional header to add to the CSV.
    """
    part_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.startswith("part-")]
    if not part_files:
        raise FileNotFoundError(f"No part-* files found in directory: {input_dir}")

    print(f"Merging {len(part_files)} files from {input_dir} into {output_file}...")

    df_list = []
    for part_file in part_files:
        try:
            # Read each part file with error handling
            df = pd.read_csv(part_file, header=None)

            # Validate the number of fields if expected_fields is provided
            if expected_fields and len(df.columns) != expected_fields:
                print(f"Skipping {part_file}: Expected {expected_fields} fields, found {len(df.columns)}.")
                continue

            df_list.append(df)
        except Exception as e:
            print(f"Error reading {part_file}: {e}")

    # Combine all valid dataframes
    if not df_list:
        raise ValueError(f"No valid data to merge in directory: {input_dir}")
    
    merged_df = pd.concat(df_list, ignore_index=True)

    # Add header if provided
    if header:
        merged_df.columns = header

    # Save the merged dataframe to the output file
    merged_df.to_csv(output_file, index=False)
    print(f"Merged file saved to: {output_file}")
