#!/usr/bin/env python3

import os
import pandas as pd
import re

def create_csv_from_excel(excel_file, output_dir):
    """
    Create CSV files from each tab in the Excel file that matches the pattern 'Cluster-\\d+-IP-Registry'
    
    Args:
        excel_file (str): Path to the Excel file
        output_dir (str): Directory to save the CSV files
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    # Read the Excel file
    xl = pd.ExcelFile(excel_file)
    
    # Get all sheet names
    sheet_names = xl.sheet_names
    
    # Pattern for matching cluster tabs
    pattern = re.compile(r'Cluster-(\d+)-IP-Registry')
    
    # Process each sheet
    processed_count = 0
    all_nodes_data = []  # List to store all dataframes for combining later
    for sheet_name in sheet_names:
        match = pattern.match(sheet_name)
        if match:
            try:
                cluster_num = match.group(1)
                output_file = f"cluster-{cluster_num.zfill(2)}.csv"  # Zero-pad the number
                output_path = os.path.join(output_dir, output_file)
                
                # Read the sheet without headers first to find the data structure
                df_raw = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                
                # Find the header row (contains 'Device', 'Rack', etc.)
                header_row = None
                for i, row in df_raw.iterrows():
                    if pd.notna(row[1]) and str(row[1]).strip() == 'Device':
                        header_row = i
                        break
                
                if header_row is None:
                    print(f"Warning: Could not find header row in {sheet_name}")
                    continue
                
                # Read the data starting from the row after headers, using columns 1-7 or 1-8 depending on availability
                data_start_row = header_row + 1
                # Check how many columns are available in this sheet
                max_cols = len(df_raw.columns)
                end_col = min(9, max_cols)  # Use columns 1 to 8 if available, otherwise up to max
                df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=data_start_row, usecols=range(1, end_col), header=None)
                
                # Set proper column names
                column_names = ['Device', 'Rack', 'Physical Hostname', 'ILo Hostname', 
                               'Mgmt / ILO', 'Host IP', 'VIP', 'Serial Number']
                df.columns = column_names[:len(df.columns)]
                
                # Filter out rows with empty or incomplete data
                # Keep only rows where Device column contains "Cohesity Cluster" (the actual device entries)
                df_filtered = df[df['Device'].astype(str).str.contains('Cohesity Cluster', na=False)]
                
                # Add username and password columns with default values
                df_filtered = df_filtered.copy()  # Create a copy to avoid SettingWithCopyWarning
                df_filtered['username'] = 'remote'
                df_filtered['password'] = 'hpeonly1'
                
                # Add cluster name column for the combined CSV (only include clusters 1-13)
                if int(cluster_num) <= 13:
                    df_for_all = df_filtered.copy()
                    df_for_all['Cluster'] = f"Cluster-{cluster_num}"
                    all_nodes_data.append(df_for_all)
                
                # Save as CSV
                df_filtered.to_csv(output_path, index=False)
                print(f"Created {output_path}")
                processed_count += 1
            
            except Exception as e:
                print(f"Error processing {sheet_name}: {str(e)}")
                continue
    
    # Create the combined all-nodes.csv file
    if all_nodes_data:
        combined_df = pd.concat(all_nodes_data, ignore_index=True)
        all_nodes_path = os.path.join(output_dir, "all-nodes.csv")
        combined_df.to_csv(all_nodes_path, index=False)
        print(f"Created {all_nodes_path} with {len(combined_df)} total nodes")
    
    return processed_count

if __name__ == "__main__":
    excel_file = "master_ip_list.xlsx"
    output_dir = "data"
    
    if not os.path.exists(excel_file):
        print(f"Error: Excel file '{excel_file}' not found.")
        exit(1)
    
    processed = create_csv_from_excel(excel_file, output_dir)
    print(f"Processed {processed} tabs and created {processed} CSV files in the '{output_dir}' directory.") 