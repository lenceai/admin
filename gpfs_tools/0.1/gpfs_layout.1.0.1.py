"""
St. Jude Data Consolidation and Analysis

This script processes and analyzes storage location mapping data from multiple sources.
It consolidates data from different sheets, filters relevant information, and distributes
the data across Cohesity storage systems.
"""

# import numpy as np
import pandas as pd
import logging
from logging import StreamHandler, FileHandler  # Ensure these are imported

# Set up logging to output to both file and console
logging.basicConfig(level=logging.INFO)  # Default to console
logger = logging.getLogger()
file_handler = FileHandler('script.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
stream_handler = StreamHandler()  # For screen output
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

# =============================================================================
# Section 1: Data Loading and Initial Consolidation
# =============================================================================
"""
This section:
- Loads the Excel file containing storage location mapping data
- Reads data from multiple sheets (RDDR01 through RDDR06)
- Combines all sheets into a single consolidated dataframe
"""

# Load the Excel file
file_path = 'DR_Storage_Location_Mapping_V1.0.xlsx'
excel_data = pd.ExcelFile(file_path)

# List of sheet names to consolidate
sheet_names = ['RDDR01', 'RDDR02', 'RDDR03', 'RDDR04', 'RDDR05', 'RDDR06']

# Read and concatenate data from the specified sheets
data_frames = [excel_data.parse(sheet) for sheet in sheet_names]
consolidated_data = pd.concat(data_frames, ignore_index=True)

# Display the last few rows to verify the data
logging.info('Initial consolidated data preview:')
logging.info(consolidated_data.tail().to_string())

logging.info('Section 1 completed: Data loaded and consolidated successfully.')

# =============================================================================
# Section 2: Data Cleaning and Filtering
# =============================================================================
"""
This section:
- Removes rows with missing source paths
- Filters data to only include entries from 'Jude', 'RS1', or 'RS2' source systems
- Saves the cleaned data to a CSV file
"""

# Remove rows with NaN in the 'source system' column
consolidated_data = consolidated_data.dropna(subset=['Source Path'])

# Drop rows that do not have 'Jude', 'RS1', or 'RS2' in the 'Source System' column
consolidated_data = consolidated_data[consolidated_data['Source System'].isin(['Jude', 'RS1', 'RS2'])]

# Save the consolidated data to a CSV file
output_csv_path = 'consolidated_data.csv'
consolidated_data.to_csv(output_csv_path, index=False)

logging.info(f'Consolidated CSV file saved to {output_csv_path}')
logging.info('Section 2 completed: Data cleaned and filtered successfully.')

# =============================================================================
# Section 3: Data Column Selection
# =============================================================================
"""
This section:
- Selects only the essential columns: Source System, Source Path, and Source GB (3/29)
- Saves the filtered dataset to a new CSV file
"""

# Keep only the specified columns
columns_to_keep = ['Source System', 'Source Path', 'Source GB 3/29']
data = consolidated_data[columns_to_keep]

# Save the data to a CSV file
output_csv_path = 'data.csv'
data.to_csv(output_csv_path, index=False)

logging.info(f'Filtered data CSV file saved to {output_csv_path}')
logging.info('Section 3 completed: Columns selected and data saved.')

# =============================================================================
# Section 4: Total Storage Calculation
# =============================================================================
"""
This section:
- Sums up all values in the 'Source GB 3/29' column
- Converts the total from GB to PB (Petabytes)
"""

# Sum the 'Source GB 3/29' column and convert from GB to PB
total_gb = data['Source GB 3/29'].sum()
total_pb = total_gb / 1_000_000

logging.info('Total Storage Calculation:')
logging.info(f'Total GB: {total_gb:,.2f}')
logging.info(f'Total PB: {total_pb:,.2f}')
logging.info('Section 4 completed: Total storage calculated.')

# =============================================================================
# Section 5: Cohesity Storage Distribution
# =============================================================================
"""
This section:
- Creates 13 Cohesity storage groups
- Sorts data by storage size in descending order
- Distributes the data across Cohesity groups, aiming for approximately 5 PB per group
- Saves the distribution layout to a CSV file
"""

# Divide data into 13 names for Cohesity column
names = ['Cohesity_' + f'{i + 1:02d}' for i in range(13)]
data = data.sort_values(by='Source GB 3/29', ascending=False).reset_index(drop=True)
data['Cohesity'] = ''

logging.info('Starting Cohesity distribution: Sorting data and initializing sums.')
cohesity_sums = [0] * len(names)
for idx, row in data.iterrows():
    logging.info(f'Processing row {idx} for distribution.')
    min_index = cohesity_sums.index(min(cohesity_sums))
    data.at[idx, 'Cohesity'] = str(names[min_index])
    cohesity_sums[min_index] += row['Source GB 3/29']
    logging.info(f'Allocated row {idx} to {names[min_index]}. Current sum for that group: {cohesity_sums[min_index]:,.2f} GB')

# Save the grouped data to a CSV file
output_csv_path = 'layout_by_size.csv'
data.to_csv(output_csv_path, index=False)

# Create and save the new sorted DataFrame
sorted_data_by_cluster = data.sort_values(by=['Cohesity', 'Source GB 3/29'], ascending=[True, False])
sorted_data_by_cluster.to_csv('layout_by_cluster.csv', index=False)
logging.info('Section 5: layout_by_cluster.csv saved, sorted by Cohesity group and then by size descending.')

logging.info(f'Grouped data CSV file saved to {output_csv_path}')
logging.info('Section 5 completed: Cohesity distribution performed and saved.')

# =============================================================================
# Section 6: Storage Distribution Summary
# =============================================================================
"""
This section:
- Groups the data by Cohesity storage system
- Calculates the total storage allocated to each Cohesity system
- Converts the storage values from GB to PB
- Saves the summary to a CSV file
"""

# Group by 'Cohesity' and sum 'Source GB 3/29', then convert to PB
grouped_data = data.groupby('Cohesity')['Source GB 3/29'].sum().reset_index()
grouped_data['Source PB'] = grouped_data['Source GB 3/29'] / 1_000_000

# Save the grouped data to a CSV file
output_csv_path = 'grouped_data.csv'
grouped_data.to_csv(output_csv_path, index=False)

logging.info(f'Grouped data CSV file saved to {output_csv_path}')
logging.info('Section 6 completed: Storage distribution summary generated and saved.') 