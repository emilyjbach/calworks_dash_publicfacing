import streamlit as st
import pandas as pd
import plotly.express as px

# Configuration
st.set_page_config(page_title="CalWORKs Dashboard (Multi-Year/Multi-County)", layout="wide")

# Define the list of data files and the dictionary file
DATA_FILES = [
    'FinalData-Table 1.csv',
    'Data_External-Table 2.csv',
    'Data_External-Table 1.csv',
    'Data_External-Table 3.csv',
    'FinalData-Table 4.csv',
    'FinalData-Table 5.csv',
    'FinalData-Table 6.csv',
    'Data_External-Table 7.csv',
]

DICT_FILE = 'DataDictionary-Table 1.csv'

# Use st.cache_data for faster performance, only runs once
@st.cache_data
def load_data(data_files_list, dict_file):
    
    # --- 1. Load Main Data (Handles Multiple Files) ---
    df_list = []
    for DATA_FILE in data_files_list:
        try:
            # Data starts at row 5 (header=4)
            temp_df = pd.read_csv(DATA_FILE, header=4)
            df_list.append(temp_df)
        except FileNotFoundError:
            print(f"Warning: Data file '{DATA_FILE}' not found. Skipping.")
            continue
        except Exception as e:
            print(f"Error reading file {DATA_FILE}: {e}")
            continue

    if not df_list:
        raise FileNotFoundError("No data files were successfully loaded. Check your DATA_FILES list.")
        
    # Concatenate all dataframes into one
    df = pd.concat(df_list, ignore_index=True)
    df.drop_duplicates(inplace=True)
    df = df.dropna(subset=['County Name', 'Report Month'])
    
    # --- Chronological Sort Fix ---
    # Convert Report Month (e.g., "Sep 2025") to datetime object for correct sorting
    df['Report Date'] = pd.to_datetime(df['Report Month'], errors='coerce')
    # Remove rows where date parsing failed
    df.dropna(subset=['Report Date'], inplace=True)
    
    # --- 2. Load Dictionary Data ---
    df_dict = pd.read_csv(dict_file, skiprows=1)
    
    # Correctly handle 5 columns (Cell, Part, Item, Column, Unused)
    df_dict.columns = ['Cell', 'Part', 'Item', 'Column', 'Unused']
    
    # Clean dictionary
    df_dict = df_dict.dropna(subset=['Cell'])
    df_dict['Cell'] = df_dict['Cell'].astype(float).astype(int).astype(str)
    
    # Create human-readable labels
    df_dict['Label'] = df_dict['Part'].str.strip() + " | " + df_dict['Item'].str.strip()
    mask = df_dict['Column'].notna()
    df_dict.loc[mask, 'Label'] = df_dict['Label'] + " | " + df_dict['Column'].astype(str).str.strip()
    
    mapping = dict(zip(df_dict['Cell'], df_dict['Label']))
    
    # --- 3. Clean Numeric Columns ---
    numeric_cols = [c for c in df.columns if str(c).isdigit()]
    
    for col in numeric_cols:
        # Replace suppressed data ('*') with '0' with '0', remove commas, convert to numeric
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(',', '').str.replace('*', '0'), 
            errors='coerce'
        ).fillna(0)
        
    # The function returns the main dataframe with the new 'Report Date' column
    return df, mapping, numeric_cols

# --- Application Starts Here ---
try:
    df, mapping, numeric_cols = load_data(DATA_FILES, DICT_FILE)
except FileNotFoundError as e:
    st.error(f"Error: {e}. Please ensure all files listed in DATA_FILES AND the dictionary file are in the script directory.")
    st.stop()
except Exception as e:
    st.error(f"An unexpected error occurred during data processing: {e}")
    st.stop()


st.title("CalWORKs Caseload Movement Interactive Dashboard")
st.sidebar.header("Filters")

# --- 1. County Filter (Multi-select) ---
counties = sorted(df['County Name'].unique())
try:
    # Default to Statewide if present, otherwise the first county
    default_county_selection = ['Statewide'] if 'Statewide' in counties else [counties[0]]
except IndexError:
    st.error("No counties found in the data.")
    st.stop()

selected_counties = st.sidebar.multiselect(
    "1. Select Counties (1 or more)", 
    counties, 
    default=default_county_selection
)

if not selected_counties:
    st.warning("Please select at least one county to visualize.")
    st.stop()

# --- 2. Metric Filter (Multi-select) ---
metric_options = {mapping.get(c, f"Cell {c}"): c for c in numeric_cols if c in mapping}
metric_labels = list(metric_options.keys())

# Default selection: Applications received (Cell 4) and Cases open (Cell 54)
default_metric_selection = [label for label, id in metric_options.items() if id in ['4', '54'] and label in metric_labels]

selected_metric_labels = st.sidebar.multiselect(
    "2. Select Metrics (1 or more)", 
    metric_labels,
    default=default_metric_selection
)

if not selected_metric_labels:
    st.warning("Please select at least one metric to visualize.")
    st.stop()

selected_metric_ids = [metric_options[label] for label in selected_metric_labels]

# --- Data Filtering and Sorting ---
# Filter by ALL selected counties
filtered_df = df[df['County Name'].isin(selected_counties)].copy() 

# --- Sorting for chronological X-axis ---
# We sort by the new 'Report Date' (datetime object) for chronological order
filtered_df.sort_values('Report Date', inplace=True)

# --- Line Chart Visualization ---
st.subheader(f"Metrics Trend for: {', '.join(selected_counties)}")

if filtered_df.empty:
    st.warning(f"No data found for the selected filters.")
else:
    plot_df = filtered_df.copy()
    
    # Select only the relevant columns (Month, County Name, and selected metric IDs)
    plot_df = plot_df[['Report Month', 'County Name'] + selected_metric_ids]
    
    # Rename columns in the plotting dataframe for human-readable legend labels
    rename_mapping = {id: label for id, label in zip(selected_metric_ids, selected_metric_labels)}
    plot_df.rename(columns=rename_mapping, inplace=True)
    
    # Melt (unpivot) the data for Plotly to draw multiple lines correctly
    plot_df_melted = plot_df.melt(
        id_vars=['Report Month', 'County Name'], 
        value_vars=selected_metric_labels, 
        var_name='Metric', 
        value_name='Count'
    )
    
    # Create a unique line identifier column combining County and Metric
    plot_df_melted['Line Identifier'] = plot_df_melted['County Name'] + " - " + plot_df_melted['Metric']

    # Plotly Line Chart
    fig = px.line(
        plot_df_melted, 
        x='Report Month', # x-axis uses the original month string for clean labels
        y='Count', 
        color='Line Identifier', 
        title=f"Caseload Movement Comparison",
        markers=True, 
        template="plotly_white",
        height=600 # Set a fixed height for a bigger chart
    )
    
    # MOVED LEGEND TO THE BOTTOM AND ADJUSTED 'y' FOR MORE CLEARANCE
    fig.update_layout(
        yaxis_title="Count",
        legend_title="County & Metric",
        hovermode="x unified",
        legend=dict(
            orientation="h", # Horizontal legend
            yanchor="bottom",
            y=-0.35, # Adjusted position further below the chart area
            xanchor="center",
            x=0.5 # Center the legend
        )
    )

    st.plotly_chart(fig, use_container_width=True)

    # Data Table
    st.subheader("Underlying Data")
    st.dataframe(plot_df_melted)
