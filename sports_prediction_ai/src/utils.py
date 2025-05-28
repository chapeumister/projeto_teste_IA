import pandas as pd
import re

def to_snake_case(name: str) -> str:
    # Helper for normalise_columns, can be kept simple if normalise_columns handles most cases
    if not isinstance(name, str):
        return name
    name = re.sub(r'([A-Z])([A-Z]+)$', r'\1_\2', name) 
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name) 
    name = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', name)
    return name.lower().replace(' ', '_').replace('-', '_').replace('.', '_')

def normalise_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Normalises DataFrame column names:
    - Strips leading/trailing whitespace
    - Converts to lowercase
    - Replaces spaces and other common separators with underscores
    - Removes any character that is not alphanumeric or underscore
    '''
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")

    new_columns = []
    for col in df.columns:
        if not isinstance(col, str):
            new_columns.append(col) # Keep non-string columns as is
            continue
        
        normalized_col = str(col).strip().lower()
        normalized_col = normalized_col.replace(" ", "_").replace("-", "_").replace(".", "_")
        # Remove any character that is not alphanumeric or underscore
        normalized_col = re.sub(r'[^\w_]', '', normalized_col)
        # Ensure it's a valid identifier (e.g. doesn't start with a number after cleaning)
        if normalized_col and normalized_col[0].isdigit():
            normalized_col = '_' + normalized_col
        new_columns.append(normalized_col if normalized_col else to_snake_case(str(col))) # Fallback to basic snake_case if empty

    df.columns = new_columns
    return df

# You can also include the more detailed to_snake_case function here if preferred
# and call it from normalise_dataframe_columns or use the simpler direct approach above.
