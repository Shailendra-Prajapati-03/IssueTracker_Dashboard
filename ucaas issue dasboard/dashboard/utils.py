import pandas as pd
import re

def classify_status(comment):
    """
    Classify issue status based on MCM Comments text.
    Returns: 'Fixed', 'Pending', or 'Unknown'
    """
    if not isinstance(comment, str):
        return 'Unknown'
    
    comment = comment.lower().strip()
    
    # Keywords for FIXED status
    fixed_keywords = [
        'fixed', 'resolved', 'closed', 'done', 'completed', 'solved', 
        'working', 'works', 'verified', 'approved', 'passed', 'success',
        'ok', 'okay', 'fine', 'corrected', 'addressed', 'implemented'
    ]
    
    # Keywords for PENDING status
    pending_keywords = [
        'pending', 'open', 'in progress', 'not fixed', 'not resolved',
        'ongoing', 'under review', 'investigating', 'working on it',
        'to be fixed', 'not done', 'incomplete', 'awaiting', 'hold',
        'blocked', 'deferred', 'backlog', 'todo', 'not working',
        'issue remains', 'still present', 'reopen', 'not solved'
    ]
    
    # Check for fixed keywords
    for kw in fixed_keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', comment):
            return 'Fixed'
    
    # Check for pending keywords
    for kw in pending_keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', comment):
            return 'Pending'
    
    return 'Unknown'

def classify_status_detailed(comment):
    if not isinstance(comment, str) or not comment.strip():
        return 'Unknown'
    text = comment.lower()
    fixed_kw = [
        'fixed','resolved','closed','done','completed','solved','verified','approved','passed','success','corrected','addressed','implemented','working fine','works','ok','okay'
    ]
    inprog_kw = [
        'in progress','ongoing','under review','investigating','working on it','wip','progress','testing','test in progress'
    ]
    pending_kw = [
        'pending','open','to be fixed','not fixed','not resolved','awaiting','hold','blocked','deferred','backlog','todo','reopen','not solved','not working'
    ]
    for kw in fixed_kw:
        if re.search(r'\b' + re.escape(kw) + r'\b', text):
            return 'Fixed'
    for kw in inprog_kw:
        if re.search(r'\b' + re.escape(kw) + r'\b', text):
            return 'In Progress'
    for kw in pending_kw:
        if re.search(r'\b' + re.escape(kw) + r'\b', text):
            return 'Pending'
    if text.strip() in ['fixed','resolved','closed','done','completed','solved']:
        return 'Fixed'
    if text.strip() in ['in progress','progress','wip','testing']:
        return 'In Progress'
    if text.strip() in ['pending','open','blocked','hold','deferred','backlog']:
        return 'Pending'
    return 'Unknown'
def map_columns(df_columns):
    """
    Standardize column names based on user rules.
    Returns: (mapped_cols, missing_cols)
    """
    rules = {
        'issue_id': {
            'standard': 'Issue ID',
            'variations': ["issue id", "id", "number", "no", "ticket number", "sr no", "sr. no", "serial number"]
        },
        'component_name': {
            'standard': 'Component Name',
            'variations': ["component name", "component", "section", "module", "area", "feature"]
        },
        'issue_message': {
            'standard': 'Issue Message',
            'variations': ["issue message", "error", "issue", "message", "bug description", "issues", "description"]
        },
        'mcm_comment': {
            'standard': 'MCM Comments',
            'variations': ["mcm comments", "comments", "comment", "remarks", "notes", "status", "fixed", "resolution", "test", "testing status", "mcm status"]
        }
    }
    
    # Optional columns mapping
    optional_rules = {
        'screenshot_url': ['screenshot url', 'screenshot', 'url', 'image', 'photo', 'attachment', 'screenshots/videorec urls'],
        'capanicus_comment': ['capanicus comment', 'capanicus comments', 'dev comment', 'developer comment', 'internal notes', 'dev status', 'copernicus comments', 'copernicus comment'],
        'planned_hours': ['planned hours', 'estimated hours', 'est hours', 'estimated', 'plan'],
        'utilized_hours': ['utilized hours', 'actual hours', 'spent hours', 'actual', 'spent'],
        'resource': ['resource', 'assigned to', 'owner', 'developer', 'assignee', 'person'],
        'priority': ['priority', 'severity', 'level', 'urgency', 'importance']
    }

    mapped_cols = {}
    missing_cols = []

    # Process required columns with stricter matching
    for key, info in rules.items():
        found = False
        # 1. Try exact match first (case-insensitive)
        for actual in df_columns:
            if str(actual).strip().lower() == info['standard'].lower():
                mapped_cols[key] = actual
                found = True
                break
        
        if not found:
            # 2. Try variations - prioritize longer matches to avoid partial hits like "ID" matching "ID Number"
            sorted_vars = sorted(info['variations'], key=len, reverse=True)
            for var in sorted_vars:
                for actual in df_columns:
                    actual_str = str(actual).strip().lower()
                    var_clean = var.lower()
                    if var_clean == actual_str: # Exact variation match
                        mapped_cols[key] = actual
                        found = True
                        break
                if found: break
        
        if not found:
            # 3. Partial match as last resort
            for var in info['variations']:
                for actual in df_columns:
                    if var.lower() in str(actual).lower():
                        mapped_cols[key] = actual
                        found = True
                        break
                if found: break
        
        if not found:
            missing_cols.append(info['standard'])
            mapped_cols[key] = None

    # Process optional columns
    for key, variations in optional_rules.items():
        found = False
        for var in variations:
            for actual in df_columns:
                if var.lower() in actual.strip().lower():
                    mapped_cols[key] = actual
                    found = True
                    break
            if found: break
        if not found:
            mapped_cols[key] = None

    return mapped_cols, missing_cols

def process_file(file_path, selected_sheet=None, connection=None, manual_mapping=None):
    """
    Process file and save issues to database.
    
    Args:
        file_path: Path to the file
        selected_sheet: Specific sheet to process (optional)
        connection: SheetConnection object to link issues (optional)
        manual_mapping: Dictionary of user-selected column mappings (optional)
    """
    all_sheets_data = []
    final_mapping = {}
    try:
        if file_path.endswith(('.xlsx', '.xls')):
            with pd.ExcelFile(file_path) as xls:
                if selected_sheet and selected_sheet in xls.sheet_names:
                    # Process only selected sheet
                    df = pd.read_excel(xls, sheet_name=selected_sheet)
                    mapping = process_dataframe(df, selected_sheet, all_sheets_data, connection, manual_mapping)
                    if mapping: final_mapping.update(mapping)
                else:
                    # Process all sheets by combining them into one DataFrame
                    all_dfs = []
                    for sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name)
                        if not df.empty:
                            # Keep track of original sheet name for each row
                            df['_source_sheet_name'] = sheet_name
                            all_dfs.append(df)
                    
                    if all_dfs:
                        # Combine all sheets
                        combined_df = pd.concat(all_dfs, ignore_index=True)
                        # Process the combined dataframe. We pass 'All Sheets' as default, 
                        # but process_dataframe will use _source_sheet_name
                        mapping = process_dataframe(combined_df, 'All Sheets', all_sheets_data, connection, manual_mapping)
                        if mapping: final_mapping.update(mapping)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            mapping = process_dataframe(df, 'Default', all_sheets_data, connection, manual_mapping)
            if mapping: final_mapping.update(mapping)
        return {'data': all_sheets_data, 'mapping': final_mapping}
    except Exception as e:
        print(f"Error processing file: {e}")
        return {'data': [], 'mapping': {}}

def normalize_dataframe(df, manual_mapping=None):
    """
    Standardize DataFrame columns using auto-detection and manual mapping.
    Renames columns to internal standardized keys.
    Returns: (df, actual_cols, missing_cols)
    """
    actual_cols, missing_cols = map_columns(df.columns)
    
    # If manual mapping is provided, override the auto-detected columns
    if manual_mapping:
        for key, val in manual_mapping.items():
            if val and val in df.columns:
                actual_cols[key] = val
                # If this key was in missing_cols, remove it since it's now mapped
                standard_name = {
                    'issue_id': 'Issue ID',
                    'component_name': 'Component Name',
                    'issue_message': 'Issue Message',
                    'mcm_comment': 'MCM Comments'
                }.get(key)
                if standard_name in missing_cols:
                    missing_cols.remove(standard_name)
    
    # Create a rename map: {original_column_name: standardized_key}
    # We only care about columns we actually found
    rename_map = {v: k for k, v in actual_cols.items() if v}
    
    # Rename the columns in the DataFrame
    return df.rename(columns=rename_map), actual_cols, missing_cols

def process_dataframe(df, sheet_name, all_sheets_data, connection=None, manual_mapping=None):
    """
    Process DataFrame and extract issue data.
    Returns: The final mapped_cols used for this dataframe
    """
    # Normalize the DataFrame first - this renames columns to standardized keys
    df, mapped_cols, missing_cols = normalize_dataframe(df, manual_mapping)
    
    # DEBUG: Print found columns
    print(f"\n=== Processing Sheet: {sheet_name} ===")
    print(f"Original columns: {list(df.columns)}")
    print(f"Mapped columns: {mapped_cols}")
    
    # If required columns are missing, we still want to show the tab if it has any data
    if missing_cols:
        print(f"Sheet {sheet_name} missing columns: {missing_cols}. Attempting to extract what we can.")
    
    # If the sheet is completely empty, we still want it to show up in the UI
    if df.empty:
        issue_data = {
            'sheet_name': sheet_name,
            'issue_id': 'INFO',
            'component_name': 'Empty Sheet',
            'issue_message': 'This sheet does not contain any data rows.',
            'status': 'Unknown',
            'connection': connection
        }
        all_sheets_data.append(issue_data)
        return

    rows_added = 0
    for index, row in df.iterrows():
        # Get standard fields from normalized columns
        issue_val = str(row.get('issue_message', '')).strip() if not pd.isna(row.get('issue_message')) else ""
        mcm_val = str(row.get('mcm_comment', '')).strip() if not pd.isna(row.get('mcm_comment')) else ""
        
        # Determine actual sheet name for this row (important when processing combined dfs)
        actual_sheet_name = str(row.get('_source_sheet_name', sheet_name))
        if pd.isna(row.get('_source_sheet_name')):
            actual_sheet_name = sheet_name
        
        # Skip if both issue and comment are empty
        if not issue_val and not mcm_val:
            continue
        
        # Skip if row is mostly empty (only ID present but no content)
        if issue_val in ['', 'nan', 'None', '-'] and mcm_val in ['', 'nan', 'None', '-']:
            continue
        
        # Classify status based on MCM Comment content
        status = classify_status(mcm_val)
        
        # Get Issue ID - Handle numbers/floats properly
        issue_id = f"ID-{index + 1:04d}"
        raw_id = row.get('issue_id')
        if not pd.isna(raw_id):
            if isinstance(raw_id, float) and raw_id.is_integer():
                issue_id = str(int(raw_id))
            else:
                issue_id = str(raw_id).strip()
        
        # Ensure we have an ID even if mapping exists but value is blank
        if not issue_id or str(issue_id).lower() in ['nan', 'none', '']:
            issue_id = f"ID-{index + 1:04d}"
        
        # Get Component Name
        component = ""
        raw_comp = row.get('component_name')
        if not pd.isna(raw_comp):
            if isinstance(raw_comp, float) and raw_comp.is_integer():
                component = str(int(raw_comp))
            else:
                component = str(raw_comp).strip()
        
        # Standardize numerical fields
        def to_float(val):
            try:
                return float(val) if not pd.isna(val) else 0.0
            except:
                return 0.0

        issue_data = {
            'sheet_name': actual_sheet_name,
            'issue_id': issue_id,
            'component_name': component,
            'issue_message': issue_val or "No message provided",
            'screenshot_url': str(row.get('screenshot_url', '')) if not pd.isna(row.get('screenshot_url')) else "",
            'capanicus_comment': str(row.get('capanicus_comment', '')) if not pd.isna(row.get('capanicus_comment')) else "",
            'mcm_comment': mcm_val,
            'status': status,
            'planned_hours': to_float(row.get('planned_hours')),
            'utilized_hours': to_float(row.get('utilized_hours')),
            'resource': str(row.get('resource', '')) if not pd.isna(row.get('resource')) else "",
            'priority': str(row.get('priority', '')) if not pd.isna(row.get('priority')) else "",
        }
        
        if connection:
            issue_data['connection'] = connection
        
        all_sheets_data.append(issue_data)
        rows_added += 1
    
    # If no rows were added (e.g. all rows were blank), add a placeholder
    if rows_added == 0:
        issue_data = {
            'sheet_name': sheet_name,
            'issue_id': 'INFO',
            'component_name': 'No Data',
            'issue_message': 'This sheet does not contain any issues or records.',
            'status': 'Unknown',
            'connection': connection
        }
        all_sheets_data.append(issue_data)
    
    return mapped_cols
