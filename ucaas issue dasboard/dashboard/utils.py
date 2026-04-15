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

def process_file(file_path, selected_sheet=None, connection=None):
    """
    Process file and save issues to database.
    
    Args:
        file_path: Path to the file
        selected_sheet: Specific sheet to process (optional)
        connection: SheetConnection object to link issues (optional)
    """
    all_sheets_data = []
    try:
        if file_path.endswith(('.xlsx', '.xls')):
            with pd.ExcelFile(file_path) as xls:
                if selected_sheet and selected_sheet in xls.sheet_names:
                    # Process only selected sheet
                    df = pd.read_excel(xls, sheet_name=selected_sheet)
                    process_dataframe(df, selected_sheet, all_sheets_data, connection)
                else:
                    # Process all sheets
                    for sheet_name in xls.sheet_names:
                        df = pd.read_excel(xls, sheet_name=sheet_name)
                        process_dataframe(df, sheet_name, all_sheets_data, connection)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            process_dataframe(df, 'Default', all_sheets_data, connection)
        return all_sheets_data
    except Exception as e:
        print(f"Error processing file: {e}")
        return []

def process_dataframe(df, sheet_name, all_sheets_data, connection=None):
    """
    Process DataFrame and extract issue data.
    """
    actual_cols, missing_cols = map_columns(df.columns)
    
    # DEBUG: Print found columns
    print(f"\n=== Processing Sheet: {sheet_name} ===")
    print(f"Available columns: {list(df.columns)}")
    print(f"Mapped columns: {actual_cols}")
    
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
        # Skip blank rows - check if issue_message or mcm_comment is empty
        issue_val = ""
        if actual_cols['issue_message'] and not pd.isna(row[actual_cols['issue_message']]):
            issue_val = str(row[actual_cols['issue_message']]).strip()
        
        mcm_val = ""
        if actual_cols['mcm_comment'] and not pd.isna(row[actual_cols['mcm_comment']]):
            mcm_val = str(row[actual_cols['mcm_comment']]).strip()
        
        # Skip if both issue and comment are empty
        if not issue_val and not mcm_val:
            continue
        
        # Skip if row is mostly empty (only ID present but no content)
        if issue_val in ['', 'nan', 'None', '-'] and mcm_val in ['', 'nan', 'None', '-']:
            continue
        
        # Get MCM Comment - used ONLY for status classification
        mcm_text = mcm_val
        
        # Classify status based on MCM Comment content
        status = classify_status(mcm_text)
        
        # Get Issue ID - Handle numbers/floats properly
        issue_id = f"ID-{index + 1:04d}"
        if actual_cols['issue_id'] and not pd.isna(row[actual_cols['issue_id']]):
            val = row[actual_cols['issue_id']]
            if isinstance(val, float) and val.is_integer():
                issue_id = str(int(val))
            else:
                issue_id = str(val).strip()
        
        # Get Component Name
        component = ""
        if actual_cols['component_name'] and not pd.isna(row[actual_cols['component_name']]):
            val = row[actual_cols['component_name']]
            if isinstance(val, float) and val.is_integer():
                component = str(int(val))
            else:
                component = str(val).strip()
        
        # CRITICAL: Get Issue Message from "Issues" column - THIS IS DISPLAYED IN UI
        issue_msg = "No message provided"
        if actual_cols['issue_message'] and not pd.isna(row[actual_cols['issue_message']]):
            val = row[actual_cols['issue_message']]
            if isinstance(val, float) and val.is_integer():
                issue_msg = str(int(val))
            else:
                issue_msg = str(val).strip()
        
        # Get Screenshot URL
        screenshot = ""
        if actual_cols['screenshot_url'] and not pd.isna(row[actual_cols['screenshot_url']]):
            screenshot = str(row[actual_cols['screenshot_url']])
        
        # Get Capanicus Comment
        capanicus = ""
        if actual_cols['capanicus_comment'] and not pd.isna(row[actual_cols['capanicus_comment']]):
            capanicus = str(row[actual_cols['capanicus_comment']])
        
        # Get Planned Hours
        planned_hrs = 0
        if actual_cols['planned_hours'] and not pd.isna(row[actual_cols['planned_hours']]):
            try:
                planned_hrs = float(row[actual_cols['planned_hours']])
            except (ValueError, TypeError):
                planned_hrs = 0
        
        # Get Utilized Hours
        utilized_hrs = 0
        if actual_cols['utilized_hours'] and not pd.isna(row[actual_cols['utilized_hours']]):
            try:
                utilized_hrs = float(row[actual_cols['utilized_hours']])
            except (ValueError, TypeError):
                utilized_hrs = 0
        
        # Get Resource
        resource_name = ""
        if actual_cols['resource'] and not pd.isna(row[actual_cols['resource']]):
            resource_name = str(row[actual_cols['resource']])
        
        # Get Priority
        priority_val = ""
        if actual_cols['priority'] and not pd.isna(row[actual_cols['priority']]):
            priority_val = str(row[actual_cols['priority']])
            
        issue_data = {
            'sheet_name': sheet_name,
            'issue_id': issue_id,
            'component_name': component,
            'issue_message': issue_msg,      # From "Issues" column - DISPLAYED IN UI
            'screenshot_url': screenshot,
            'capanicus_comment': capanicus,
            'mcm_comment': mcm_text,          # From "MCM Comments" column - STATUS ONLY
            'status': status,
            'planned_hours': planned_hrs,
            'utilized_hours': utilized_hrs,
            'resource': resource_name,
            'priority': priority_val,
        }
        
        # Add connection if provided
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
