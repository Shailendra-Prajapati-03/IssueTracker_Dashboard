"""
Services for syncing data from live sheets (Google Sheets, Excel Online).
"""
import requests
import re
from django.utils import timezone
from .models import Issue
from .utils import classify_status


def extract_google_sheet_id(url):
    """Extract sheet ID from Google Sheets URL."""
    # Pattern for: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit
    pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    return None


def sync_google_sheets(connection):
    """
    Sync data from Google Sheets.
    Downloads the entire workbook as XLSX to support multiple tabs.
    """
    sheet_id = extract_google_sheet_id(connection.sheet_url)
    if not sheet_id:
        raise ValueError("Invalid Google Sheets URL")
    
    # Save sheet_id to connection
    connection.sheet_id = sheet_id
    
    try:
        # Download as XLSX to get ALL sheets (tabs)
        xlsx_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
        
        response = requests.get(xlsx_url, timeout=60)
        
        if response.status_code == 200:
            import io
            import pandas as pd
            from .utils import process_dataframe
            
            # Use a BytesIO object to read the XLSX data
            xlsx_data = io.BytesIO(response.content)
            
            # Clear old issues for this connection
            Issue.objects.filter(connection=connection).delete()
            
            all_sheets_data = []
            
            with pd.ExcelFile(xlsx_data) as xls:
                # Process ALL sheets (tabs) to ensure they are all visible in the UI
                for sheet_name in xls.sheet_names:
                    # Skip empty or internal sheets if any
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    process_dataframe(df, sheet_name, all_sheets_data, connection)
            
            # Save all issues from all tabs
            issues_to_create = [Issue(**data) for data in all_sheets_data]
            Issue.objects.bulk_create(issues_to_create)
            
            connection.last_sync = timezone.now()
            connection.save()
            
            return {'success': True, 'count': len(all_sheets_data), 'sheets': len(all_sheets_data)}
        elif response.status_code in [401, 403]:
            raise Exception(
                "Access Denied: Your Google Sheet is private. "
                "Please follow these steps: 1. Open your Sheet, 2. Click 'Share', "
                "3. Change access to 'Anyone with the link', 4. Set role to 'Viewer'."
            )
        else:
            raise Exception(f"Failed to fetch sheet: HTTP {response.status_code}")
            
    except Exception as e:
        raise Exception(f"Error syncing Google Sheets: {str(e)}")


def sync_excel_online(connection):
    """
    Sync data from Excel Online (OneDrive/SharePoint).
    Note: Requires Microsoft Graph API integration.
    """
    # For now, return a message that this needs API setup
    # Full implementation would use Microsoft Graph API
    raise NotImplementedError(
        "Excel Online sync requires Microsoft Graph API setup. "
        "Please use file upload for now."
    )


def sync_sheet_data(connection):
    """
    Main sync function that routes to appropriate service.
    """
    if connection.connection_type == 'google_sheets':
        return sync_google_sheets(connection)
    elif connection.connection_type == 'excel_online':
        return sync_excel_online(connection)
    elif connection.connection_type == 'upload':
        # For uploads, re-process the file
        if connection.uploaded_file:
            from .utils import process_file
            Issue.objects.filter(connection=connection).delete()
            all_sheets_data = process_file(connection.uploaded_file.path, connection=connection)
            
            # Save all issues from all tabs
            issues_to_create = [Issue(**data) for data in all_sheets_data]
            Issue.objects.bulk_create(issues_to_create)
            
            connection.last_sync = timezone.now()
            connection.save()
            return {'success': True, 'message': f'File re-synced ({len(all_sheets_data)} issues)'}
    else:
        raise ValueError(f"Unknown connection type: {connection.connection_type}")


def check_for_updates(connection):
    """
    Check if sheet has new data since last sync.
    Returns True if updates are available.
    """
    # For now, always return True to trigger sync
    # In production, this would compare sheet metadata/ETag
    if not connection.last_sync:
        return True
    
    # Check if more than 5 minutes since last sync
    from datetime import timedelta
    time_since_sync = timezone.now() - connection.last_sync
    return time_since_sync > timedelta(minutes=5)
