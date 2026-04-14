from django.shortcuts import render, redirect
from django.db import models
from django.db.models import Count
from django.contrib import messages
from .models import Issue, CSVUpload, SheetConnection
from .forms import CSVUploadForm, SheetConnectionForm
from .utils import process_file
from django.core.paginator import Paginator
import json
import os
import csv
from django.http import HttpResponse, JsonResponse
from django.db.models.functions import TruncDate
from django.utils import timezone
from .utils import classify_status_detailed
import pandas as pd

def get_chart_stats(sheet_name=None, connection=None):
    all_issues = Issue.objects.all()
    if connection:
        all_issues = all_issues.filter(connection=connection)
    if sheet_name:
        all_issues = all_issues.filter(sheet_name=sheet_name)
    all_issues = all_issues.order_by('-created_at')
    
    total_count = all_issues.count()
    fixed_count = all_issues.filter(status='Fixed').count()
    pending_count = all_issues.filter(status='Pending').count()
    unknown_count = total_count - fixed_count - pending_count
    
    completion_rate = round((fixed_count / total_count * 100), 1) if total_count > 0 else 0
    fixed_pct = round((fixed_count / total_count * 100), 1) if total_count > 0 else 0
    pending_pct = round((pending_count / total_count * 100), 1) if total_count > 0 else 0
    
    # Component stats with more details
    # Filter out status values that were mistakenly saved as component names
    excluded_values = ['Unknown', 'Pending', 'Fixed', 'Feature', '']
    component_stats = all_issues.exclude(
        component_name__in=excluded_values
    ).exclude(
        component_name__isnull=True
    ).values('component_name').annotate(
        total=Count('id'),
        fixed=Count('id', filter=models.Q(status='Fixed')),
        pending=Count('id', filter=models.Q(status='Pending')),
    ).order_by('-total')[:15]
    
    comp_labels = [c['component_name'] for c in component_stats]
    comp_total = [c['total'] for c in component_stats]
    comp_fixed = [c['fixed'] for c in component_stats]
    comp_pending = [c['pending'] for c in component_stats]
    
    # Calculate completion percentage for each component
    comp_completion = []
    for c in component_stats:
        pct = round((c['fixed'] / c['total'] * 100), 1) if c['total'] > 0 else 0
        comp_completion.append(pct)

    # Status distribution for donut chart
    status_stats = all_issues.values('status').annotate(count=Count('id'))
    status_labels = [s['status'] for s in status_stats]
    status_values = [s['count'] for s in status_stats]
    
    # Priority distribution (if available)
    priority_stats = all_issues.exclude(priority__isnull=True).exclude(priority='').values('priority').annotate(count=Count('id'))
    priority_labels = [p['priority'] for p in priority_stats]
    priority_values = [p['count'] for p in priority_stats]
    
    # Component stats - group by component_name only
    # Filter out blank, None, and status values that were mistakenly saved as component names
    excluded_components = ['', None, 'Fixed', 'Pending', 'Unknown', 'Feature', 'Improvement']
    resource_stats = all_issues.exclude(
        component_name__in=excluded_components
    ).exclude(
        component_name__isnull=True
    ).exclude(
        component_name__exact=''
    ).values('component_name').annotate(
        total=Count('id'),
        fixed=Count('id', filter=models.Q(status='Fixed')),
        pending=Count('id', filter=models.Q(status='Pending')),
    ).order_by('-total')[:20]
    
    total_resources = all_issues.values('component_name').distinct().count()
    
    # Calculate total utilized hours
    total_utilized_hours = all_issues.aggregate(total=models.Sum('utilized_hours', default=0))['total'] or 0
    total_planned_hours = all_issues.aggregate(total=models.Sum('planned_hours', default=0))['total'] or 0
    
    return {
        'labels': status_labels,
        'values': status_values,
        'completion_rate': completion_rate,
        'fixed_pct': fixed_pct,
        'pending_pct': pending_pct,
        'comp_labels': comp_labels,
        'comp_total': comp_total,
        'comp_fixed': comp_fixed,
        'comp_pending': comp_pending,
        'comp_completion': comp_completion,
        'priority_labels': priority_labels,
        'priority_values': priority_values,
        'total_resources': total_resources,
        'total_count': total_count,
        'fixed_count': fixed_count,
        'pending_count': pending_count,
        'unknown_count': unknown_count,
        'total_utilized_hours': round(total_utilized_hours, 1),
        'total_planned_hours': round(total_planned_hours, 1),
        'resource_stats': list(resource_stats),
    }

def dashboard(request):
    sheet_name = request.GET.get('sheet', None)
    connection_id = request.GET.get('connection', None)
    status_filter = request.GET.get('status')
    search_query = request.GET.get('search', '')
    
    # Get current connection if specified, otherwise use latest upload
    current_connection = None
    if connection_id:
        try:
            current_connection = SheetConnection.objects.get(id=connection_id)
        except SheetConnection.DoesNotExist:
            pass
    
    # If no connection specified, use the latest uploaded file connection
    if not current_connection:
        current_connection = SheetConnection.objects.filter(
            connection_type='upload'
        ).order_by('-created_at').first()
    
    # SYNC LIVE DATA before rendering if it's a live connection
    if current_connection:
        from .services import sync_sheet_data
        try:
            # For live connections, sync on every page load to ensure "live" data
            # For uploads, we can also sync to ensure DB matches file on disk
            sync_sheet_data(current_connection)
        except Exception as e:
            print(f"Initial sync error: {e}")
            # Continue with existing DB data if sync fails
    
    # Get issues for current connection only
    if current_connection:
        all_issues = Issue.objects.filter(connection=current_connection).order_by('-created_at')
    else:
        all_issues = Issue.objects.all().order_by('-created_at')
    
    # Get unique sheet names for current connection only - try to get from file if it's an upload
    sheet_names = []
    if current_connection:
        if current_connection.connection_type == 'upload' and current_connection.uploaded_file:
            try:
                with pd.ExcelFile(current_connection.uploaded_file.path) as xls:
                    sheet_names = sorted(xls.sheet_names)
            except:
                sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
                sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
        elif current_connection.connection_type in ['google_sheets', 'excel_online']:
            # For live sheets, we have to rely on synced issues' sheet names
            sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
            sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
        else:
            sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
            sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
    
    sheet_names = list(dict.fromkeys(sheet_names))

    # If sheet_name is empty string, treat as "All Sheets" (no filter)
    if sheet_name == '':
        sheet_name = None

    # Filter by sheet if specified
    active_sheet = sheet_name
    if active_sheet:
        # Note: we keep all_issues unfiltered here because it's used for issues_by_sheet
        # But for other logic we might want a filtered version
        pass
    
    # Default: show all sheets combined (no sheet filter)
    # User can select a specific sheet from dropdown

    # Group issues by sheet name
    issues_by_sheet = {}
    for sn in sheet_names:
        issues_list = all_issues.filter(sheet_name=sn)
        if status_filter:
            issues_list = issues_list.filter(status=status_filter)
        if search_query:
            issues_list = issues_list.filter(
                models.Q(issue_message__icontains=search_query) |
                models.Q(component_name__icontains=search_query) |
                models.Q(issue_id__icontains=search_query)
            )
        issues_by_sheet[sn] = issues_list
    
    if 'export' in request.GET:
        export_issues = all_issues
        if sheet_name:
            export_issues = all_issues.filter(sheet_name=sheet_name)
        if status_filter:
            export_issues = export_issues.filter(status=status_filter)
        if search_query:
            export_issues = export_issues.filter(
                models.Q(issue_message__icontains=search_query) |
                models.Q(component_name__icontains=search_query) |
                models.Q(issue_id__icontains=search_query)
            )
        return export_issues_csv(export_issues)
                          
    stats = get_chart_stats(sheet_name, current_connection)
        
    # Paginator for the active sheet
    # If sheet_name is None/empty, show all issues combined
    if sheet_name:
        active_sheet_issues = issues_by_sheet.get(sheet_name, [])
    else:
        # All sheets combined
        active_sheet_issues = all_issues
        if status_filter:
            active_sheet_issues = active_sheet_issues.filter(status=status_filter)
        if search_query:
            active_sheet_issues = active_sheet_issues.filter(
                models.Q(issue_message__icontains=search_query) |
                models.Q(component_name__icontains=search_query) |
                models.Q(issue_id__icontains=search_query)
            )
    paginator = Paginator(active_sheet_issues, 15)
    page_number = request.GET.get('page', '1')
    issues_page = paginator.get_page(page_number)
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' and not request.GET.get('export'):
        issues_data = [{
            'issue_id': issue.issue_id,
            'component_name': issue.component_name,
            'issue_message': issue.issue_message,
            'status': issue.status,
            'screenshot_url': issue.screenshot_url,
            'capanicus_comment': issue.capanicus_comment,
            'mcm_comment': issue.mcm_comment
        } for issue in issues_page]
        
        return JsonResponse({
            'issues': issues_data,
            'stats': stats,
            'has_next': issues_page.has_next(),
            'has_previous': issues_page.has_previous(),
            'num_pages': paginator.num_pages,
            'current_page': issues_page.number
        })

    # Get all connections for the selector
    live_connections = SheetConnection.objects.filter(
        is_active=True,
        connection_type__in=['google_sheets', 'excel_online']
    ).order_by('-created_at')
    
    uploaded_files = SheetConnection.objects.filter(
        is_active=True,
        connection_type='upload'
    ).order_by('-created_at')
    
    context = {
        'issues_by_sheet': issues_by_sheet,
        'stats': stats,
        'chart_data_json': json.dumps(stats),
        'current_status': status_filter,
        'search_query': search_query,
        'sheet_names': sheet_names,
        'active_sheet': sheet_name,
        'issues_page': issues_page, # For pagination
        'live_connections': live_connections,
        'uploaded_files': uploaded_files,
        'current_connection': current_connection,
    }
    return render(request, 'dashboard/dashboard_comprehensive.html', context)

def dashboard_one(request):
    sheet_name = request.GET.get('sheet')
    connection_id = request.GET.get('connection')
    current_connection = None
    if connection_id:
        try:
            current_connection = SheetConnection.objects.get(id=connection_id)
        except SheetConnection.DoesNotExist:
            current_connection = None
    if not current_connection:
        current_connection = SheetConnection.objects.filter(connection_type='upload').order_by('-created_at').first()
    
    # SYNC LIVE DATA before rendering
    if current_connection:
        from .services import sync_sheet_data
        try:
            sync_sheet_data(current_connection)
        except Exception as e:
            print(f"Comparison sync error: {e}")

    # FETCH LIVE DATA - now using the synced database
    all_issues_data = []
    if current_connection:
        db_issues = Issue.objects.filter(connection=current_connection)
        if sheet_name and sheet_name != 'all':
            db_issues = db_issues.filter(sheet_name=sheet_name)
        
        for it in db_issues:
            all_issues_data.append({
                'issue_id': it.issue_id,
                'component_name': it.component_name,
                'issue_message': it.issue_message,
                'mcm_comment': it.mcm_comment,
                'capanicus_comment': it.capanicus_comment,
                'sheet_name': it.sheet_name
            })

    mcm_counts = {'Fixed': 0, 'In Progress': 0, 'Pending': 0, 'Unknown': 0}
    cop_counts = {'Fixed': 0, 'In Progress': 0, 'Pending': 0, 'Unknown': 0}
    matches = 0
    mismatches = 0
    mismatch_pairs = {}
    highlighted_conflicts = 0
    mismatch_rows = []

    for it_data in all_issues_data:
        m_raw = it_data.get('mcm_comment') or ''
        c_raw = it_data.get('capanicus_comment') or ''
        m = classify_status_detailed(m_raw)
        c = classify_status_detailed(c_raw)
        
        if m in mcm_counts:
            mcm_counts[m] += 1
        else:
            mcm_counts['Unknown'] += 1
            
        if c in cop_counts:
            cop_counts[c] += 1
        else:
            cop_counts['Unknown'] += 1
            
        if m == c:
            matches += 1
        else:
            mismatches += 1
            key = f'{c}|{m}'
            mismatch_pairs[key] = mismatch_pairs.get(key, 0) + 1
            if c == 'Fixed' and m in ['Pending', 'In Progress', 'Unknown']:
                highlighted_conflicts += 1
            
        # Collect ALL rows for comparison details (to support tabs)
        mismatch_rows.append({
            'issue_id': it_data.get('issue_id') or '',
            'component_name': it_data.get('component_name') or '',
            'issue_message': it_data.get('issue_message') or '',
            'm_status': m,
            'c_status': c,
            'is_match': (m == c)
        })

    # Get sheet names for filtering
    if current_connection and current_connection.connection_type == 'upload' and current_connection.uploaded_file:
        try:
            with pd.ExcelFile(current_connection.uploaded_file.path) as xls:
                sheet_names = sorted(xls.sheet_names)
        except:
            sheet_names = []
    else:
        sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
        sheet_names = sorted([sn for sn in set(sheet_names_raw) if sn and sn.strip()])
    
    mismatch_pairs_list = []
    for k, v in mismatch_pairs.items():
        parts = k.split('|', 1)
        cp = parts[0] if len(parts) > 0 else ''
        mc = parts[1] if len(parts) > 1 else ''
        mismatch_pairs_list.append({'cop': cp, 'mcm': mc, 'count': v})
    mismatch_pairs_list.sort(key=lambda x: x['count'], reverse=True)

    live_connections = SheetConnection.objects.filter(is_active=True, connection_type__in=['google_sheets', 'excel_online']).order_by('-created_at')
    uploaded_files = SheetConnection.objects.filter(is_active=True, connection_type='upload').order_by('-created_at')
    chart_payload = {
        'labels': ['Fixed', 'In Progress', 'Pending', 'Unknown'],
        'mcm': [mcm_counts['Fixed'], mcm_counts['In Progress'], mcm_counts['Pending'], mcm_counts['Unknown']],
        'cop': [cop_counts['Fixed'], cop_counts['In Progress'], cop_counts['Pending'], cop_counts['Unknown']],
        'matches': matches,
        'mismatches': mismatches,
        'mismatch_pairs': mismatch_pairs,
        'highlighted': highlighted_conflicts
    }
    context = {
        'current_connection': current_connection,
        'sheet_names': sheet_names,
        'active_sheet': sheet_name,
        'chart_data_json': json.dumps(chart_payload),
        'mcm_counts': mcm_counts,
        'cop_counts': cop_counts,
        'matches': matches,
        'mismatches': mismatches,
        'mismatch_rows': mismatch_rows,
        'mismatch_pairs_list': mismatch_pairs_list,
        'highlighted_conflicts': highlighted_conflicts,
        'live_connections': live_connections,
        'uploaded_files': uploaded_files,
    }
    return render(request, 'dashboard/dashboard_one.html', context)

def export_issues_csv(queryset):
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="filtered_issues.csv"'
    
    writer = csv.writer(response)
    writer.writerow(['Issue ID', 'Component', 'Message', 'Status', 'Screenshot URL', 'Capanicus Comment', 'Created At'])
    
    for issue in queryset:
        writer.writerow([
            issue.issue_id, 
            issue.component_name, 
            issue.issue_message, 
            issue.status, 
            issue.screenshot_url, 
            issue.capanicus_comment, 
            issue.created_at
        ])
        
    return response

def upload_csv(request):
    """Handle CSV/Excel file upload and processing."""
    if request.method == 'POST':
        # Check if this is an analyze request
        if request.POST.get('action') == 'analyze':
            return analyze_file(request)
        
        form = CSVUploadForm(request.POST, request.FILES)
        if form.is_valid():
            upload = form.save()
            
            # Get selected sheet for Excel files
            selected_sheet = request.POST.get('sheet_name', None)
            
            # Create a SheetConnection for this upload
            from .models import SheetConnection
            from django.db import IntegrityError
            
            # Use original filename without extension for connection name if not provided
            original_filename = request.FILES['file'].name
            clean_name = os.path.splitext(os.path.basename(original_filename))[0]
            connection_name = request.POST.get('connection_name') or clean_name
            
            try:
                connection = SheetConnection.objects.create(
                    name=connection_name,
                    connection_type='upload',
                    uploaded_file=upload.file,
                    sheet_name=selected_sheet or 'Sheet1'
                )
            except IntegrityError:
                # If name already exists, try appending a suffix or just use the clean name and handle collision
                # For better UX, let's try to make it unique if the user didn't specify a name
                if not request.POST.get('connection_name'):
                    count = 1
                    while SheetConnection.objects.filter(name=f"{clean_name}_{count}").exists():
                        count += 1
                    connection_name = f"{clean_name}_{count}"
                    connection = SheetConnection.objects.create(
                        name=connection_name,
                        connection_type='upload',
                        uploaded_file=upload.file,
                        sheet_name=selected_sheet or 'Sheet1'
                    )
                else:
                    existing = SheetConnection.objects.filter(name=connection_name).first()
                    msg = f"A connection with the name '{connection_name}' already exists."
                    if existing:
                        msg += f" It is currently connected to: {existing.uploaded_file.name if existing.uploaded_file else existing.sheet_url}"
                    
                    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        return JsonResponse({'success': False, 'message': msg})
                    messages.error(request, msg)
                    return redirect('upload_csv')
            
            all_data = process_file(upload.file.path, selected_sheet, connection=connection)
            
            if not all_data:
                connection.delete()
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return JsonResponse({'success': False, 'message': "Failed to process file. Check columns and format."})
                messages.error(request, "Failed to process file. Ensure columns are correct.")
                return redirect('upload_csv')

            # Save issues with connection
            issues_to_create = [
                Issue(
                    connection=connection,
                    sheet_name=res.get('sheet_name'),
                    issue_id=res.get('issue_id'),
                    component_name=res.get('component_name'),
                    issue_message=res.get('issue_message'),
                    screenshot_url=res.get('screenshot_url'),
                    capanicus_comment=res.get('capanicus_comment'),
                    mcm_comment=res.get('mcm_comment'),
                    status=res['status'],
                    planned_hours=res.get('planned_hours', 0),
                    utilized_hours=res.get('utilized_hours', 0),
                    resource=res.get('resource', ''),
                    priority=res.get('priority', '')
                ) for res in all_data
            ]
            Issue.objects.bulk_create(issues_to_create)
            
            # Update connection last_sync
            connection.last_sync = timezone.now()
            connection.save()
            
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                first_sheet = all_data[0]['sheet_name'] if all_data else 'Default'
                sheet_names = sorted(list(set(d['sheet_name'] for d in all_data)))
                return JsonResponse({
                    'success': True, 
                    'message': f"Successfully processed {len(all_data)} issues.",
                    'connection_id': connection.id,
                    'sheet_names': sheet_names,
                    'active_sheet': first_sheet
                })

            messages.success(request, f"Successfully processed {len(all_data)} issues. Dashboard updated.")
            return redirect(f'/dashboard/?connection={connection.id}')
    else:
        form = CSVUploadForm()
    
    return render(request, 'dashboard/upload_premium.html', {'form': form})


def analyze_file(request):
    """Analyze uploaded file and return sheet info for Excel files."""
    if 'file' not in request.FILES:
        return JsonResponse({'success': False, 'message': 'No file provided'})
    
    file = request.FILES['file']
    
    # Save temporarily
    from django.core.files.storage import default_storage
    from django.core.files.base import ContentFile
    import pandas as pd
    
    temp_path = default_storage.save('temp/' + file.name, ContentFile(file.read()))
    full_path = default_storage.path(temp_path)
    
    try:
        if file.name.endswith(('.xlsx', '.xls')):
            # Get sheet names
            with pd.ExcelFile(full_path) as xls:
                sheets = xls.sheet_names
                
                # Analyze each sheet for columns
                sheet_info = {}
                for s in sheets:
                    df_temp = pd.read_excel(xls, sheet_name=s, nrows=0)
                    from .utils import map_columns
                    mapped, missing = map_columns(df_temp.columns)
                    sheet_info[s] = {
                        'mapped': mapped,
                        'missing': missing
                    }
                
                # Get preview of first sheet
                df = pd.read_excel(xls, sheet_name=sheets[0], nrows=5)
                preview = {
                    'columns': list(df.columns),
                    'rows': df.fillna('').to_dict('records'),
                    'total_rows': len(pd.read_excel(xls, sheet_name=sheets[0]))
                }
            
            # Clean up
            default_storage.delete(temp_path)
            
            return JsonResponse({
                'success': True,
                'sheets': sheets,
                'sheet_info': sheet_info,
                'preview': preview
            })
        else:
            # CSV file - return columns and preview
            df = pd.read_csv(full_path, nrows=5)
            from .utils import map_columns
            mapped, missing = map_columns(df.columns)
            
            preview = {
                'columns': list(df.columns),
                'rows': df.fillna('').to_dict('records'),
                'total_rows': sum(1 for line in open(full_path)) - 1,
                'mapped': mapped,
                'missing': missing
            }
            
            default_storage.delete(temp_path)
            return JsonResponse({
                'success': True,
                'preview': preview
            })
    except Exception as e:
        # Clean up on error
        if default_storage.exists(temp_path):
            default_storage.delete(temp_path)
        return JsonResponse({'success': False, 'message': str(e)})


def issues_list(request):
    """
    Display a list of issues with filtering and search capabilities.
    Clickable from dashboard cards.
    """
    status_filter = request.GET.get('status', '')
    search_query = request.GET.get('search', '')
    sheet_filter = request.GET.get('sheet', '')
    connection_id = request.GET.get('connection', '')
    
    # Base queryset
    issues = Issue.objects.all().order_by('-created_at')
    
    # Apply connection filter
    current_connection = None
    if connection_id:
        try:
            current_connection = SheetConnection.objects.get(id=connection_id)
            issues = issues.filter(connection=current_connection)
        except (SheetConnection.DoesNotExist, ValueError):
            pass
    
    # If no connection filter, but we have some issues, maybe we should default to latest?
    # For now, let's keep it consistent with dashboard view logic if needed.
    
    # Apply sheet filter (tab name)
    if sheet_filter:
        issues = issues.filter(sheet_name=sheet_filter)
    
    # Apply status filter
    if status_filter and status_filter in ['Fixed', 'Pending', 'Unknown']:
        issues = issues.filter(status=status_filter)
    
    # Apply search filter
    if search_query:
        issues = issues.filter(
            models.Q(issue_message__icontains=search_query) |
            models.Q(mcm_comment__icontains=search_query) |
            models.Q(issue_id__icontains=search_query) |
            models.Q(component_name__icontains=search_query)
        )
    
    # Pagination
    paginator = Paginator(issues, 20)  # 20 issues per page
    page_number = request.GET.get('page', '1')
    issues_page = paginator.get_page(page_number)
    
    # Get all chart stats and sheet names for sidebar
    # Only get sheet names for the current connection if available
    sheet_names = []
    if current_connection:
        if current_connection.connection_type == 'upload' and current_connection.uploaded_file:
            try:
                with pd.ExcelFile(current_connection.uploaded_file.path) as xls:
                    sheet_names = sorted(xls.sheet_names)
            except:
                sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
                sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
        else:
            sheet_names_raw = Issue.objects.filter(connection=current_connection).values_list('sheet_name', flat=True).distinct()
            sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
    else:
        # Fallback to all sheet names if no connection filter
        sheet_names_raw = Issue.objects.values_list('sheet_name', flat=True).distinct()
        sheet_names = sorted(list(set([sn for sn in sheet_names_raw if sn and sn.strip()])))
    
    sheet_names = list(dict.fromkeys(sheet_names))
    
    stats = get_chart_stats(sheet_filter if sheet_filter else None, current_connection)
    
    context = {
        'issues': issues_page,
        'stats': stats,
        'sheet_names': sheet_names,
        'active_sheet': sheet_filter,
        'current_status': status_filter,
        'search_query': search_query,
        'current_connection': current_connection,
    }
    
    return render(request, 'dashboard/issues_list.html', context)


def issue_detail(request, issue_id):
    """Display detailed view of a single issue."""
    issue = Issue.objects.get(id=issue_id)
    return render(request, 'dashboard/issue_detail.html', {'issue': issue})


def connect_sheet(request):
    """Handle sheet connection form (live sheets or upload)."""
    if request.method == 'POST':
        form = SheetConnectionForm(request.POST, request.FILES)
        if form.is_valid():
            connection = form.save(commit=False)
            
            # Check for unique name manually to provide better error message
            if SheetConnection.objects.filter(name=connection.name).exists():
                existing = SheetConnection.objects.get(name=connection.name)
                source = existing.uploaded_file.name if existing.uploaded_file else existing.sheet_url
                messages.error(request, f"A connection with the name '{connection.name}' already exists. It is connected to: {source}")
                return render(request, 'dashboard/connect_sheet.html', {'form': form, 'preselected_type': ''})

            # Handle file upload
            if connection.connection_type == 'upload' and request.FILES.get('file'):
                connection.uploaded_file = request.FILES['file']
                connection.save()
                
                # Process the uploaded file
                try:
                    from .utils import process_file
                    process_file(connection.uploaded_file.path, connection=connection)
                    connection.last_sync = timezone.now()
                    connection.save()
                    messages.success(request, f'File uploaded successfully! {connection.name} is now connected.')
                    return redirect(f'/dashboard/?connection={connection.id}')
                except Exception as e:
                    messages.error(request, f'Error processing file: {str(e)}')
                    connection.delete()
                    # Stay on form page with error
            
            # Handle live sheet connection
            elif connection.connection_type in ['google_sheets', 'excel_online']:
                connection.save()
                
                # Try to sync immediately
                try:
                    from .services import sync_sheet_data
                    result = sync_sheet_data(connection)
                    messages.success(request, f'{connection.name} connected successfully! {result.get("count", 0)} issues imported.')
                except Exception as e:
                    messages.warning(request, f'Connection saved but sync failed: {str(e)}')
                
                return redirect(f'/dashboard/?connection={connection.id}')
            else:
                connection.save()
                messages.success(request, f'{connection.name} connection created successfully!')
                return redirect(f'/dashboard/?connection={connection.id}')
        
        # If form is invalid, it will fall through to render below
    else:
        # Check if type=upload parameter is passed
        connection_type = request.GET.get('type', '')
        initial_data = {}
        if connection_type == 'upload':
            initial_data = {'connection_type': 'upload'}
        form = SheetConnectionForm(initial=initial_data)
    
    # Ensure connection_type is always defined for template
    if 'connection_type' not in locals():
        connection_type = ''
    
    return render(request, 'dashboard/connect_sheet.html', {'form': form, 'preselected_type': connection_type})


def check_connection_name(request):
    """API endpoint to check if a connection name already exists."""
    name = request.GET.get('name', '').strip()
    if not name:
        return JsonResponse({'exists': False})
    
    exists = SheetConnection.objects.filter(name=name).exists()
    return JsonResponse({'exists': exists})


def sync_sheet(request, connection_id):
    """API endpoint to sync sheet data."""
    try:
        connection = SheetConnection.objects.get(id=connection_id)
        
        # Use the sync service
        from .services import sync_sheet_data
        result = sync_sheet_data(connection)
        
        return JsonResponse({'success': True, **result})
            
    except SheetConnection.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Connection not found'}, status=404)
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def landing_page(request):
    """Landing page with two options: Connect Live Sheet or Upload File."""
    filter_type = request.GET.get('filter', None)
    
    # Base queryset
    all_connections = SheetConnection.objects.filter(is_active=True)
    
    # Apply filter for the display list
    connections = all_connections
    if filter_type == 'upload':
        connections = connections.filter(connection_type='upload')
    elif filter_type == 'live':
        connections = connections.exclude(connection_type='upload')
        
    connections = connections.order_by('-created_at')[:10]
    
    # Get counts for filter buttons
    total_count = all_connections.count()
    upload_count = all_connections.filter(connection_type='upload').count()
    live_count = all_connections.exclude(connection_type='upload').count()
    
    return render(request, 'dashboard/landing_page.html', {
        'connections': connections,
        'filter_type': filter_type,
        'total_count': total_count,
        'upload_count': upload_count,
        'live_count': live_count,
        'total_connections': total_count,
    })


def connections_list(request):
    """List all connections with view and delete options."""
    filter_type = request.GET.get('filter', None)
    
    # Base queryset
    connections = SheetConnection.objects.filter(is_active=True)
    
    # Apply filter
    if filter_type == 'upload':
        connections = connections.filter(connection_type='upload')
    elif filter_type == 'live':
        connections = connections.exclude(connection_type='upload')
    
    # Order by most recent
    connections = connections.order_by('-created_at')
    
    # Get issue count for each connection
    for conn in connections:
        conn.issue_count = Issue.objects.filter(connection=conn).count()
    
    # Get counts for filter buttons
    total_count = SheetConnection.objects.filter(is_active=True).count()
    upload_count = SheetConnection.objects.filter(is_active=True, connection_type='upload').count()
    live_count = SheetConnection.objects.filter(is_active=True).exclude(connection_type='upload').count()
    
    return render(request, 'dashboard/connections_list.html', {
        'connections': connections,
        'filter_type': filter_type,
        'total_count': total_count,
        'upload_count': upload_count,
        'live_count': live_count,
    })


def delete_connection(request, connection_id):
    """Delete a connection and its associated issues."""
    try:
        connection = SheetConnection.objects.get(id=connection_id)
        connection_name = connection.name
        
        # Delete associated issues
        Issue.objects.filter(connection=connection).delete()
        
        # Delete the connection
        connection.delete()
        
        messages.success(request, f'Connection "{connection_name}" and its data have been deleted.')
    except SheetConnection.DoesNotExist:
        messages.error(request, 'Connection not found.')
    
    return redirect('connections_list')


def dashboard_live_data(request):
    """API endpoint for live dashboard data updates."""
    connection_id = request.GET.get('connection')
    sheet_name = request.GET.get('sheet', None)
    
    if not connection_id:
        return JsonResponse({'error': 'Connection ID required'}, status=400)
    
    try:
        connection = SheetConnection.objects.get(id=connection_id)
    except SheetConnection.DoesNotExist:
        return JsonResponse({'error': 'Connection not found'}, status=404)
    
    # Sync data for live connections or re-process for uploads
    if connection.connection_type in ['google_sheets', 'excel_online', 'upload']:
        try:
            from .services import sync_sheet_data
            sync_sheet_data(connection)
        except Exception as e:
            # Log error but still return current data
            print(f"Live API sync error: {e}")
    
    # Get fresh stats
    stats = get_chart_stats(sheet_name, connection)
    
    # Get issues
    all_issues = Issue.objects.filter(connection=connection)
    if sheet_name:
        all_issues = all_issues.filter(sheet_name=sheet_name)
    all_issues = all_issues.order_by('-created_at')[:15]
    
    issues_data = [{
        'issue_id': issue.issue_id,
        'component_name': issue.component_name,
        'issue_message': issue.issue_message,
        'status': issue.status,
        'screenshot_url': issue.screenshot_url,
        'capanicus_comment': issue.capanicus_comment,
        'mcm_comment': issue.mcm_comment
    } for issue in all_issues]
    
    return JsonResponse({
        'stats': stats,
        'issues': issues_data,
        'last_sync': connection.last_sync.isoformat() if connection.last_sync else None
    })
