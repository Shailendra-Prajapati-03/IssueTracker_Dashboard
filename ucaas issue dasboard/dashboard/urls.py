from django.urls import path
from . import views

urlpatterns = [
    path('', views.landing_page, name='landing_page'),
    path('dashboard/', views.dashboard, name='dashboard_comprehensive'),
    path('dashboard-one/', views.dashboard_one, name='dashboard_one'),
    path('connect/', views.connect_sheet, name='connect_sheet'),
    path('sync/<int:connection_id>/', views.sync_sheet, name='sync_sheet'),
    path('upload/', views.upload_csv, name='upload_csv'),
    path('issues/', views.issues_list, name='issues_list'),
    path('issues/<int:issue_id>/', views.issue_detail, name='issue_detail'),
    path('connections/', views.connections_list, name='connections_list'),
    path('connections/delete-all/', views.delete_all_connections, name='delete_all_connections'),
    path('connections/<int:connection_id>/delete/', views.delete_connection, name='delete_connection'),
    path('api/dashboard/live/', views.dashboard_live_data, name='dashboard_live_data'),
    path('api/check-connection-name/', views.check_connection_name, name='check_connection_name'),
]
