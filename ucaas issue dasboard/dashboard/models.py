from django.db import models

class SheetConnection(models.Model):
    """Model to store sheet connections (live or upload)."""
    CONNECTION_TYPES = [
        ('google_sheets', 'Google Sheets'),
        ('excel_online', 'Excel Online'),
        ('upload', 'File Upload'),
    ]
    
    name = models.CharField(max_length=100, unique=True)
    connection_type = models.CharField(max_length=20, choices=CONNECTION_TYPES)
    
    # For live sheets
    sheet_url = models.URLField(blank=True, null=True)
    api_key = models.CharField(max_length=500, blank=True, null=True)
    sheet_id = models.CharField(max_length=200, blank=True, null=True)
    sheet_name = models.CharField(max_length=100, blank=True, null=True)
    
    # For upload
    uploaded_file = models.FileField(upload_to='uploads/', blank=True, null=True)
    
    # Metadata
    is_active = models.BooleanField(default=True)
    last_sync = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.name} ({self.get_connection_type_display()})"


class Issue(models.Model):
    """
    Model to store issue data from CSV.
    
    IMPORTANT DATA FLOW:
    - "Issues" column → issue_message (displayed in UI)
    - "MCM Comments" column → mcm_comment (used ONLY for status classification)
    """
    STATUS_CHOICES = [
        ('Fixed', 'Fixed'),
        ('Pending', 'Pending'),
        ('Unknown', 'Unknown'),
    ]

    issue_id = models.CharField(max_length=50, blank=True, null=True)
    component_name = models.CharField(max_length=255, blank=True, null=True)
    
    # issue_message comes from "Issues" column - THIS IS WHAT USERS SEE
    issue_message = models.TextField(default="No message provided")
    
    # mcm_comment comes from "MCM Comments" column - used ONLY for status detection
    mcm_comment = models.TextField(blank=True, null=True)
    
    # Auto-classified status based on mcm_comment content
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='Unknown')
    
    # Optional fields
    screenshot_url = models.URLField(max_length=500, blank=True, null=True)
    capanicus_comment = models.TextField(blank=True, null=True)
    sheet_name = models.CharField(max_length=255, default='Default')
    resource = models.CharField(max_length=255, blank=True, null=True)
    planned_hours = models.FloatField(default=0)
    utilized_hours = models.FloatField(default=0)
    priority = models.CharField(max_length=50, blank=True, null=True)
    
    # Link to connection (live sheet or upload)
    connection = models.ForeignKey(SheetConnection, on_delete=models.CASCADE, null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['issue_id']),
        ]

    def __str__(self):
        return f"{self.issue_id or 'No ID'} - {self.status}"

    def get_short_message(self, length=100):
        """Return truncated issue message for preview."""
        if len(self.issue_message) > length:
            return self.issue_message[:length] + "..."
        return self.issue_message


class CSVUpload(models.Model):
    """Model to track CSV file uploads."""
    file = models.FileField(upload_to='csv_uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)
    processed = models.BooleanField(default=False)
    row_count = models.IntegerField(default=0)

    def __str__(self):
        return f"Upload at {self.uploaded_at}"
