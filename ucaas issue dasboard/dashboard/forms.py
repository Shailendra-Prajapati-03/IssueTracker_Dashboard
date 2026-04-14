from django import forms
from .models import CSVUpload, SheetConnection

class CSVUploadForm(forms.ModelForm):
    class Meta:
        model = CSVUpload
        fields = ['file']


class SheetConnectionForm(forms.ModelForm):
    """Form for creating sheet connections (live or upload)."""
    
    # Extra field for file upload (only used when connection_type is 'upload')
    file = forms.FileField(required=False, help_text="Upload Excel/CSV file (only for File Upload type)")
    sheet_name = forms.CharField(required=False, initial='Sheet1', widget=forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Sheet1'}))
    
    class Meta:
        model = SheetConnection
        fields = ['name', 'connection_type', 'sheet_url', 'api_key', 'sheet_name']
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Connection Name'}),
            'connection_type': forms.Select(attrs={'class': 'form-control', 'id': 'connectionType'}),
            'sheet_url': forms.URLInput(attrs={'class': 'form-control', 'placeholder': 'https://docs.google.com/spreadsheets/...'}),
            'api_key': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Optional API Key'}),
        }
    
    def clean(self):
        cleaned_data = super().clean()
        connection_type = cleaned_data.get('connection_type')
        sheet_url = cleaned_data.get('sheet_url')
        file = self.cleaned_data.get('file')
        
        # Validate based on connection type
        if connection_type in ['google_sheets', 'excel_online']:
            if not sheet_url:
                raise forms.ValidationError("Sheet URL is required for live sheet connections.")
        elif connection_type == 'upload':
            if not file:
                raise forms.ValidationError("Please upload a file.")
        
        return cleaned_data
