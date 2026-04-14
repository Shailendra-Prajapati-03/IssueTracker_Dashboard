from django import template

register = template.Library()

@register.filter
def index(value, arg):
    """Get item at index from list"""
    try:
        return value[arg]
    except (IndexError, TypeError):
        return ''
