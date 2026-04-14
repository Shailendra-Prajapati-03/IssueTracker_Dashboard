/**
 * Live Updates JavaScript
 * Handles real-time sync and polling for sheet connections
 */

// Configuration
const POLL_INTERVAL = 30000; // 30 seconds
let pollingInterval = null;
let currentConnectionId = null;

/**
 * Start polling for updates
 */
function startLiveUpdates(connectionId) {
    currentConnectionId = connectionId;
    
    // Stop any existing polling
    stopLiveUpdates();
    
    // Start new polling
    pollingInterval = setInterval(() => {
        checkForUpdates();
    }, POLL_INTERVAL);
    
    console.log(`Live updates started for connection ${connectionId}`);
}

/**
 * Stop polling
 */
function stopLiveUpdates() {
    if (pollingInterval) {
        clearInterval(pollingInterval);
        pollingInterval = null;
        console.log('Live updates stopped');
    }
}

/**
 * Check for updates from the server
 */
async function checkForUpdates() {
    if (!currentConnectionId) return;
    
    try {
        const response = await fetch(`/sync/${currentConnectionId}/`, {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });
        
        const data = await response.json();
        
        if (data.success && data.count > 0) {
            // Show notification
            showUpdateNotification(data.count);
            
            // Optionally auto-refresh
            // refreshDashboard();
        }
    } catch (error) {
        console.error('Error checking for updates:', error);
    }
}

/**
 * Show update notification
 */
function showUpdateNotification(count) {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded-lg shadow-lg z-50 animate-fade-in';
    notification.innerHTML = `
        <div class="flex items-center gap-2">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
            </svg>
            <span>${count} new issues found!</span>
            <button onclick="refreshDashboard()" class="ml-2 underline hover:no-underline">
                Refresh
            </button>
        </div>
    `;
    
    document.body.appendChild(notification);
    
    // Auto-remove after 10 seconds
    setTimeout(() => {
        notification.remove();
    }, 10000);
}

/**
 * Refresh dashboard data
 */
async function refreshDashboard() {
    try {
        const url = new URL(window.location.href);
        url.searchParams.set('ajax', '1');
        
        const response = await fetch(url.toString(), {
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });
        
        const data = await response.json();
        
        // Update stats
        if (data.stats) {
            updateStats(data.stats);
        }
        
        // Update charts
        if (data.chart_data) {
            updateCharts(data.chart_data);
        }
        
        // Reload page to show new data
        window.location.reload();
        
    } catch (error) {
        console.error('Error refreshing dashboard:', error);
    }
}

/**
 * Update stats on the page
 */
function updateStats(stats) {
    // Update summary cards
    const totalElement = document.querySelector('[data-stat="total"]');
    const fixedElement = document.querySelector('[data-stat="fixed"]');
    const pendingElement = document.querySelector('[data-stat="pending"]');
    
    if (totalElement) totalElement.textContent = stats.total_count || 0;
    if (fixedElement) fixedElement.textContent = stats.fixed_count || 0;
    if (pendingElement) pendingElement.textContent = stats.pending_count || 0;
}

/**
 * Manual sync button handler
 */
async function manualSync(connectionId) {
    const button = document.getElementById('syncButton');
    if (button) {
        button.disabled = true;
        button.innerHTML = '<span class="animate-spin">↻</span> Syncing...';
    }
    
    try {
        const response = await fetch(`/sync/${connectionId}/`, {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            showUpdateNotification(data.count || 0);
            setTimeout(() => window.location.reload(), 1500);
        } else {
            alert('Sync failed: ' + (data.error || 'Unknown error'));
        }
    } catch (error) {
        alert('Sync error: ' + error.message);
    } finally {
        if (button) {
            button.disabled = false;
            button.innerHTML = '↻ Sync Now';
        }
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // Check if we're on a dashboard with a connection
    const urlParams = new URLSearchParams(window.location.search);
    const connectionId = urlParams.get('connection');
    
    if (connectionId) {
        startLiveUpdates(connectionId);
    }
});
