// Processing Page Script

class ProcessingManager {
    constructor() {
        this.sessionId = null;
        this.isProcessing = false;
        this.lastLogIndex = 0;
        this.pollInterval = null;
        this.pollDelay = 1200; // Poll backend logs at a moderate rate
        this.allLogs = [];
        this.processingMetrics = {
            rowsProcessed: 0,
            totalMapped: 0,
            totalProcessed: 0,
            status: 'Starting...'
        };
        this.init();
    }

    init() {
        // Retrieve session ID from localStorage
        this.sessionId = localStorage.getItem('mapping_session_id');
        
        if (!this.sessionId) {
            this.displayError('No active session found. Please go back and run the mapping process.');
            return;
        }

        // Update session info display
        document.getElementById('sessionIdDisplay').textContent = this.sessionId;

        // Attach event listeners
        document.getElementById('clearLogsBtn').addEventListener('click', () => this.clearLogs());
        document.getElementById('closeReviewModal').addEventListener('click', () => this.closeModal());
        document.getElementById('proceedToReviewBtn').addEventListener('click', () => this.proceedToReview());

        // Add initial log entry
        this.addLog('system', 'Connected to processing server');
        this.addLog('system', `Session ID: ${this.sessionId}`);
        this.addLog('info', 'Starting data mapping process...');

        // Start polling for updates
        this.isProcessing = true;
        this.startPolling();
    }

    buildApiUrl(endpoint) {
        // Since frontend is now served from FastAPI backend,
        // all API calls should be to the same server
        return endpoint;
    }

    startPolling() {
        // Initial fetch immediately
        this.pollForUpdates();

        // Then set up recurring polls
        this.pollInterval = setInterval(() => {
            if (this.isProcessing) {
                this.pollForUpdates();
            }
        }, this.pollDelay);
    }

    stopPolling() {
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
            this.pollInterval = null;
            console.log("Polling stopped.");
        }
    }

    async pollForUpdates() {
        try {
            const url = this.buildApiUrl(`/api/v1/sessions/${this.sessionId}/logs?from_index=${this.lastLogIndex}`);
            const response = await fetch(url, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (response.ok) {
                const data = await response.json();
                console.log("Received data:", data);
                this.processUpdate(data);

                if (Array.isArray(data.logs) && data.logs.length > 0) {
                    data.logs.forEach((backendLog) => this.renderBackendLog(backendLog));
                }
                if (typeof data.next_index === 'number') {
                    this.lastLogIndex = data.next_index;
                }

                if (data.status === 'failed') {
                    this.displayError(data.message || 'Mapping process failed.');
                    return;
                }

                if (data.done) {
                    this.stopPolling();
                    this.completeProcessing();
                }
            } else {
                console.error("Failed to fetch updates:", response.statusText);
            }
        } catch (error) {
            console.error("Error during polling:", error);
        }
    }

    processUpdate(data) {
        // Update metrics from response
        if (data.total_suggestions !== undefined) {
            this.processingMetrics.totalProcessed = data.total_suggestions;
            document.getElementById('totalProcessed').textContent = data.total_suggestions;
        }

        if (data.auto_rejected_count !== undefined) {
            this.processingMetrics.rowsProcessed = data.total_suggestions - (data.auto_rejected_count || 0);
            document.getElementById('rowsProcessed').textContent = this.processingMetrics.rowsProcessed;
        }

        if (data.unmapped_count !== undefined) {
            this.processingMetrics.totalMapped = data.total_suggestions - (data.unmapped_count || 0);
            document.getElementById('totalMapped').textContent = this.processingMetrics.totalMapped;
        }

        // Add log entries if available (simulated based on data changes)
        if (data.current_record) {
            this.addLog('info', `Processing record: ${data.current_record}`);
            document.getElementById('currentStatus').textContent = `Processing ${data.current_record}`;
        }

        // Update status
        if (data.status) {
            this.processingMetrics.status = data.status;
            document.getElementById('currentStatus').textContent = data.status;
        }
    }

    renderBackendLog(backendLog) {
        if (!backendLog || !backendLog.message) {
            return;
        }

        const type = backendLog.stream === 'stderr' ? 'error' : 'info';
        this.renderLog({
            type,
            message: backendLog.message,
            timestamp: backendLog.timestamp || new Date().toLocaleTimeString('en-US', {
                hour12: false,
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            })
        });
    }

    addLog(type, message) {
        const timestamp = new Date().toLocaleTimeString('en-US', { 
            hour12: false, 
            hour: '2-digit', 
            minute: '2-digit', 
            second: '2-digit'
        });

        const logEntry = {
            type,
            message,
            timestamp
        };

        this.allLogs.push(logEntry);
        this.renderLog(logEntry);
    }

    renderLog(logEntry) {
        const terminal = document.getElementById('terminalOutput');
        const logLine = document.createElement('div');
        logLine.className = `log-line ${logEntry.type}`;

        const timestampSpan = document.createElement('span');
        timestampSpan.className = 'timestamp';
        timestampSpan.textContent = `[${logEntry.timestamp}]`;

        const messageSpan = document.createElement('span');
        messageSpan.className = 'message';
        messageSpan.textContent = logEntry.message;

        logLine.appendChild(timestampSpan);
        logLine.appendChild(messageSpan);
        terminal.appendChild(logLine);

        // Auto-scroll to bottom
        terminal.scrollTop = terminal.scrollHeight;
    }

    clearLogs() {
        const terminal = document.getElementById('terminalOutput');
        terminal.innerHTML = '';
        this.allLogs = [];
        this.addLog('system', 'Logs cleared');
    }

    displayError(message) {
        this.addLog('error', message);
        this.isProcessing = false;
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
        }
    }

    completeProcessing() {
        this.isProcessing = false;
        
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
        }

        this.addLog('success', 'Processing completed successfully!');
        this.addLog('info', 'All records have been processed and mapped.');
        
        // Small delay before showing modal for better UX
        setTimeout(() => {
            this.showHumanReviewModal();
        }, 1500);
    }

    showHumanReviewModal() {
        const modal = document.getElementById('humanReviewModal');
        modal.removeAttribute('hidden');
        
        // Ensure stats are updated in modal
        document.getElementById('totalProcessed').textContent = this.processingMetrics.totalProcessed || 0;
        document.getElementById('totalMapped').textContent = this.processingMetrics.totalMapped || 0;
    }

    closeModal() {
        const modal = document.getElementById('humanReviewModal');
        modal.setAttribute('hidden', '');
    }

    proceedToReview() {
        // Save current metrics to localStorage for next stage
        localStorage.setItem('mapping_metrics', JSON.stringify(this.processingMetrics));
        
        // Navigate to human review page (Stage 3)
        // For now, show a message since Stage 3 is placeholder
        this.addLog('info', 'Navigating to Human Review page...');
        
        setTimeout(() => {
            // Navigate to human review page with absolute path
            window.location.href = '/pages/human-review.html';
        }, 1000);
    }
}

// Initialize processing manager when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.processingManager = new ProcessingManager();

    // Optional: Simulate progress for demo/testing
    // Uncomment the line below to see demo progress
    // simulateDemoProgress();
});

// Demo simulation function for testing without backend
function simulateDemoProgress() {
    const manager = window.processingManager;
    const totalRecords = 45;
    let currentRecord = 0;

    const demoMessages = [
        'Initializing mapper engine...',
        'Validating CDM schema...',
        'Loading mapping rules...',
        'Validating input data...',
        'Starting semantic analysis...',
        'Processing field mappings...',
        'Resolving conflicts...',
        'Generating suggestions...',
        'Finalizing mappings...',
        'All records processed successfully!'
    ];

    const simulationInterval = setInterval(() => {
        if (currentRecord < totalRecords) {
            const messageIndex = Math.floor((currentRecord / totalRecords) * demoMessages.length);
            const message = demoMessages[Math.min(messageIndex, demoMessages.length - 1)];
            
            manager.addLog('info', `[${currentRecord}/${totalRecords}] ${message}`);
            
            currentRecord++;
            document.getElementById('rowsProcessed').textContent = currentRecord;
            document.getElementById('currentStatus').textContent = `Processing record ${currentRecord}/${totalRecords}`;
            document.getElementById('totalProcessed').textContent = totalRecords;
            document.getElementById('totalMapped').textContent = Math.floor(currentRecord * 0.95);

        } else {
            clearInterval(simulationInterval);
            manager.completeProcessing();
        }
    }, 300);
}

// Handle page unload to stop polling
window.addEventListener('beforeunload', () => {
    if (window.processingManager && window.processingManager.pollInterval) {
        clearInterval(window.processingManager.pollInterval);
    }
});
