// src/dashboard/dashboard_controller.js
// Robust Dashboard Controller for Shareholder Pipeline
// Handles UI reset, SSE, error/no_results, and paginated data fetching

let eventSource = null;
let currentPage = 1;
const PAGE_SIZE = 50;

function resetUI() {
    currentPage = 1;
    document.getElementById('progress-bar').style.width = '0%';
    document.getElementById('status-label').textContent = 'Initializing';
    clearTable();
    if (eventSource) {
        eventSource.close();
        eventSource = null;
    }
}

function clearTable() {
    const table = document.getElementById('results-table');
    while (table.rows.length > 1) table.deleteRow(1);
}

function startTask(apiEndpoint, payload) {
    resetUI();
    fetch(apiEndpoint, {
        method: 'POST',
        body: JSON.stringify(payload),
        headers: {'Content-Type': 'application/json'}
    })
    .then(res => res.json())
    .then(data => {
        listenToProgress();
        fetchPage(1);
    });
}

function listenToProgress() {
    if (eventSource) eventSource.close();
    eventSource = new EventSource('/api/pipeline/status');
    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        if (data.step === 'No PDFs found' || data.status === 'no_results') {
            document.getElementById('status-label').textContent = 'Information not found';
            eventSource.close();
        } else if (data.step === 'Error' || data.status === 'error') {
            document.getElementById('status-label').textContent = 'Error: ' + data.message;
            eventSource.close();
        } else {
            document.getElementById('progress-bar').style.width = `${data.progress || 0}%`;
            document.getElementById('status-label').textContent = data.message || data.step;
            if (data.step === 'Complete' || data.status === 'done') {
                fetchPage(1);
            }
        }
    };
}

function fetchPage(page) {
    currentPage = page;
    fetch(`/api/shareholders?page=${page}&per_page=${PAGE_SIZE}`)
        .then(res => res.json())
        .then(data => {
            renderTable(data.records);
            renderPagination(data.page, data.pages);
        });
}

function renderTable(records) {
    clearTable();
    const table = document.getElementById('results-table');
    records.forEach(row => {
        const tr = table.insertRow();
        Object.values(row).forEach(val => {
            tr.insertCell().textContent = val;
        });
    });
}

function renderPagination(page, pages) {
    const pagination = document.getElementById('pagination');
    pagination.innerHTML = '';
    for (let i = 1; i <= pages; i++) {
        const btn = document.createElement('button');
        btn.textContent = i;
        btn.disabled = (i === page);
        btn.onclick = () => fetchPage(i);
        pagination.appendChild(btn);
    }
}

document.getElementById('search-btn').onclick = () => {
    startTask('/api/pipeline/run', {/* payload */});
};
document.getElementById('upload-btn').onclick = () => {
    startTask('/api/upload', {/* payload */});
};
