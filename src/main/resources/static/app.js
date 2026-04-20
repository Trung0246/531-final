const state = {
    charts: {}
};

const elements = {
    form: document.getElementById('dataset-form'),
    message: document.getElementById('message'),
    datasetSelect: document.getElementById('dataset-select'),
    loadButton: document.getElementById('load-dashboard'),
    maxFiles: document.getElementById('max-files'),
    refreshToggle: document.getElementById('refresh-toggle'),
    summary: document.getElementById('summary'),
    keywordsList: document.getElementById('keywords-list'),
    edgesTableBody: document.getElementById('edges-table-body')
};

async function api(url, options = {}) {
    const response = await fetch(url, {
        headers: {
            'Content-Type': 'application/json'
        },
        ...options
    });

    if (!response.ok) {
        let message = response.statusText;
        try {
            const body = await response.json();
            message = body.detail || body.title || message;
        } catch (error) {
            // Ignore non-JSON error bodies.
        }
        throw new Error(message);
    }

    if (response.status === 204) {
        return null;
    }
    return response.json();
}

function showMessage(text, type = '') {
    elements.message.textContent = text;
    elements.message.className = `message ${type}`.trim();
}

function formatInstant(value) {
    if (!value) {
        return '—';
    }
    return new Date(value).toLocaleString();
}

function renderSummary(snapshot) {
    const overview = snapshot.overview;
    const items = [
        ['Dataset', snapshot.datasetName],
        ['Scanned files', overview.scannedFiles],
        ['Parsed emails', overview.parsedEmails],
        ['Failed files', overview.failedFiles],
        ['Unique senders', overview.uniqueSenders],
        ['Unique recipients', overview.uniqueRecipients],
        ['First email', formatInstant(overview.firstEmailAt)],
        ['Last email', formatInstant(overview.lastEmailAt)]
    ];

    elements.summary.innerHTML = items
        .map(([label, value]) => `
            <div class="summary-card">
                <div class="label">${label}</div>
                <div class="value">${value}</div>
            </div>
        `)
        .join('');
}

function destroyChart(name) {
    if (state.charts[name]) {
        state.charts[name].destroy();
        delete state.charts[name];
    }
}

function renderBarChart(canvasId, chartName, items, labelKey = 'name') {
    destroyChart(chartName);
    const canvas = document.getElementById(canvasId);
    state.charts[chartName] = new Chart(canvas, {
        type: 'bar',
        data: {
            labels: items.map(item => item[labelKey]),
            datasets: [{
                label: 'Count',
                data: items.map(item => item.count)
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

function renderLineChart(canvasId, chartName, items) {
    destroyChart(chartName);
    const canvas = document.getElementById(canvasId);
    state.charts[chartName] = new Chart(canvas, {
        type: 'line',
        data: {
            labels: items.map(item => item.bucket),
            datasets: [{
                label: 'Count',
                data: items.map(item => item.count),
                tension: 0.2,
                fill: false
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

function renderKeywords(snapshot) {
    elements.keywordsList.innerHTML = snapshot.topSubjectKeywords
        .map(item => `<li>${item.name} (${item.count})</li>`)
        .join('');
}

function renderEdges(snapshot) {
    elements.edgesTableBody.innerHTML = snapshot.communicationGraph
        .map(edge => `
            <tr>
                <td>${edge.source}</td>
                <td>${edge.target}</td>
                <td>${edge.count}</td>
            </tr>
        `)
        .join('');
}

async function loadDatasets() {
    const datasets = await api('/api/datasets');
    elements.datasetSelect.innerHTML = datasets
        .map(dataset => `<option value="${dataset.id}">${dataset.name} — ${dataset.hdfsPath}</option>`)
        .join('');

    if (datasets.length === 0) {
        elements.datasetSelect.innerHTML = '<option value="">No datasets yet</option>';
    }
    return datasets;
}

async function loadDashboard() {
    const datasetId = elements.datasetSelect.value;
    if (!datasetId) {
        showMessage('Register a dataset first.', 'error');
        return;
    }

    showMessage('Loading analytics...');
    try {
        const maxFiles = Number(elements.maxFiles.value || 5000);
        const refresh = elements.refreshToggle.checked;
        const snapshot = await api(`/api/datasets/${datasetId}/analytics?maxFiles=${maxFiles}&refresh=${refresh}`);

        renderSummary(snapshot);
        renderBarChart('volume-chart', 'volumeChart', snapshot.volumeByMonth, 'bucket');
        renderBarChart('senders-chart', 'sendersChart', snapshot.topSenders);
        renderBarChart('recipients-chart', 'recipientsChart', snapshot.topRecipients);
        renderLineChart('hourly-chart', 'hourlyChart', snapshot.hourlyDistribution);
        renderKeywords(snapshot);
        renderEdges(snapshot);
        showMessage(`Loaded analytics for ${snapshot.datasetName}.`, 'success');
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

elements.form.addEventListener('submit', async event => {
    event.preventDefault();
    showMessage('Registering dataset...');

    const payload = {
        name: document.getElementById('dataset-name').value,
        description: document.getElementById('dataset-description').value,
        datasetType: document.getElementById('dataset-type').value,
        hdfsPath: document.getElementById('dataset-path').value
    };

    try {
        const dataset = await api('/api/datasets/register', {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        await loadDatasets();
        elements.datasetSelect.value = dataset.id;
        showMessage(`Registered dataset ${dataset.name}.`, 'success');
    } catch (error) {
        showMessage(error.message, 'error');
    }
});

elements.loadButton.addEventListener('click', loadDashboard);

window.addEventListener('load', async () => {
    try {
        await loadDatasets();
        showMessage('Ready. Register a dataset or choose one from the list.', 'success');
    } catch (error) {
        showMessage(error.message, 'error');
    }
});
