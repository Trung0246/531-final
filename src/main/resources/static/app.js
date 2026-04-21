const state = {
    charts: {},
    datasets: []
};

const chartColors = ['#7fb4ff', '#95f2b7', '#ffb86b', '#ff8b8b', '#c4a1ff'];

const elements = {
    form: document.getElementById('dataset-form'),
    message: document.getElementById('message'),
    datasetSelect: document.getElementById('dataset-select'),
    datasetTypeDisplay: document.getElementById('dataset-type-display'),
    loadButton: document.getElementById('load-dashboard'),
    maxFiles: document.getElementById('max-files'),
    refreshToggle: document.getElementById('refresh-toggle'),
    summary: document.getElementById('summary'),
    keywordsList: document.getElementById('keywords-list'),
    edgesTableBody: document.getElementById('edges-table-body'),
    primaryChartTitle: document.getElementById('primary-chart-title'),
    secondaryChartTitle: document.getElementById('secondary-chart-title'),
    tertiaryChartTitle: document.getElementById('tertiary-chart-title'),
    quaternaryChartTitle: document.getElementById('quaternary-chart-title'),
    listPanelTitle: document.getElementById('list-panel-title'),
    tablePanelTitle: document.getElementById('table-panel-title'),
    tableCol1: document.getElementById('table-col-1'),
    tableCol2: document.getElementById('table-col-2'),
    tableCol3: document.getElementById('table-col-3')
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

function renderSummaryItems(items) {
    elements.summary.innerHTML = items
        .map(([label, value]) => `
            <div class="summary-card">
                <div class="label">${label}</div>
                <div class="value">${value ?? '—'}</div>
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
                data: items.map(item => item.count),
                backgroundColor: chartColors[0]
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

function renderLineChart(canvasId, chartName, items, label = 'Count') {
    destroyChart(chartName);
    const canvas = document.getElementById(canvasId);
    state.charts[chartName] = new Chart(canvas, {
        type: 'line',
        data: {
            labels: items.map(item => item.bucket),
            datasets: [{
                label,
                data: items.map(item => item.count),
                borderColor: chartColors[0],
                backgroundColor: chartColors[0],
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

function renderMultiLineChart(canvasId, chartName, seriesList) {
    destroyChart(chartName);
    const canvas = document.getElementById(canvasId);
    const labels = Array.from(new Set(seriesList.flatMap(series => series.points.map(point => point.bucket)))).sort();

    state.charts[chartName] = new Chart(canvas, {
        type: 'line',
        data: {
            labels,
            datasets: seriesList.map((series, index) => {
                const pointsByBucket = new Map(series.points.map(point => [point.bucket, point.count]));
                const color = chartColors[index % chartColors.length];
                return {
                    label: series.name,
                    data: labels.map(label => pointsByBucket.get(label) || 0),
                    borderColor: color,
                    backgroundColor: color,
                    tension: 0.2,
                    fill: false
                };
            })
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

function renderListItems(items) {
    elements.keywordsList.innerHTML = items
        .map(item => `<li>${item}</li>`)
        .join('');
}

function renderTable(headers, rows) {
    const safeHeaders = [headers[0] || '', headers[1] || '', headers[2] || ''];
    elements.tableCol1.textContent = safeHeaders[0];
    elements.tableCol2.textContent = safeHeaders[1];
    elements.tableCol3.textContent = safeHeaders[2];

    elements.edgesTableBody.innerHTML = rows
        .map(row => `
            <tr>
                <td>${row[0] ?? '—'}</td>
                <td>${row[1] ?? '—'}</td>
                <td>${row[2] ?? '—'}</td>
            </tr>
        `)
        .join('');
}

function setDashboardTitles(titles) {
    elements.primaryChartTitle.textContent = titles.primary;
    elements.secondaryChartTitle.textContent = titles.secondary;
    elements.tertiaryChartTitle.textContent = titles.tertiary;
    elements.quaternaryChartTitle.textContent = titles.quaternary;
    elements.listPanelTitle.textContent = titles.list;
    elements.tablePanelTitle.textContent = titles.table;
}

function renderEmailDashboard(snapshot) {
    const overview = snapshot.overview;

    setDashboardTitles({
        primary: 'Email volume by month',
        secondary: 'Top senders',
        tertiary: 'Top recipients',
        quaternary: 'Hourly distribution (UTC)',
        list: 'Top subject keywords',
        table: 'Communication graph edges'
    });

    renderSummaryItems([
        ['Dataset', snapshot.datasetName],
        ['Dataset type', snapshot.datasetType],
        ['Scanned files', overview.scannedFiles],
        ['Parsed emails', overview.parsedEmails],
        ['Failed files', overview.failedFiles],
        ['Unique senders', overview.uniqueSenders],
        ['Unique recipients', overview.uniqueRecipients],
        ['First email', formatInstant(overview.firstEmailAt)],
        ['Last email', formatInstant(overview.lastEmailAt)]
    ]);

    renderBarChart('volume-chart', 'volumeChart', snapshot.volumeByMonth, 'bucket');
    renderBarChart('senders-chart', 'sendersChart', snapshot.topSenders);
    renderBarChart('recipients-chart', 'recipientsChart', snapshot.topRecipients);
    renderLineChart('hourly-chart', 'hourlyChart', snapshot.hourlyDistribution);
    renderListItems(snapshot.topSubjectKeywords.map(item => `${item.name} (${item.count})`));
    renderTable(
        ['Source', 'Target', 'Count'],
        snapshot.communicationGraph.map(edge => [edge.source, edge.target, edge.count])
    );
}

function renderCsvDashboard(snapshot) {
    const overview = snapshot.overview;
    const metricTotals = snapshot.metricTotals || [];
    const topLocationBreakdowns = snapshot.topLocationsByMetric || [];
    const primaryBreakdown = topLocationBreakdowns[0]?.items || [];
    const metricSeries = (snapshot.metricTimeSeries || []).slice(0, 3);

    setDashboardTitles({
        primary: 'Rows by observation date',
        secondary: 'Latest metric totals',
        tertiary: topLocationBreakdowns[0]?.name ? `Top locations by ${topLocationBreakdowns[0].name}` : 'Top locations',
        quaternary: 'Metric trends over time',
        list: 'Detected schema and totals',
        table: 'Top locations by metric'
    });

    renderSummaryItems([
        ['Dataset', snapshot.datasetName],
        ['Dataset type', snapshot.datasetType],
        ['Scanned files', overview.scannedFiles],
        ['Processed rows', overview.processedRows],
        ['Failed files', overview.failedFiles],
        ['Detected metrics', overview.detectedMetrics],
        ['Distinct locations', overview.distinctLocations],
        ['Date column', overview.dateColumn || '—'],
        ['Location column', overview.locationColumn || '—'],
        ['First observation', formatInstant(overview.firstObservedAt)],
        ['Last observation', formatInstant(overview.lastObservedAt)]
    ]);

    renderLineChart('volume-chart', 'volumeChart', snapshot.rowsByDate || [], 'Rows');
    renderBarChart('senders-chart', 'totalsChart', metricTotals);
    renderBarChart('recipients-chart', 'locationsChart', primaryBreakdown);
    renderMultiLineChart('hourly-chart', 'metricTrendsChart', metricSeries);

    renderListItems([
        ...(overview.metricColumns || []).map(name => `Metric: ${name}`),
        ...metricTotals.map(item => `${item.name}: ${item.count}`)
    ]);

    const metricNames = topLocationBreakdowns.map(breakdown => breakdown.name).slice(0, 2);
    const locations = new Map();
    topLocationBreakdowns.slice(0, 2).forEach(breakdown => {
        breakdown.items.forEach(item => {
            if (!locations.has(item.name)) {
                locations.set(item.name, {});
            }
            locations.get(item.name)[breakdown.name] = item.count;
        });
    });

    const rows = Array.from(locations.entries())
        .map(([location, values]) => [location, values[metricNames[0]] || 0, values[metricNames[1]] || 0])
        .sort((left, right) => right[1] - left[1]);

    renderTable(
        ['Location', metricNames[0] || 'Metric', metricNames[1] || 'Value'],
        rows.length > 0 ? rows : [['No location data', '—', '—']]
    );
}

function updateDatasetTypeDisplay() {
    const selectedDataset = state.datasets.find(dataset => dataset.id === elements.datasetSelect.value);
    elements.datasetTypeDisplay.value = selectedDataset ? selectedDataset.datasetType : '';
}

async function loadDatasets() {
    state.datasets = await api('/api/datasets');
    elements.datasetSelect.innerHTML = state.datasets
        .map(dataset => `<option value="${dataset.id}">${dataset.name} — ${dataset.datasetType} — ${dataset.hdfsPath}</option>`)
        .join('');

    if (state.datasets.length === 0) {
        elements.datasetSelect.innerHTML = '<option value="">No datasets yet</option>';
    }

    updateDatasetTypeDisplay();
    return state.datasets;
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

        if (snapshot.datasetType === 'CSV_TEXT') {
            renderCsvDashboard(snapshot);
        } else {
            renderEmailDashboard(snapshot);
        }

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
        updateDatasetTypeDisplay();
        showMessage(`Registered dataset ${dataset.name}.`, 'success');
    } catch (error) {
        showMessage(error.message, 'error');
    }
});

elements.datasetSelect.addEventListener('change', updateDatasetTypeDisplay);
elements.loadButton.addEventListener('click', loadDashboard);

window.addEventListener('load', async () => {
    try {
        await loadDatasets();
        showMessage('Ready. Register a dataset or choose one from the list.', 'success');
    } catch (error) {
        showMessage(error.message, 'error');
    }
});
