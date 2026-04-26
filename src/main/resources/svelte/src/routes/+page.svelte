<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import ChartPanel from '$lib/components/ChartPanel.svelte';
	import { graphqlRequest } from '$lib/graphql';
	import type {
		ChartMode,
		DashboardChart,
		DashboardProgressEvent,
		DashboardView,
		DatasetView,
		HdfsFileDescriptor,
		ImportLocalDirectoryInput,
		RegisterDatasetInput
	} from '$lib/types';

	const DATASETS_QUERY = `
		query Datasets {
			datasets {
				id
				name
				description
				datasetType
				hdfsPath
				registeredAt
				hdfsPathAlreadyExisted
			}
		}
	`;

	const DASHBOARD_QUERY = `
		query Dashboard($datasetId: ID!, $maxFiles: Int, $refresh: Boolean!, $updateEveryRows: Int, $fullDashboardUpdateEveryRows: Int) {
			dashboard(datasetId: $datasetId, maxFiles: $maxFiles, refresh: $refresh, updateEveryRows: $updateEveryRows, fullDashboardUpdateEveryRows: $fullDashboardUpdateEveryRows) {
				datasetId
				datasetName
				datasetType
				hdfsPath
				generatedAt
				maxFiles
				summaryItems {
					label
					value
				}
				charts {
					id
					title
					type
					series {
						name
						points {
							label
							value
						}
					}
				}
				columnProfiles {
					name
					type
					sampleValues
				}
				listPanel {
					title
					items
				}
				tablePanel {
					title
					columns
					rows {
						cells
					}
				}
			}
		}
	`;

	const REGISTER_MUTATION = `
		mutation RegisterDataset($input: RegisterDatasetInput!) {
			registerDataset(input: $input) {
				id
				name
				description
				datasetType
				hdfsPath
				registeredAt
				hdfsPathAlreadyExisted
			}
		}
	`;

	const chartModes: ChartMode[] = ['BAR', 'LINE', 'TABLE'];
	const focusValueLimit = 40;
	const selectedDatasetStorageKey = 'datasetviz:selectedDatasetId';
	const updateEveryRowsStorageKey = 'datasetviz:updateEveryRows';
	const fullDashboardUpdateEveryRowsStorageKey = 'datasetviz:fullDashboardUpdateEveryRows';
	const chartShellUpdateIntervalMs = 1000;
	type MessageState = { text: string; type: 'info' | 'success' | 'error' };

	let datasets = $state<DatasetView[]>([]);
	let files = $state<HdfsFileDescriptor[]>([]);
	let dashboard = $state<DashboardView | null>(null);
	let chartTypeOverrides = $state<Record<string, ChartMode>>({});
	let chartFocusOverrides = $state<Record<string, string[]>>({});
	let selectedDatasetId = $state('');
	let maxFiles = $state(5000);
	let updateEveryRows = $state(25000);
	let fullDashboardUpdateEveryRows = $state(500);
	let refresh = $state(false);
	let isRegistering = $state(false);
	let isImporting = $state(false);
	let isUploading = $state(false);
	let isDeleting = $state(false);
	let isLoadingDashboard = $state(false);
	let isLoadingDatasets = $state(false);
	let message = $state<MessageState>({ text: '', type: 'info' });
	let dashboardProgress = $state<DashboardProgressEvent | null>(null);
	let progressMessage = $state('');
	let progressScannedFiles = $state(0);
	let progressTotalFiles = $state(0);
	let progressProcessedRows = $state(0);
	let progressFailedFiles = $state(0);
	let progressFiles = $state<DashboardProgressEvent['files']>([]);
	let progressSocket: WebSocket | null = null;
	let lastChartShellUpdate = 0;
	let hasLivePartialDashboard = $state(false);
	let activeDashboardDatasetId = '';
	let dashboardRequestSequence = 0;
	let form = $state<RegisterDatasetInput>({
		name: '',
	description: '',
		hdfsPath: ''
	});
	let importForm = $state<ImportLocalDirectoryInput>({
		datasetId: '',
		datasetType: 'CSV_TEXT',
		localDirectory: '',
		targetSubdirectory: ''
	});
	let remoteTargetSubdirectory = $state('');
	let remoteDatasetType = $state<'EMAIL_ARCHIVE' | 'CSV_TEXT' | 'GENERIC_FILES'>('CSV_TEXT');
	let remoteFiles = $state<File[]>([]);
	let deleteFilePath = $state('');

	let selectedDataset = $derived(datasets.find((dataset) => dataset.id === selectedDatasetId) ?? null);
	let selectedDatasetSupportsDashboard = $derived(
		selectedDataset?.datasetType === 'CSV_TEXT' || selectedDataset?.datasetType === 'EMAIL_ARCHIVE'
	);

	function setMessage(text: string, type: 'info' | 'success' | 'error' = 'info') {
		message = { text, type };
	}

	function toMessage(error: unknown) {
		return error instanceof Error ? error.message : 'Unexpected error';
	}

	function normalizeHdfsPath(path: string) {
		const normalized = path.trim().replaceAll('\\', '/');
		const schemeIndex = normalized.indexOf('://');
		if (schemeIndex < 0) {
			return normalized;
		}
		const pathStart = normalized.indexOf('/', schemeIndex + 3);
		return pathStart >= 0 ? normalized.slice(pathStart) : '/';
	}

	function storedPositiveInt(key: string, fallback: number): number {
		const rawValue = localStorage.getItem(key);
		const parsedValue = rawValue == null ? Number.NaN : Number.parseInt(rawValue, 10);
		return Number.isFinite(parsedValue) && parsedValue > 0 ? parsedValue : fallback;
	}

	function applyProgressSummary(progress: DashboardProgressEvent) {
		progressMessage = progress.message;
		progressScannedFiles = progress.scannedFiles;
		progressTotalFiles = progress.totalFiles;
		progressProcessedRows = progress.processedRows;
		progressFailedFiles = progress.failedFiles;
		progressFiles = progress.files;
	}

	function visibleSummaryItems() {
		if (!dashboard) {
			return [];
		}
		if (!dashboardProgress) {
			return dashboard.summaryItems;
		}

		return dashboard.summaryItems.map((item) => {
			if (item.label === 'Scanned files') {
				return { ...item, value: String(progressScannedFiles) };
			}
			if (item.label === 'Processed rows') {
				return { ...item, value: String(progressProcessedRows) };
			}
			if (item.label === 'Failed files') {
				return { ...item, value: String(progressFailedFiles) };
			}
			return item;
		});
	}

	function clearProgressSummary() {
		progressMessage = '';
		progressScannedFiles = 0;
		progressTotalFiles = 0;
		progressProcessedRows = 0;
		progressFailedFiles = 0;
		progressFiles = [];
		hasLivePartialDashboard = false;
	}

	function summaryInt(label: string): number {
		const rawValue = dashboard?.summaryItems.find((item) => item.label === label)?.value ?? '';
		const parsedValue = Number.parseInt(rawValue.replaceAll(',', ''), 10);
		return Number.isFinite(parsedValue) ? parsedValue : 0;
	}

	function updateDashboardChartShell(charts: DashboardChart[], force = false) {
		if (!dashboard || charts.length === 0) {
			return;
		}

		const now = performance.now();
		if (!force && now - lastChartShellUpdate < chartShellUpdateIntervalMs) {
			return;
		}

		const chartById = new Map(charts.map((chart) => [chart.id, chart]));
		dashboard = {
			...dashboard,
			charts: dashboard.charts.map((chart) => {
				const updatedChart = chartById.get(chart.id);
				return updatedChart ? { ...updatedChart, type: chart.type } : chart;
			})
		};
		lastChartShellUpdate = now;
	}

	function closeProgressSocket() {
		if (progressSocket) {
			progressSocket.onclose = null;
			progressSocket.onerror = null;
			progressSocket.onmessage = null;
			progressSocket.close();
			progressSocket = null;
		}
	}

	function handleProgress(progress: DashboardProgressEvent) {
		if (progress.datasetId !== selectedDatasetId) {
			return;
		}
		if (progress.stage === 'connected') {
			return;
		}

		applyProgressSummary(progress);
		dashboardProgress = { ...progress, charts: [], dashboard: null };
		if (progress.dashboard || progress.charts?.length) {
			hasLivePartialDashboard = true;
		}

		if (progress.dashboard) {
			dashboard = progress.dashboard;
			lastChartShellUpdate = performance.now();
		} else if (!dashboard && progress.charts?.length && selectedDataset) {
			dashboard = {
				datasetId: selectedDataset.id,
				datasetName: selectedDataset.name,
				datasetType: selectedDataset.datasetType,
				hdfsPath: selectedDataset.hdfsPath,
				generatedAt: new Date().toISOString(),
				maxFiles,
				summaryItems: [],
				charts: progress.charts,
				columnProfiles: [],
				listPanel: null,
				tablePanel: null
			};
			lastChartShellUpdate = performance.now();
		} else if (progress.charts?.length) {
			updateDashboardChartShell(progress.charts, progress.complete);
		}

		if (progress.charts?.length) {
			requestAnimationFrame(() => {
				window.dispatchEvent(new CustomEvent<DashboardChart[]>('dashboard-charts:update', { detail: progress.charts }));
			});
		}

		setMessage(progress.message, progress.complete ? 'success' : 'info');
	}

	function openProgressSocket(datasetId: string) {
		closeProgressSocket();
		const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
		const url = `${protocol}//${window.location.host}/ws/dashboard-progress?datasetId=${encodeURIComponent(datasetId)}`;
		const socket = new WebSocket(url);
		progressSocket = socket;
		socket.onmessage = (event) => {
			try {
				handleProgress(JSON.parse(event.data) as DashboardProgressEvent);
			} catch {
				// Ignore non-JSON control frames.
			}
		};
		socket.onerror = () => setMessage('Live dashboard progress is unavailable; the dashboard request is still running.', 'info');
	}

	async function restRequest<T>(url: string, init: RequestInit = {}): Promise<T> {
		const response = await fetch(url, init);
		if (response.status === 204) {
			return undefined as T;
		}

		const contentType = response.headers.get('content-type') ?? '';
		const payload = contentType.includes('application/json') ? await response.json() : await response.text();
		if (!response.ok) {
			throw new Error(typeof payload === 'string' ? payload : payload.detail ?? response.statusText);
		}
		return payload as T;
	}

	function hdfsPathMessage(dataset: DatasetView, createdVerb: string) {
		return dataset.hdfsPathAlreadyExisted
			? `${createdVerb} ${dataset.name}. Warning: HDFS path ${dataset.hdfsPath} already existed, so existing files may be reused or overwritten by imports.`
			: `${createdVerb} ${dataset.name}. Created HDFS path ${dataset.hdfsPath}.`;
	}

	function selectableValues(chart: DashboardChart): string[] {
		if (chart.series.length > 1) {
			return chart.series.map((series) => series.name);
		}

		if (chart.series.length === 1) {
			return chart.series[0].points.length <= focusValueLimit
				? chart.series[0].points.map((point) => point.label)
				: [];
		}

		return [];
	}

	function selectedValues(chart: DashboardChart): string[] {
		return chartFocusOverrides[chart.id] ?? selectableValues(chart);
	}

	function setChartType(chartId: string, mode: string) {
		chartTypeOverrides = {
			...chartTypeOverrides,
			[chartId]: mode as ChartMode
		};
	}

	function handleChartTypeChange(chartId: string, event: Event): void {
		const target = event.currentTarget;
		if (target instanceof HTMLSelectElement) {
			setChartType(chartId, target.value);
		}
	}

	function toggleChartValue(chart: DashboardChart, value: string) {
		const available = selectableValues(chart);
		if (available.length === 0) {
			return;
		}
		const current = selectedValues(chart);
		const next = current.includes(value)
			? current.filter((entry) => entry !== value)
			: [...current, value];

		chartFocusOverrides = {
			...chartFocusOverrides,
			[chart.id]: next.length === 0 ? available : next
		};
	}

	function resolvedChart(chart: DashboardChart): DashboardChart {
		const type = chartTypeOverrides[chart.id] ?? chart.type;

		if (chart.series.length > 1) {
			const focusedValues = new Set(selectedValues(chart));
			return {
				...chart,
				type,
				series: chart.series.filter((series) => focusedValues.has(series.name))
			};
		}

		if (chart.series.length === 1) {
			const focusedValues = chartFocusOverrides[chart.id];
			if (!focusedValues || chart.series[0].points.length > focusValueLimit) {
				return { ...chart, type };
			}
			const focusedValueSet = new Set(focusedValues);
			return {
				...chart,
				type,
				series: [
					{
						...chart.series[0],
						points: chart.series[0].points.filter((point) => focusedValueSet.has(point.label))
					}
				]
			};
		}

		return { ...chart, type };
	}

	async function loadDatasets() {
		isLoadingDatasets = true;
		try {
			const data = await graphqlRequest<{ datasets: DatasetView[] }>(DATASETS_QUERY);
			datasets = data.datasets;
			if (datasets.length > 0 && !datasets.some((dataset) => dataset.id === selectedDatasetId)) {
				selectedDatasetId = datasets[0].id;
			}
			if (datasets.length === 0) {
				selectedDatasetId = '';
				dashboard = null;
				files = [];
			}
			if (selectedDatasetId) {
				await loadFiles();
			}
		} finally {
			isLoadingDatasets = false;
		}
	}

	async function loadFiles() {
		if (!selectedDatasetId) {
			files = [];
			return;
		}
		try {
			files = await restRequest<HdfsFileDescriptor[]>(`/api/datasets/${selectedDatasetId}/files?limit=200&recursive=true`);
		} catch (error) {
			files = [];
			setMessage(toMessage(error), 'error');
		}
	}

	async function handleDatasetSelectionChange() {
		dashboardRequestSequence++;
		localStorage.setItem(selectedDatasetStorageKey, selectedDatasetId);
		dashboard = null;
		dashboardProgress = null;
		isLoadingDashboard = false;
		clearProgressSummary();
		closeProgressSocket();
		deleteFilePath = '';
		await loadFiles();
		if (selectedDatasetId) {
			openProgressSocket(selectedDatasetId);
		}
	}

	async function loadDashboard() {
		if (!selectedDatasetId) {
			setMessage('Register or select a dataset before loading analytics.', 'error');
			return;
		}
		if (!selectedDatasetSupportsDashboard) {
			setMessage('Import or upload files as CSV_TEXT or EMAIL_ARCHIVE before loading analytics.', 'error');
			return;
		}
		if (files.length === 0) {
			setMessage('This dataset has no files in HDFS. Import or upload files before loading analytics.', 'error');
			return;
		}

		isLoadingDashboard = true;
		const requestSequence = ++dashboardRequestSequence;
		activeDashboardDatasetId = selectedDatasetId;
		dashboardProgress = null;
		clearProgressSummary();
		localStorage.setItem(updateEveryRowsStorageKey, String(updateEveryRows));
		localStorage.setItem(fullDashboardUpdateEveryRowsStorageKey, String(fullDashboardUpdateEveryRows));
		openProgressSocket(selectedDatasetId);
		setMessage('Loading dashboard...', 'info');
		try {
			const requestedDatasetId = selectedDatasetId;
			const data = await graphqlRequest<{ dashboard: DashboardView }>(DASHBOARD_QUERY, {
				datasetId: selectedDatasetId,
				maxFiles,
				updateEveryRows,
				fullDashboardUpdateEveryRows,
				refresh
			});
			if (requestSequence !== dashboardRequestSequence || selectedDatasetId !== requestedDatasetId) {
				return;
			}
			dashboard = data.dashboard;
			progressMessage = 'Dashboard analytics ready.';
			progressScannedFiles = summaryInt('Scanned files');
			progressProcessedRows = summaryInt('Processed rows');
			progressFailedFiles = summaryInt('Failed files');
			const currentProgress = dashboardProgress;
			if (currentProgress) {
				dashboardProgress = Object.assign({}, currentProgress, { message: progressMessage, complete: true });
			}
			chartTypeOverrides = {};
			chartFocusOverrides = {};
			setMessage(`Loaded ${data.dashboard.datasetName}.`, 'success');
		} catch (error) {
			if (requestSequence !== dashboardRequestSequence) {
				return;
			}
			setMessage(toMessage(error), 'error');
		} finally {
			if (requestSequence === dashboardRequestSequence) {
				isLoadingDashboard = false;
				activeDashboardDatasetId = '';
				closeProgressSocket();
			}
		}
	}

	async function cancelDashboard(datasetId = activeDashboardDatasetId, silent = false) {
		if (!datasetId) {
			return;
		}
		try {
			await fetch(`/api/datasets/${datasetId}/dashboard/cancel`, {
				method: 'POST',
				keepalive: true
			});
			if (!silent) {
				setMessage('Cancelled dashboard processing.', 'info');
			}
		} catch (error) {
			if (!silent) {
				setMessage(toMessage(error), 'error');
			}
		} finally {
			if (activeDashboardDatasetId === datasetId) {
				activeDashboardDatasetId = '';
			}
		}
	}

	async function registerDataset() {
		isRegistering = true;
		setMessage('Registering dataset...', 'info');
		try {
			const data = await graphqlRequest<{ registerDataset: DatasetView }>(REGISTER_MUTATION, {
				input: form
			});
			await loadDatasets();
			selectedDatasetId = data.registerDataset.id;
			localStorage.setItem(selectedDatasetStorageKey, selectedDatasetId);
			form = {
				name: '',
				description: '',
				hdfsPath: ''
			};
			setMessage(hdfsPathMessage(data.registerDataset, 'Registered dataset'), 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isRegistering = false;
		}
	}

	async function importLocalDirectory() {
		if (!selectedDatasetId) {
			setMessage('Create or select a dataset before importing files.', 'error');
			return;
		}
		isImporting = true;
		setMessage('Importing server directory into the selected dataset...', 'info');
		try {
			files = await restRequest<HdfsFileDescriptor[]>('/api/datasets/import-local', {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify({ ...importForm, datasetId: selectedDatasetId })
			});
			importForm = {
				datasetId: selectedDatasetId,
				datasetType: importForm.datasetType,
				localDirectory: '',
				targetSubdirectory: importForm.targetSubdirectory
			};
			dashboard = null;
			await loadDatasets();
			setMessage(`Imported directory into ${selectedDataset?.name ?? 'dataset'}.`, 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isImporting = false;
		}
	}

	function handleRemoteFileChange(event: Event) {
		const target = event.currentTarget;
		if (target instanceof HTMLInputElement) {
			remoteFiles = Array.from(target.files ?? []);
		}
	}

	async function importRemoteFiles() {
		if (!selectedDatasetId) {
			setMessage('Create or select a dataset before uploading files.', 'error');
			return;
		}
		if (remoteFiles.length === 0) {
			setMessage('Choose at least one file to upload.', 'error');
			return;
		}

		isUploading = true;
		setMessage('Uploading files into the selected dataset...', 'info');
		try {
			const formData = new FormData();
			remoteFiles.forEach((file) => formData.append('files', file));
			if (remoteTargetSubdirectory.trim()) {
				formData.append('targetSubdirectory', remoteTargetSubdirectory.trim());
			}
			formData.append('datasetType', remoteDatasetType);
			files = await restRequest<HdfsFileDescriptor[]>(`/api/datasets/${selectedDatasetId}/import-remote`, {
				method: 'POST',
				body: formData
			});
			dashboard = null;
			await loadDatasets();
			setMessage(`Uploaded ${remoteFiles.length} file(s) into ${selectedDataset?.name ?? 'dataset'}.`, 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isUploading = false;
		}
	}

	async function deleteDatasetFile(path = deleteFilePath) {
		if (!selectedDatasetId || !path) {
			setMessage('Select a dataset file to delete.', 'error');
			return;
		}

		isDeleting = true;
		setMessage('Deleting dataset file...', 'info');
		try {
			const normalizedPath = normalizeHdfsPath(path);
			await restRequest<void>(`/api/datasets/${selectedDatasetId}/files?path=${encodeURIComponent(normalizedPath)}`, {
				method: 'DELETE'
			});
			deleteFilePath = '';
			dashboard = null;
			await loadFiles();
			setMessage('Deleted file from dataset.', 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isDeleting = false;
		}
	}

	onMount(async () => {
		try {
			selectedDatasetId = localStorage.getItem(selectedDatasetStorageKey) ?? '';
			updateEveryRows = storedPositiveInt(updateEveryRowsStorageKey, updateEveryRows);
			fullDashboardUpdateEveryRows = storedPositiveInt(fullDashboardUpdateEveryRowsStorageKey, fullDashboardUpdateEveryRows);
			await loadDatasets();
			if (selectedDatasetId) {
				openProgressSocket(selectedDatasetId);
			}
			setMessage('Ready. Register a dataset or load one from the list.', 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		}
	});

	onDestroy(() => {
		closeProgressSocket();
	});
</script>

<svelte:head>
	<title>Dataset Visualization Dashboard</title>
	<meta
		name="description"
		content="Register HDFS datasets and render analytics dashboards from GraphQL-powered APIs."
	/>
</svelte:head>

<div class="page-shell">
	<section class="hero panel">
		<div>
			<p class="eyebrow">Visualization-first analytics</p>
			<h1>Dataset visualization cockpit</h1>
			<p class="lede">
				Register HDFS datasets, keep GraphQL/HTTP for control operations, and use the live WebSocket
				channel for long-running dashboard progress.
			</p>
		</div>
		<div class="hero-meta">
			<div>
				<span class="meta-label">Datasets</span>
				<strong>{datasets.length}</strong>
			</div>
			<div>
				<span class="meta-label">Endpoint</span>
				<strong>/graphql</strong>
			</div>
			<div>
				<span class="meta-label">Live channel</span>
				<strong>/ws/dashboard-progress</strong>
			</div>
		</div>
	</section>

	<section class="panel control-panel">
		<div class="panel-heading">
			<div>
				<p class="eyebrow">Dataset manager</p>
				<h2>Register or select dataset</h2>
			</div>
			<button class="primary-button" type="button" onclick={registerDataset} disabled={isRegistering}>
				{isRegistering ? 'Registering…' : 'Register dataset'}
			</button>
		</div>

		<div class="form-grid">
			<label>
				<span>Name</span>
				<input bind:value={form.name} placeholder="dataset-name" required />
			</label>
			<label>
				<span>HDFS path</span>
				<input bind:value={form.hdfsPath} placeholder="/data/covid" required />
			</label>
			<label>
				<span>Description</span>
				<input bind:value={form.description} placeholder="Dataset stored in HDFS" />
			</label>
		</div>

		{#if datasets.length > 0}
			<div class="manager-divider"></div>
			<div class="form-grid compact-grid">
				<label>
					<span>Existing dataset</span>
					<select bind:value={selectedDatasetId} onchange={handleDatasetSelectionChange}>
						{#each datasets as dataset}
							<option value={dataset.id}>{dataset.name}</option>
						{/each}
					</select>
				</label>
				<label class="wide-field">
					<span>Dataset HDFS root</span>
					<input value={selectedDataset?.hdfsPath ?? ''} readonly />
				</label>
			</div>
		{:else}
			<div class="flow-note">Register a dataset before importing files or opening the dashboard manager.</div>
		{/if}
	</section>

	{#if selectedDataset}
		<section class="panel control-panel">
			<div class="panel-heading">
				<div>
					<p class="eyebrow">File manager</p>
					<h2>Import, replace, or delete files</h2>
				</div>
				<button class="secondary-button" type="button" onclick={loadFiles}>Refresh files</button>
			</div>

			<div class="flow-note">
				Files are managed under <strong>{selectedDataset.hdfsPath}</strong>. Choose the processing type here; uploading or importing the same file path replaces the existing file.
			</div>

			<div class="form-grid">
				<label>
					<span>Processing type</span>
					<select bind:value={importForm.datasetType}>
						<option value="CSV_TEXT">CSV_TEXT</option>
						<option value="EMAIL_ARCHIVE">EMAIL_ARCHIVE</option>
						<option value="GENERIC_FILES">GENERIC_FILES</option>
					</select>
				</label>
				<label>
					<span>Server directory</span>
					<input bind:value={importForm.localDirectory} placeholder="/mnt/main/trung/Text/Data" />
				</label>
				<label>
					<span>Target subdirectory</span>
					<input bind:value={importForm.targetSubdirectory} placeholder="optional/subdir" />
				</label>
				<div class="button-row wide-field">
					<button class="primary-button" type="button" onclick={importLocalDirectory} disabled={isImporting}>
						{isImporting ? 'Importing…' : 'Import server directory'}
					</button>
				</div>
				<label>
					<span>Client files</span>
					<input type="file" multiple onchange={handleRemoteFileChange} />
				</label>
				<label>
					<span>Processing type</span>
					<select bind:value={remoteDatasetType}>
						<option value="CSV_TEXT">CSV_TEXT</option>
						<option value="EMAIL_ARCHIVE">EMAIL_ARCHIVE</option>
						<option value="GENERIC_FILES">GENERIC_FILES</option>
					</select>
				</label>
				<label>
					<span>Upload subdirectory</span>
					<input bind:value={remoteTargetSubdirectory} placeholder="optional/subdir" />
				</label>
				<div class="button-row wide-field">
					<button class="primary-button" type="button" onclick={importRemoteFiles} disabled={isUploading}>
						{isUploading ? 'Uploading…' : `Upload ${remoteFiles.length || ''} file${remoteFiles.length === 1 ? '' : 's'}`}
					</button>
				</div>
			</div>

			<div class="file-list">
				<div class="file-list-heading">
					<span>Dataset files</span>
					<strong>{files.length}</strong>
				</div>
				{#if files.length === 0}
					<p>No files imported yet.</p>
				{:else}
					{#each files as file}
						<div class="file-row">
							<div>
								<strong>{file.name}</strong>
								<span>{file.path}</span>
							</div>
							<button class="danger-button" type="button" onclick={() => deleteDatasetFile(file.path)} disabled={isDeleting}>Delete</button>
						</div>
					{/each}
				{/if}
			</div>
		</section>

		<section class="panel control-panel">
			<div class="panel-heading">
				<div>
					<p class="eyebrow">Dashboard manager</p>
					<h2>Process and visualize data</h2>
				</div>
				<button
					class="primary-button"
					type="button"
					onclick={loadDashboard}
					disabled={isLoadingDashboard || isLoadingDatasets || !selectedDatasetSupportsDashboard || files.length === 0}
				>
					{isLoadingDashboard ? 'Loading…' : 'Load dashboard'}
				</button>
			</div>

			<div class="form-grid compact-grid">
				<label>
					<span>Max files</span>
					<input bind:value={maxFiles} type="number" min="1" />
				</label>
				<label>
					<span>Update every rows</span>
					<input bind:value={updateEveryRows} type="number" min="50" step="50" />
				</label>
				<label>
					<span>Full dashboard update rows</span>
					<input bind:value={fullDashboardUpdateEveryRows} type="number" min="50" step="50" />
				</label>
				<label class="checkbox-field">
					<input bind:checked={refresh} type="checkbox" />
					<span>Force refresh</span>
				</label>
			</div>

			<div class="flow-note dashboard-note">
				Max files limits how many files are scanned. Update every rows controls lightweight row/file progress and live chart data. Full dashboard update rows controls list/table/preview/focus-picker refresh cadence.
			</div>
			{#if !selectedDatasetSupportsDashboard}
				<div class="flow-note dashboard-note warning-note">
					This dataset is still marked as {selectedDataset.datasetType}. Import or upload files with CSV_TEXT or EMAIL_ARCHIVE selected before loading analytics.
				</div>
			{:else if files.length === 0}
				<div class="flow-note dashboard-note warning-note">
					No files are visible under this dataset HDFS root. Import or upload files before loading analytics.
				</div>
			{/if}

			{#if isLoadingDashboard || dashboardProgress}
				<div class="progress-panel">
					<div class="progress-copy">
						<strong>{progressMessage || 'Starting dashboard load...'}</strong>
						<span>
							{progressScannedFiles}/{progressTotalFiles} files · {progressProcessedRows} rows · {progressFailedFiles} failed
						</span>
					</div>
					{#if hasLivePartialDashboard && !dashboardProgress?.complete}
						<div class="stream-note">Charts and previews below are live partial results and will continue updating until the scan completes.</div>
					{/if}
					{#if progressFiles.length}
						<div class="file-progress-grid">
							{#each progressFiles as file}
								<article class={`file-progress-card ${file.status}`}>
									<div>
										<strong>{file.name}</strong>
										<span>{file.status}</span>
									</div>
									<p>{file.processedRows} rows processed</p>
									<small>{file.message}</small>
								</article>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		</section>
	{/if}

	<div class={`message ${message.type}`}>{message.text}</div>

	{#if dashboard}
		<section class="summary-grid">
			{#each visibleSummaryItems() as item}
				<article class="summary-card panel">
					<span class="meta-label">{item.label}</span>
					<strong>{item.value}</strong>
				</article>
			{/each}
		</section>

		{#if dashboard.columnProfiles.length > 0}
			<section class="panel preview-panel">
				<div class="panel-heading small-heading">
					<div>
						<p class="eyebrow">Column preview</p>
						<h2>First 10 values by detected type</h2>
					</div>
					<span class="preview-count">{dashboard.columnProfiles.length} columns</span>
				</div>
				<div class="column-preview-grid">
					{#each dashboard.columnProfiles as column}
						<article class="column-preview-card">
							<div>
								<strong>{column.name}</strong>
								<span>{column.type}</span>
							</div>
							{#if column.sampleValues.length > 0}
								<ol>
									{#each column.sampleValues as value}
										<li>{value}</li>
									{/each}
								</ol>
							{:else}
								<p>No non-empty values in scanned rows.</p>
							{/if}
						</article>
					{/each}
				</div>
			</section>
		{/if}

		<section class="chart-grid">
			{#each dashboard.charts as chart (`${chart.id}:${chartTypeOverrides[chart.id] ?? chart.type}`)}
				{@const focusValues = selectableValues(chart)}
				{@const displayChart = resolvedChart(chart)}
				<article class="panel chart-card">
					<div class="chart-tools">
						<label>
							<span>Visualization</span>
							<select value={chartTypeOverrides[chart.id] ?? chart.type} onchange={(event: Event) => handleChartTypeChange(chart.id, event)}>
								{#each chartModes as mode}
									<option value={mode}>{mode}</option>
								{/each}
							</select>
						</label>

						{#if focusValues.length > 0}
							<div class="focus-picker">
								<span>Focus</span>
								<div class="focus-chips">
									{#each focusValues as value}
										<button
											type="button"
											class:selected={selectedValues(chart).includes(value)}
											onclick={() => toggleChartValue(chart, value)}
										>
											{value}
										</button>
									{/each}
								</div>
							</div>
						{/if}
					</div>
					{#key displayChart.type}
						<ChartPanel chart={displayChart} mode={displayChart.type} />
					{/key}
				</article>
			{/each}
		</section>

		<section class="bottom-grid">
			{#if dashboard.listPanel}
				<article class="panel list-panel">
					<div class="panel-heading small-heading">
						<div>
							<p class="eyebrow">List panel</p>
							<h2>{dashboard.listPanel.title}</h2>
						</div>
					</div>
					<ul>
						{#each dashboard.listPanel.items as item}
							<li>{item}</li>
						{/each}
					</ul>
				</article>
			{/if}

			{#if dashboard.tablePanel}
				<article class="panel table-panel">
					<div class="panel-heading small-heading">
						<div>
							<p class="eyebrow">Table panel</p>
							<h2>{dashboard.tablePanel.title}</h2>
						</div>
					</div>
					<div class="table-wrapper">
						<table>
							<thead>
								<tr>
									{#each dashboard.tablePanel.columns as column}
										<th>{column}</th>
									{/each}
								</tr>
							</thead>
							<tbody>
								{#each dashboard.tablePanel.rows as row}
									<tr>
										{#each row.cells as cell}
											<td>{cell}</td>
										{/each}
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</article>
			{/if}
		</section>
	{:else if selectedDataset}
		<section class="panel empty-state">
			<h2>No dashboard loaded</h2>
			<p>
				Import files if needed, then use the dashboard manager to process and visualize this dataset.
			</p>
		</section>
	{/if}
</div>

<style>
	:global(body) {
		margin: 0;
		font-family:
			Inter,
			system-ui,
			sans-serif;
		background:
			radial-gradient(circle at top, rgba(110, 168, 254, 0.16), transparent 28%),
			#07111f;
		color: #eef2ff;
	}

	.page-shell {
		max-width: 1280px;
		margin: 0 auto;
		padding: 2rem 1.25rem 4rem;
		display: grid;
		gap: 1.25rem;
	}

	.panel {
		background: rgba(7, 17, 31, 0.86);
		border: 1px solid rgba(255, 255, 255, 0.08);
		border-radius: 1.2rem;
		box-shadow: 0 30px 80px rgba(0, 0, 0, 0.28);
		backdrop-filter: blur(18px);
	}

	.hero {
		padding: 1.8rem;
		display: flex;
		justify-content: space-between;
		gap: 2rem;
		align-items: end;
	}

	.eyebrow {
		margin: 0 0 0.45rem;
		text-transform: uppercase;
		letter-spacing: 0.14em;
		font-size: 0.72rem;
		color: #8fb2ff;
	}

	h1,
	h2 {
		margin: 0;
	}

	h1 {
		font-size: clamp(2rem, 4vw, 3.25rem);
		line-height: 1.02;
	}

	.lede {
		max-width: 55rem;
		margin: 0.75rem 0 0;
		color: #aab7d8;
		font-size: 1rem;
		line-height: 1.6;
	}

	.hero-meta {
		display: grid;
		gap: 0.85rem;
		grid-template-columns: repeat(3, minmax(0, 1fr));
	}

	.hero-meta {
		min-width: 260px;
	}

	.meta-label {
		display: block;
		font-size: 0.75rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
		color: #8b94b4;
		margin-bottom: 0.35rem;
	}

	.hero-meta strong,
	.summary-card strong {
		font-size: 1rem;
		font-weight: 600;
	}

	.control-panel,
	.empty-state {
		padding: 1.4rem;
	}

	.panel-heading {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: center;
		margin-bottom: 1rem;
	}

	.small-heading {
		margin-bottom: 1.1rem;
	}

	.form-grid {
		display: grid;
		gap: 0.9rem;
		grid-template-columns: repeat(2, minmax(0, 1fr));
	}

	.flow-note {
		margin: -0.2rem 0 1rem;
		color: #aab7d8;
		line-height: 1.5;
	}

	.dashboard-note {
		margin: 1rem 0 0;
	}

	.warning-note {
		padding: 0.85rem 1rem;
		border-radius: 0.9rem;
		background: rgba(255, 196, 87, 0.1);
		border: 1px solid rgba(255, 196, 87, 0.22);
		color: #ffe0a3;
	}

	.manager-divider {
		height: 1px;
		margin: 1.2rem 0;
		background: rgba(255, 255, 255, 0.08);
	}

	.wide-field {
		grid-column: 1 / -1;
	}

	.button-row {
		display: flex;
		gap: 0.75rem;
		align-items: center;
	}

	.compact-grid {
		grid-template-columns: 2fr 1fr 1fr auto;
		align-items: end;
	}

	label {
		display: grid;
		gap: 0.45rem;
		font-size: 0.88rem;
		color: #aab7d8;
	}

	input,
	select,
	button {
		font: inherit;
	}

	input,
	select {
		padding: 0.85rem 0.95rem;
		border-radius: 0.85rem;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(255, 255, 255, 0.03);
		color: #eef2ff;
	}

	input::placeholder {
		color: #6f7a99;
	}

	input[readonly] {
		color: #b9c6e7;
	}

	option {
		color: #07111f;
	}

	.checkbox-field {
		display: flex;
		align-items: center;
		gap: 0.7rem;
		padding-bottom: 0.2rem;
	}

	.checkbox-field input {
		width: 1rem;
		height: 1rem;
		margin: 0;
	}

	.primary-button {
		border: 0;
		border-radius: 999px;
		padding: 0.85rem 1.2rem;
		background: linear-gradient(135deg, #6ea8fe, #8d5cf6);
		color: white;
		font-weight: 600;
		cursor: pointer;
	}

	.secondary-button,
	.danger-button {
		border-radius: 999px;
		padding: 0.75rem 1rem;
		font-weight: 600;
		cursor: pointer;
	}

	.secondary-button {
		border: 1px solid rgba(255, 255, 255, 0.12);
		background: rgba(255, 255, 255, 0.04);
		color: #dfe7ff;
	}

	.danger-button {
		border: 1px solid rgba(255, 123, 123, 0.45);
		background: rgba(255, 123, 123, 0.1);
		color: #ffd1d1;
	}

	.primary-button:disabled,
	.secondary-button:disabled,
	.danger-button:disabled {
		opacity: 0.65;
		cursor: wait;
	}

	.file-list {
		margin-top: 1.25rem;
		display: grid;
		gap: 0.65rem;
	}

	.file-list-heading,
	.file-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
	}

	.file-list-heading {
		color: #aab7d8;
	}

	.file-row {
		padding: 0.8rem 0.9rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
	}

	.file-row div {
		display: grid;
		gap: 0.25rem;
		min-width: 0;
	}

	.file-row span {
		color: #8b94b4;
		font-size: 0.82rem;
		overflow-wrap: anywhere;
	}

	.progress-panel {
		margin-top: 1rem;
		padding: 1rem;
		border-radius: 1rem;
		background: rgba(110, 168, 254, 0.08);
		border: 1px solid rgba(110, 168, 254, 0.18);
		display: grid;
		gap: 0.8rem;
	}

	.progress-copy {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		color: #cddcff;
		flex-wrap: wrap;
	}

	.progress-copy span {
		color: #8fb2ff;
	}

	.stream-note {
		padding: 0.75rem 0.85rem;
		border-radius: 0.8rem;
		background: rgba(88, 214, 141, 0.1);
		color: #bdf3d0;
	}

	.file-progress-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
		gap: 0.7rem;
	}

	.file-progress-card {
		padding: 0.85rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
		border: 1px solid rgba(255, 255, 255, 0.08);
		display: grid;
		gap: 0.45rem;
	}

	.file-progress-card div {
		display: flex;
		justify-content: space-between;
		gap: 0.75rem;
		align-items: start;
	}

	.file-progress-card span {
		padding: 0.18rem 0.55rem;
		border-radius: 999px;
		background: rgba(255, 255, 255, 0.08);
		color: #dfe7ff;
		font-size: 0.72rem;
		font-weight: 700;
		text-transform: uppercase;
	}

	.file-progress-card p,
	.file-progress-card small {
		margin: 0;
		color: #aab7d8;
	}

	.file-progress-card.processing {
		border-color: rgba(110, 168, 254, 0.5);
		background: rgba(110, 168, 254, 0.1);
	}

	.file-progress-card.complete {
		border-color: rgba(88, 214, 141, 0.38);
	}

	.file-progress-card.failed {
		border-color: rgba(255, 123, 123, 0.5);
		background: rgba(255, 123, 123, 0.1);
	}

	.message {
		padding: 0.95rem 1.1rem;
		border-radius: 1rem;
		border: 1px solid rgba(255, 255, 255, 0.08);
	}

	.message.info {
		background: rgba(110, 168, 254, 0.12);
		color: #cddcff;
	}

	.message.success {
		background: rgba(88, 214, 141, 0.12);
		color: #bdf3d0;
	}

	.message.error {
		background: rgba(255, 123, 123, 0.12);
		color: #ffd1d1;
	}

	.summary-grid {
		display: grid;
		gap: 0.9rem;
		grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
	}

	.summary-card {
		padding: 1rem;
	}

	.preview-panel {
		padding: 1.25rem;
	}

	.preview-count {
		color: #8fb2ff;
		font-weight: 700;
	}

	.column-preview-grid {
		display: grid;
		gap: 0.85rem;
		grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
		max-height: 520px;
		overflow: auto;
		padding-right: 0.25rem;
	}

	.column-preview-card {
		padding: 0.95rem;
		border-radius: 1rem;
		background: rgba(255, 255, 255, 0.04);
		border: 1px solid rgba(255, 255, 255, 0.06);
		display: grid;
		gap: 0.75rem;
	}

	.column-preview-card div {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
	}

	.column-preview-card span {
		padding: 0.2rem 0.55rem;
		border-radius: 999px;
		background: rgba(141, 92, 246, 0.16);
		color: #d8ccff;
		font-size: 0.72rem;
		font-weight: 700;
	}

	.column-preview-card ol {
		margin: 0;
		padding-left: 1.35rem;
		display: grid;
		gap: 0.35rem;
		color: #dfe7ff;
		font-size: 0.86rem;
	}

	.column-preview-card li,
	.column-preview-card p {
		overflow-wrap: anywhere;
	}

	.column-preview-card p {
		margin: 0;
		color: #8b94b4;
	}

	.chart-grid {
		display: grid;
		gap: 1rem;
		grid-template-columns: repeat(2, minmax(0, 1fr));
	}

	.chart-card,
	.list-panel,
	.table-panel {
		padding: 1.25rem;
	}

	.chart-tools {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: start;
		margin-bottom: 1rem;
		flex-wrap: wrap;
	}

	.chart-tools label {
		min-width: 180px;
	}

	.focus-picker {
		display: grid;
		gap: 0.45rem;
		color: #aab7d8;
	}

	.focus-picker > span {
		font-size: 0.88rem;
	}

	.focus-chips {
		display: flex;
		gap: 0.5rem;
		flex-wrap: wrap;
	}

	.focus-chips button {
		padding: 0.45rem 0.75rem;
		border-radius: 999px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(255, 255, 255, 0.04);
		color: #b9c6e7;
		font-size: 0.84rem;
		font-weight: 500;
	}

	.focus-chips button.selected {
		background: rgba(110, 168, 254, 0.2);
		border-color: rgba(110, 168, 254, 0.55);
		color: #eef2ff;
	}

	.bottom-grid {
		display: grid;
		gap: 1rem;
		grid-template-columns: minmax(280px, 0.95fr) minmax(0, 1.35fr);
	}

	.list-panel ul {
		list-style: none;
		padding: 0;
		margin: 0;
		display: grid;
		gap: 0.7rem;
	}

	.list-panel li {
		padding: 0.8rem 0.9rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
		color: #d9e2ff;
	}

	.table-wrapper {
		overflow-x: auto;
	}

	table {
		width: 100%;
		border-collapse: collapse;
	}

	th,
	td {
		padding: 0.9rem 0.75rem;
		border-bottom: 1px solid rgba(255, 255, 255, 0.08);
		text-align: left;
	}

	th {
		font-size: 0.78rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
		color: #8b94b4;
	}

	td {
		color: #dfe7ff;
	}

	.empty-state {
		text-align: center;
		padding-block: 3rem;
	}

	.empty-state p {
		max-width: 44rem;
		margin: 0.9rem auto 0;
		color: #aab7d8;
		line-height: 1.6;
	}

	@media (max-width: 1100px) {
		.chart-grid,
		.bottom-grid,
		.compact-grid,
		.hero,
		.hero-meta {
			grid-template-columns: 1fr;
			display: grid;
		}

		.hero {
			align-items: start;
		}
	}

	@media (max-width: 720px) {
		.page-shell {
			padding-inline: 0.9rem;
		}

		.form-grid {
			grid-template-columns: 1fr;
		}

		.chart-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
