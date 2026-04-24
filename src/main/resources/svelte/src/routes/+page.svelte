<script lang="ts">
	import { onMount } from 'svelte';
	import ChartPanel from '$lib/components/ChartPanel.svelte';
	import { graphqlRequest } from '$lib/graphql';
	import type { ChartMode, DashboardChart, DashboardView, DatasetView, RegisterDatasetInput } from '$lib/types';

	const DATASETS_QUERY = `
		query Datasets {
			datasets {
				id
				name
				description
				datasetType
				hdfsPath
				registeredAt
			}
		}
	`;

	const DASHBOARD_QUERY = `
		query Dashboard($datasetId: ID!, $maxFiles: Int, $refresh: Boolean!) {
			dashboard(datasetId: $datasetId, maxFiles: $maxFiles, refresh: $refresh) {
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
			}
		}
	`;

	const chartModes: ChartMode[] = ['BAR', 'LINE', 'TABLE'];
	type MessageState = { text: string; type: 'info' | 'success' | 'error' };

	let datasets = $state<DatasetView[]>([]);
	let dashboard = $state<DashboardView | null>(null);
	let chartTypeOverrides = $state<Record<string, ChartMode>>({});
	let chartFocusOverrides = $state<Record<string, string[]>>({});
	let selectedDatasetId = $state('');
	let maxFiles = $state(5000);
	let refresh = $state(false);
	let isRegistering = $state(false);
	let isLoadingDashboard = $state(false);
	let isLoadingDatasets = $state(false);
	let message = $state<MessageState>({ text: '', type: 'info' });
	let form = $state<RegisterDatasetInput>({
		name: '',
		description: '',
		datasetType: 'EMAIL_ARCHIVE',
		hdfsPath: ''
	});

	let selectedDataset = $derived(datasets.find((dataset) => dataset.id === selectedDatasetId) ?? null);

	function setMessage(text: string, type: 'info' | 'success' | 'error' = 'info') {
		message = { text, type };
	}

	function toMessage(error: unknown) {
		return error instanceof Error ? error.message : 'Unexpected error';
	}

	function selectableValues(chart: DashboardChart): string[] {
		if (chart.series.length > 1) {
			return chart.series.map((series) => series.name);
		}

		if (chart.series.length === 1) {
			return chart.series[0].points.map((point) => point.label);
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
		const focusedValues = selectedValues(chart);

		if (chart.series.length > 1) {
			return {
				...chart,
				type,
				series: chart.series.filter((series) => focusedValues.includes(series.name))
			};
		}

		if (chart.series.length === 1) {
			return {
				...chart,
				type,
				series: [
					{
						...chart.series[0],
						points: chart.series[0].points.filter((point) => focusedValues.includes(point.label))
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
			}
		} finally {
			isLoadingDatasets = false;
		}
	}

	async function loadDashboard() {
		if (!selectedDatasetId) {
			setMessage('Register or select a dataset before loading analytics.', 'error');
			return;
		}

		isLoadingDashboard = true;
		setMessage('Loading dashboard...', 'info');
		try {
			const data = await graphqlRequest<{ dashboard: DashboardView }>(DASHBOARD_QUERY, {
				datasetId: selectedDatasetId,
				maxFiles,
				refresh
			});
			dashboard = data.dashboard;
			chartTypeOverrides = {};
			chartFocusOverrides = {};
			setMessage(`Loaded ${data.dashboard.datasetName}.`, 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isLoadingDashboard = false;
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
			form = {
				name: '',
				description: '',
				datasetType: form.datasetType,
				hdfsPath: ''
			};
			setMessage(`Registered dataset ${data.registerDataset.name}.`, 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		} finally {
			isRegistering = false;
		}
	}

	onMount(async () => {
		try {
			await loadDatasets();
			setMessage('Ready. Register a dataset or load one from the list.', 'success');
		} catch (error) {
			setMessage(toMessage(error), 'error');
		}
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
				Register HDFS datasets, fetch analytics through GraphQL, and let the backend describe the
				dashboard layout.
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
		</div>
	</section>

	<section class="panel control-panel">
		<div class="panel-heading">
			<div>
				<p class="eyebrow">Register dataset</p>
				<h2>GraphQL mutation</h2>
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
				<input bind:value={form.hdfsPath} placeholder="/datasets/example" required />
			</label>
			<label>
				<span>Description</span>
				<input bind:value={form.description} placeholder="Dataset stored in HDFS" />
			</label>
			<label>
				<span>Dataset type</span>
				<select bind:value={form.datasetType}>
					<option value="EMAIL_ARCHIVE">EMAIL_ARCHIVE</option>
					<option value="CSV_TEXT">CSV_TEXT</option>
				</select>
			</label>
		</div>
	</section>

	<section class="panel control-panel">
		<div class="panel-heading">
			<div>
				<p class="eyebrow">Load dashboard</p>
				<h2>Query-driven layout</h2>
			</div>
			<button class="primary-button" type="button" onclick={loadDashboard} disabled={isLoadingDashboard || isLoadingDatasets}>
				{isLoadingDashboard ? 'Loading…' : 'Load dashboard'}
			</button>
		</div>

		<div class="form-grid compact-grid">
			<label>
				<span>Dataset</span>
				<select bind:value={selectedDatasetId} disabled={datasets.length === 0}>
					{#if datasets.length === 0}
						<option value="">No datasets registered</option>
					{:else}
						{#each datasets as dataset}
							<option value={dataset.id}>{dataset.name} • {dataset.datasetType}</option>
						{/each}
					{/if}
				</select>
			</label>
			<label>
				<span>Dataset type</span>
				<input value={selectedDataset?.datasetType ?? ''} readonly />
			</label>
			<label>
				<span>Max files</span>
				<input bind:value={maxFiles} type="number" min="1" />
			</label>
			<label class="checkbox-field">
				<input bind:checked={refresh} type="checkbox" />
				<span>Force refresh</span>
			</label>
		</div>

		{#if selectedDataset}
			<div class="dataset-callout">
				<div>
					<span class="meta-label">Selected dataset</span>
					<strong>{selectedDataset.name}</strong>
				</div>
				<div>
					<span class="meta-label">HDFS path</span>
					<strong>{selectedDataset.hdfsPath}</strong>
				</div>
				<div>
					<span class="meta-label">Registered</span>
					<strong>{selectedDataset.registeredAt}</strong>
				</div>
			</div>
		{/if}
	</section>

	<div class={`message ${message.type}`}>{message.text}</div>

	{#if dashboard}
		<section class="summary-grid">
			{#each dashboard.summaryItems as item}
				<article class="summary-card panel">
					<span class="meta-label">{item.label}</span>
					<strong>{item.value}</strong>
				</article>
			{/each}
		</section>

		<section class="chart-grid">
			{#each dashboard.charts as chart}
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

						{#if selectableValues(chart).length > 0}
							<div class="focus-picker">
								<span>Focus</span>
								<div class="focus-chips">
									{#each selectableValues(chart) as value}
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
					<ChartPanel chart={displayChart} />
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
	{:else}
		<section class="panel empty-state">
			<h2>No dashboard loaded</h2>
			<p>
				The new Svelte client waits for the backend to describe the summary cards and chart panels.
				Choose a dataset and run the dashboard query when you are ready.
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

	.hero-meta,
	.dataset-callout {
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
	.dataset-callout strong,
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

	.primary-button:disabled {
		opacity: 0.65;
		cursor: wait;
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
		.hero-meta,
		.dataset-callout {
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
