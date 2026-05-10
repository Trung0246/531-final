<script lang="ts">
	import type { DashboardChart, DashboardPoint, DashboardSeries } from '$lib/types';

	let { chart, mode } = $props<{ chart: DashboardChart; mode?: DashboardChart['type'] }>();

	const useVega = import.meta.env.VITE_DASHBOARD_CHART_RENDERER === 'vega';

	let vegaHost = $state<HTMLDivElement | null>(null);
	let vegaView: { finalize: () => void } | null = null;
	let latestChart = $state<DashboardChart | null>(null);
	let renderMode = $derived(mode ?? chart.type);
	let currentChart = $derived(latestChart ?? chart);
	let chartLabels = $derived(labels(currentChart));
	let chartData = $derived(toChartData(currentChart));
	let hasRenderableData = $derived(currentChart.series.length > 0 && chartLabels.length > 0);
	let maxValue = $derived(Math.max(1, ...chartData.map((point) => point.value)));
	let donutPoints = $derived<DashboardPoint[]>(currentChart.series[0]?.points ?? []);
	let donutTotal = $derived(donutPoints.reduce((total: number, point: DashboardPoint) => total + Math.max(0, point.value), 0));

	const width = 640;
	const height = 280;
	const padding = { top: 18, right: 24, bottom: 48, left: 52 };
	const plotWidth = width - padding.left - padding.right;
	const plotHeight = height - padding.top - padding.bottom;
	const palette = ['#6ea8fe', '#58d68d', '#f7b267', '#ff7b7b', '#c084fc'];

	type ChartDatum = DashboardPoint & {
		series: string;
		seriesIndex: number;
		labelIndex: number;
	};

	function labels(input: DashboardChart): string[] {
		return Array.from(
			new Set(input.series.flatMap((series: DashboardSeries) => series.points.map((point: DashboardPoint) => point.label)))
		);
	}

	function toChartData(input: DashboardChart): ChartDatum[] {
		const allLabels = labels(input);
		return input.series.flatMap((series, seriesIndex) =>
			series.points.map((point) => ({
				...point,
				series: series.name,
				seriesIndex,
				labelIndex: allLabels.indexOf(point.label)
			}))
		);
	}

	function chartHasData(input: DashboardChart): boolean {
		return input.series.length > 0 && labels(input).length > 0;
	}

	function tableRows(): string[][] {
		if (currentChart.series.length <= 1) {
			const firstSeries = currentChart.series[0];
			return firstSeries ? firstSeries.points.map((point: DashboardPoint) => [point.label, String(point.value)]) : [];
		}

		return chartLabels.map((label) => [
			label,
			...currentChart.series.map((series: DashboardSeries) => String(series.points.find((point: DashboardPoint) => point.label === label)?.value ?? 0))
		]);
	}

	function xForLabel(labelIndex: number): number {
		if (chartLabels.length <= 1) {
			return padding.left + plotWidth / 2;
		}
		return padding.left + (labelIndex / (chartLabels.length - 1)) * plotWidth;
	}

	function yForValue(value: number): number {
		return padding.top + plotHeight - (value / maxValue) * plotHeight;
	}

	function barX(point: ChartDatum): number {
		const labelBand = plotWidth / Math.max(1, chartLabels.length);
		const seriesCount = Math.max(1, currentChart.series.length);
		const barWidth = labelBand / seriesCount;
		return padding.left + point.labelIndex * labelBand + point.seriesIndex * barWidth + barWidth * 0.12;
	}

	function barWidth(): number {
		const labelBand = plotWidth / Math.max(1, chartLabels.length);
		return Math.max(2, (labelBand / Math.max(1, currentChart.series.length)) * 0.76);
	}

	function linePoints(series: DashboardSeries): string {
		return series.points
			.map((point) => `${xForLabel(chartLabels.indexOf(point.label))},${yForValue(point.value)}`)
			.join(' ');
	}

	function visibleLabels(): string[] {
		if (chartLabels.length <= 10) {
			return chartLabels;
		}
		const step = Math.ceil(chartLabels.length / 10);
		return chartLabels.filter((_, index) => index % step === 0);
	}

	function polarToCartesian(centerX: number, centerY: number, radius: number, angle: number) {
		const radians = ((angle - 90) * Math.PI) / 180;
		return {
			x: centerX + radius * Math.cos(radians),
			y: centerY + radius * Math.sin(radians)
		};
	}

	function donutArc(startAngle: number, endAngle: number): string {
		const centerX = width / 2;
		const centerY = height / 2;
		const outerRadius = 105;
		const innerRadius = 58;
		const outerStart = polarToCartesian(centerX, centerY, outerRadius, endAngle);
		const outerEnd = polarToCartesian(centerX, centerY, outerRadius, startAngle);
		const innerStart = polarToCartesian(centerX, centerY, innerRadius, startAngle);
		const innerEnd = polarToCartesian(centerX, centerY, innerRadius, endAngle);
		const largeArc = endAngle - startAngle <= 180 ? '0' : '1';

		return [
			`M ${outerStart.x} ${outerStart.y}`,
			`A ${outerRadius} ${outerRadius} 0 ${largeArc} 0 ${outerEnd.x} ${outerEnd.y}`,
			`L ${innerStart.x} ${innerStart.y}`,
			`A ${innerRadius} ${innerRadius} 0 ${largeArc} 1 ${innerEnd.x} ${innerEnd.y}`,
			'Z'
		].join(' ');
	}

	function donutStart(index: number): number {
		if (donutTotal <= 0) {
			return 0;
		}
		return donutPoints.slice(0, index).reduce((total: number, point: DashboardPoint) => total + Math.max(0, point.value), 0) / donutTotal * 360;
	}

	function vegaData(input: DashboardChart) {
		return input.series.flatMap((series) =>
			series.points.map((point) => ({
				label: point.label,
				value: point.value,
				series: series.name
			}))
		);
	}

	function vegaSpec(input: DashboardChart, selectedMode: string) {
		const values = vegaData(input);
		return {
			$schema: 'https://vega.github.io/schema/vega/v6.json',
			background: 'transparent',
			width,
			height,
			padding: { top: 8, right: 20, bottom: 46, left: 48 },
			data: [{ name: 'table', values }],
			scales: [
				{
					name: 'x',
					type: selectedMode === 'LINE' ? 'point' : 'band',
					domain: { data: 'table', field: 'label' },
					range: 'width',
					padding: selectedMode === 'LINE' ? 0.5 : 0.2
				},
				{ name: 'y', type: 'linear', domain: { data: 'table', field: 'value' }, nice: true, zero: true, range: 'height' },
				{ name: 'color', type: 'ordinal', domain: { data: 'table', field: 'series' }, range: palette }
			],
			axes: [
				{ orient: 'bottom', scale: 'x', labelColor: '#8b94b4', domainColor: 'rgba(255,255,255,0.14)', tickColor: 'rgba(255,255,255,0.14)' },
				{ orient: 'left', scale: 'y', labelColor: '#8b94b4', gridColor: 'rgba(255,255,255,0.08)', domain: false, tickColor: 'rgba(255,255,255,0.14)' }
			],
			legends: input.series.length > 1 ? [{ fill: 'color', orient: 'top', labelColor: '#8b94b4' }] : [],
			marks:
				selectedMode === 'LINE'
					? [
							{
								type: 'group',
								from: { facet: { name: 'seriesFacet', data: 'table', groupby: 'series' } },
								marks: [
									{
										type: 'line',
										from: { data: 'seriesFacet' },
										encode: { update: { x: { scale: 'x', field: 'label' }, y: { scale: 'y', field: 'value' }, stroke: { scale: 'color', field: 'series' }, strokeWidth: { value: 3 } } }
									}
								]
							}
						]
					: [
							{
								type: 'rect',
								from: { data: 'table' },
								encode: { update: { x: { scale: 'x', field: 'label' }, width: { scale: 'x', band: 0.7 }, y: { scale: 'y', field: 'value' }, y2: { scale: 'y', value: 0 }, fill: { scale: 'color', field: 'series' } } }
							}
						]
		};
	}

	$effect(() => {
		const host = vegaHost;
		const selectedMode = renderMode;
		const selectedChart = currentChart;
		if (!useVega || !host || selectedMode === 'TABLE' || selectedMode === 'DONUT' || !chartHasData(selectedChart)) {
			vegaView?.finalize();
			vegaView = null;
			return;
		}

		let disposed = false;
		vegaView?.finalize();
		vegaView = null;
		host.innerHTML = '';
		void import('vega-embed').then(({ default: embed }) =>
			embed(host, vegaSpec(selectedChart, selectedMode) as never, { actions: false, renderer: 'canvas', theme: 'dark' })
				.then((result) => {
					if (disposed) {
						result.finalize();
						return;
					}
					vegaView = result.view;
				})
		);

		return () => {
			disposed = true;
			vegaView?.finalize();
			vegaView = null;
		};
	});

	$effect(() => {
		function handleChartUpdate(event: Event) {
			if (!(event instanceof CustomEvent)) {
				return;
			}
			const updatedChart = (event.detail as DashboardChart[]).find((entry) => entry.id === chart.id);
			if (updatedChart) {
				latestChart = updatedChart;
			}
		}

		window.addEventListener('dashboard-charts:update', handleChartUpdate);
		return () => window.removeEventListener('dashboard-charts:update', handleChartUpdate);
	});

	$effect(() => {
		chart;
		latestChart = null;
	});
</script>

<div class="chart-panel">
	<div class="chart-header">
		<h3>{chart.title}</h3>
	</div>

	{#if renderMode === 'TABLE'}
		{#if !hasRenderableData}
			<div class="empty-chart">No chart data available.</div>
		{:else}
			<div class="table-wrapper">
				<table>
					<thead>
						<tr>
							{#if currentChart.series.length <= 1}
								<th>Label</th>
								<th>{currentChart.series[0]?.name ?? 'Value'}</th>
							{:else}
								<th>Label</th>
								{#each currentChart.series as series}
									<th>{series.name}</th>
								{/each}
							{/if}
						</tr>
					</thead>
					<tbody>
						{#each tableRows() as row}
							<tr>
								{#each row as cell}
									<td>{cell}</td>
								{/each}
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{:else if !hasRenderableData}
		<div class="empty-chart">No chart data available.</div>
	{:else if renderMode === 'DONUT'}
		<svg class="chart-svg donut-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={chart.title}>
			{#if donutTotal <= 0}
				<text class="axis-label" x={width / 2} y={height / 2} text-anchor="middle">No positive values</text>
			{:else}
				{#each donutPoints as point, index}
					{@const startAngle = donutStart(index)}
					{@const endAngle = startAngle + (Math.max(0, point.value) / donutTotal) * 360}
					<path d={donutArc(startAngle, endAngle)} fill={palette[index % palette.length]}>
						<title>{point.label}: {point.value}</title>
					</path>
				{/each}
				<text class="donut-total" x={width / 2} y={height / 2 - 4} text-anchor="middle">{donutTotal}</text>
				<text class="axis-label" x={width / 2} y={height / 2 + 18} text-anchor="middle">total</text>
			{/if}
		</svg>
		<div class="legend-grid">
			{#each donutPoints.slice(0, 8) as point, index}
				<span><i style={`background: ${palette[index % palette.length]}`}></i>{point.label}</span>
			{/each}
		</div>
	{:else if useVega}
		<div bind:this={vegaHost} class="vega-host"></div>
	{:else}
		<svg class="chart-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label={chart.title}>
			<line class="axis" x1={padding.left} y1={padding.top + plotHeight} x2={padding.left + plotWidth} y2={padding.top + plotHeight} />
			<line class="axis" x1={padding.left} y1={padding.top} x2={padding.left} y2={padding.top + plotHeight} />

			{#if renderMode === 'LINE'}
				{#each currentChart.series as series, seriesIndex}
					<polyline class="line-series" points={linePoints(series)} stroke={palette[seriesIndex % palette.length]} />
					{#each series.points as point}
						<circle cx={xForLabel(chartLabels.indexOf(point.label))} cy={yForValue(point.value)} r="3" fill={palette[seriesIndex % palette.length]} />
					{/each}
				{/each}
			{:else}
				{#each chartData as point}
					<rect
						x={barX(point)}
						y={yForValue(point.value)}
						width={barWidth()}
						height={padding.top + plotHeight - yForValue(point.value)}
						fill={palette[point.seriesIndex % palette.length]}
						rx="3"
					/>
				{/each}
			{/if}

			{#each visibleLabels() as label}
				<text class="axis-label" x={xForLabel(chartLabels.indexOf(label))} y={height - 14} text-anchor="middle">{label}</text>
			{/each}
		</svg>
	{/if}
</div>

<style>
	.chart-panel {
		display: grid;
		gap: 1rem;
	}

	.chart-header {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: start;
	}

	h3 {
		margin: 0;
		font-size: 1rem;
		font-weight: 600;
	}

	.chart-svg {
		width: 100%;
		min-height: 280px;
		overflow: visible;
	}

	.vega-host {
		width: 100%;
		min-height: 280px;
	}

	:global(.vega-host .vega-actions) {
		display: none;
	}

	:global(.vega-host canvas) {
		max-width: 100%;
		height: auto;
	}

	.axis {
		stroke: rgba(255, 255, 255, 0.18);
		stroke-width: 1;
	}

	.axis-label {
		fill: #8b94b4;
		font-size: 10px;
	}

	.line-series {
		fill: none;
		stroke-width: 3;
		stroke-linejoin: round;
		stroke-linecap: round;
	}

	.donut-svg path {
		stroke: #111827;
		stroke-width: 2;
	}

	.donut-total {
		fill: #f8fbff;
		font-size: 1.45rem;
		font-weight: 700;
	}

	.legend-grid {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem 0.85rem;
		color: #c8d3f2;
		font-size: 0.78rem;
	}

	.legend-grid span {
		display: inline-flex;
		align-items: center;
		gap: 0.35rem;
	}

	.legend-grid i {
		display: inline-block;
		width: 0.65rem;
		height: 0.65rem;
		border-radius: 999px;
	}

	.empty-chart {
		min-height: 220px;
		display: grid;
		place-items: center;
		border-radius: 1rem;
		border: 1px dashed rgba(255, 255, 255, 0.14);
		color: #8b94b4;
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
		padding: 0.75rem;
		border-bottom: 1px solid rgba(255, 255, 255, 0.08);
		text-align: left;
	}

	th {
		font-size: 0.76rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
		color: #8b94b4;
	}

	td {
		color: #dfe7ff;
	}
</style>
