<script lang="ts">
	import type { DashboardChart, DashboardPoint, DashboardSeries } from '$lib/types';

	let { chart, mode } = $props<{ chart: DashboardChart; mode?: DashboardChart['type'] }>();

	let latestChart = $state<DashboardChart | null>(null);
	let renderMode = $derived(mode ?? chart.type);
	let currentChart = $derived(latestChart ?? chart);
	let chartLabels = $derived(labels(currentChart));
	let chartData = $derived(toChartData(currentChart));
	let hasRenderableData = $derived(currentChart.series.length > 0 && chartLabels.length > 0);
	let maxValue = $derived(Math.max(1, ...chartData.map((point) => point.value)));

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
