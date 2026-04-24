<script lang="ts">
	import type { DashboardChart, DashboardPoint, DashboardSeries } from '$lib/types';

	let { chart } = $props<{ chart: DashboardChart }>();

	const width = 640;
	const height = 280;
	const marginTop = 20;
	const marginRight = 16;
	const marginBottom = 44;
	const marginLeft = 44;
	const palette = ['#6ea8fe', '#58d68d', '#f7b267', '#ff7b7b', '#c084fc'];

	function labels(): string[] {
		return Array.from(
			new Set(chart.series.flatMap((series: DashboardSeries) => series.points.map((point: DashboardPoint) => point.label)))
		);
	}

	function maxValue(): number {
		return Math.max(
			1,
			...chart.series.flatMap((series: DashboardSeries) => series.points.map((point: DashboardPoint) => point.value))
		);
	}

	function valueAt(series: DashboardSeries, label: string) {
		return series.points.find((point) => point.label === label)?.value ?? 0;
	}

	function chartWidth() {
		return width - marginLeft - marginRight;
	}

	function chartHeight() {
		return height - marginTop - marginBottom;
	}

	function xForLabel(index: number, count: number) {
		if (count <= 1) {
			return marginLeft + chartWidth() / 2;
		}
		return marginLeft + (index / (count - 1)) * chartWidth();
	}

	function yForValue(value: number) {
		return marginTop + chartHeight() - (value / maxValue()) * chartHeight();
	}

	function linePoints(series: DashboardSeries) {
		const allLabels = labels();
		return allLabels
			.map((label, index) => `${xForLabel(index, allLabels.length)},${yForValue(valueAt(series, label))}`)
			.join(' ');
	}

	function yTicks() {
		return [0, 0.25, 0.5, 0.75, 1].map((ratio) => ({
			label: Math.round(maxValue() * ratio),
			y: marginTop + chartHeight() - chartHeight() * ratio
		}));
	}

	function barWidth(labelCount: number, seriesCount: number) {
		const safeLabels = Math.max(labelCount, 1);
		const safeSeries = Math.max(seriesCount, 1);
		return Math.max(10, (chartWidth() / safeLabels) * 0.72 / safeSeries);
	}
</script>

<div class="chart-panel">
	<div class="chart-header">
		<h3>{chart.title}</h3>
		{#if chart.series.length > 1}
			<div class="legend">
				{#each chart.series as series, index}
					<span><i style={`background:${palette[index % palette.length]}`}></i>{series.name}</span>
				{/each}
			</div>
		{/if}
	</div>

	{#if chart.series.length === 0 || labels().length === 0}
		<div class="empty-chart">No chart data available.</div>
	{:else}
		<svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={chart.title}>
			{#each yTicks() as tick}
				<line class="grid-line" x1={marginLeft} y1={tick.y} x2={width - marginRight} y2={tick.y}></line>
				<text class="axis-label" x={marginLeft - 10} y={tick.y + 4}>{tick.label}</text>
			{/each}

			{#if chart.type === 'LINE'}
				{#each chart.series as series, index}
					<polyline
						fill="none"
						stroke={palette[index % palette.length]}
						stroke-width="3"
						points={linePoints(series)}
					></polyline>
					{#each labels() as label, pointIndex}
						<circle
							cx={xForLabel(pointIndex, labels().length)}
							cy={yForValue(valueAt(series, label))}
							r="4"
							fill={palette[index % palette.length]}
						></circle>
					{/each}
				{/each}
			{:else}
				{@const allLabels = labels()}
				{@const computedBarWidth = barWidth(allLabels.length, chart.series.length)}
				{@const groupWidth = chartWidth() / Math.max(allLabels.length, 1)}
				{#each chart.series as series, seriesIndex}
					{#each allLabels as label, labelIndex}
						{@const value = valueAt(series, label)}
						{@const barHeight = (value / maxValue()) * chartHeight()}
						<rect
							x={marginLeft + labelIndex * groupWidth + groupWidth * 0.14 + seriesIndex * computedBarWidth}
							y={marginTop + chartHeight() - barHeight}
							width={computedBarWidth - 4}
							height={barHeight}
							rx="4"
							fill={palette[seriesIndex % palette.length]}
						></rect>
					{/each}
				{/each}
			{/if}

			{#each labels() as label, index}
				<text class="axis-label x-axis-label" x={xForLabel(index, labels().length)} y={height - 12}>{label}</text>
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

	.legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem;
		justify-content: flex-end;
		font-size: 0.78rem;
		color: #8b94b4;
	}

	.legend span {
		display: inline-flex;
		gap: 0.45rem;
		align-items: center;
	}

	.legend i {
		width: 0.75rem;
		height: 0.75rem;
		display: inline-block;
		border-radius: 999px;
	}

	svg {
		width: 100%;
		height: auto;
		overflow: visible;
	}

	.grid-line {
		stroke: rgba(255, 255, 255, 0.08);
		stroke-width: 1;
	}

	.axis-label {
		fill: #8b94b4;
		font-size: 11px;
		text-anchor: end;
	}

	.x-axis-label {
		text-anchor: middle;
	}

	.empty-chart {
		min-height: 220px;
		display: grid;
		place-items: center;
		border-radius: 1rem;
		border: 1px dashed rgba(255, 255, 255, 0.14);
		color: #8b94b4;
	}

	@media (max-width: 720px) {
		.chart-header {
			grid-template-columns: 1fr;
			display: grid;
		}

		.legend {
			justify-content: flex-start;
		}
	}
</style>
