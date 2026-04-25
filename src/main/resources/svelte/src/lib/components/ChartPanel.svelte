<script lang="ts">
	import embed from 'vega-embed';
	import type { VisualizationSpec } from 'vega-embed';
	import { changeset, type View } from 'vega';
	import type { DashboardChart, DashboardPoint, DashboardSeries } from '$lib/types';

	let { chart } = $props<{ chart: DashboardChart }>();

	let chartHost = $state<HTMLDivElement | null>(null);
	let view: View | null = null;
	let viewReady = $state(0);
	let renderMode = $derived(chart.type);
	let showLegend = $derived(chart.series.length > 1);

	const width = 640;
	const height = 280;
	const palette = ['#6ea8fe', '#58d68d', '#f7b267', '#ff7b7b', '#c084fc'];

	type VegaDatum = {
		label: string;
		value: number;
		series: string;
	};

	function labels(): string[] {
		return Array.from(
			new Set(chart.series.flatMap((series: DashboardSeries) => series.points.map((point: DashboardPoint) => point.label)))
		);
	}

	function toVegaData(): VegaDatum[] {
		return chart.series.flatMap((series: DashboardSeries) =>
			series.points.map((point: DashboardPoint) => ({
				label: point.label,
				value: point.value,
				series: series.name
			}))
		);
	}

	function tableRows(): string[][] {
		const allLabels = labels();
		if (chart.series.length <= 1) {
			const firstSeries = chart.series[0];
			return firstSeries ? firstSeries.points.map((point: DashboardPoint) => [point.label, String(point.value)]) : [];
		}

		return allLabels.map((label: string) => [
			label,
			...chart.series.map((series: DashboardSeries) => String(series.points.find((point) => point.label === label)?.value ?? 0))
		]);
	}

	function vegaSpec(mode: string, includeLegend: boolean): VisualizationSpec {
		if (mode === 'LINE') {
			return {
				$schema: 'https://vega.github.io/schema/vega/v5.json',
				background: 'transparent',
				width,
				height,
				padding: { top: 8, right: 20, bottom: 46, left: 48 },
				data: [{ name: 'table', values: [] }],
				scales: [
					{ name: 'x', type: 'point', domain: { data: 'table', field: 'label' }, range: 'width', padding: 0.5 },
					{ name: 'y', type: 'linear', domain: { data: 'table', field: 'value' }, nice: true, zero: true, range: 'height' },
					{ name: 'color', type: 'ordinal', domain: { data: 'table', field: 'series' }, range: palette }
				],
				axes: [
					{ orient: 'bottom', scale: 'x', labelColor: '#8b94b4', domainColor: 'rgba(255,255,255,0.14)', tickColor: 'rgba(255,255,255,0.14)' },
					{ orient: 'left', scale: 'y', labelColor: '#8b94b4', gridColor: 'rgba(255,255,255,0.08)', domain: false, tickColor: 'rgba(255,255,255,0.14)' }
				],
				legends: includeLegend ? [{ fill: 'color', orient: 'top', labelColor: '#8b94b4' }] : [],
				marks: [
					{
						type: 'group',
						from: { facet: { name: 'seriesFacet', data: 'table', groupby: 'series' } },
						marks: [
							{
								type: 'line',
								from: { data: 'seriesFacet' },
								encode: {
									update: {
										x: { scale: 'x', field: 'label' },
										y: { scale: 'y', field: 'value' },
										stroke: { scale: 'color', field: 'series' },
										strokeWidth: { value: 3 }
									}
								}
							},
							{
								type: 'symbol',
								from: { data: 'seriesFacet' },
								encode: {
									update: {
										x: { scale: 'x', field: 'label' },
										y: { scale: 'y', field: 'value' },
										size: { value: 64 },
										fill: { scale: 'color', field: 'series' }
									}
								}
							}
						]
					}
				]
			};
		}

		return {
			$schema: 'https://vega.github.io/schema/vega/v5.json',
			background: 'transparent',
			width,
			height,
			padding: { top: 8, right: 20, bottom: 46, left: 48 },
			data: [{ name: 'table', values: [] }],
			scales: [
				{ name: 'x', type: 'band', domain: { data: 'table', field: 'label' }, range: 'width', paddingInner: 0.2, paddingOuter: 0.08 },
				{ name: 'y', type: 'linear', domain: { data: 'table', field: 'value' }, nice: true, zero: true, range: 'height' },
				{ name: 'color', type: 'ordinal', domain: { data: 'table', field: 'series' }, range: palette }
			],
			axes: [
				{ orient: 'bottom', scale: 'x', labelColor: '#8b94b4', domainColor: 'rgba(255,255,255,0.14)', tickColor: 'rgba(255,255,255,0.14)' },
				{ orient: 'left', scale: 'y', labelColor: '#8b94b4', gridColor: 'rgba(255,255,255,0.08)', domain: false, tickColor: 'rgba(255,255,255,0.14)' }
			],
			legends: includeLegend ? [{ fill: 'color', orient: 'top', labelColor: '#8b94b4' }] : [],
			marks: [
				{
					type: 'group',
					from: { facet: { name: 'labelFacet', data: 'table', groupby: 'label' } },
					encode: {
						update: {
							x: { scale: 'x', field: 'label' },
							width: { scale: 'x', band: 1 }
						}
					},
					scales: [
						{ name: 'series', type: 'band', domain: { data: 'labelFacet', field: 'series' }, range: 'width', padding: 0.14 }
					],
					marks: [
						{
							type: 'rect',
							from: { data: 'labelFacet' },
							encode: {
								update: {
									x: { scale: 'series', field: 'series' },
									width: { scale: 'series', band: 1 },
									y: { scale: 'y', field: 'value' },
									y2: { scale: 'y', value: 0 },
									fill: { scale: 'color', field: 'series' },
									cornerRadiusTopLeft: { value: 4 },
									cornerRadiusTopRight: { value: 4 }
								}
							}
						}
					]
				}
			]
		};
	}

	function updateVegaData() {
		if (!view) {
			return;
		}

		void view
			.change('table', changeset().remove(() => true).insert(toVegaData()))
			.runAsync()
			.catch(() => {
				if (chartHost) {
					chartHost.innerHTML = '<div class="empty-chart">Unable to update chart.</div>';
				}
			});
	}

	$effect(() => {
		const host = chartHost;
		const mode = renderMode;
		const includeLegend = showLegend;

		if (!host || mode === 'TABLE') {
			view?.finalize();
			view = null;
			return;
		}

		let disposed = false;
		view?.finalize();
		view = null;
		viewReady += 1;
		host.innerHTML = '';

		void embed(host, vegaSpec(mode, includeLegend), {
			actions: false,
			renderer: 'canvas',
			theme: 'dark'
		})
			.then((result) => {
				if (disposed) {
					void result.finalize();
					return;
				}
				view = result.view;
				viewReady += 1;
			})
			.catch(() => {
				if (host) {
					host.innerHTML = '<div class="empty-chart">Unable to render chart.</div>';
				}
			});

		return () => {
			disposed = true;
			view?.finalize();
			view = null;
			if (host) {
				host.innerHTML = '';
			}
		};
	});

	$effect(() => {
		viewReady;
		if (chart.type === 'TABLE' || chart.series.length === 0 || labels().length === 0) {
			return;
		}
		updateVegaData();
	});
</script>

<div class="chart-panel">
	<div class="chart-header">
		<h3>{chart.title}</h3>
	</div>

	{#if chart.series.length === 0 || labels().length === 0}
		<div class="empty-chart">No chart data available.</div>
	{:else if chart.type === 'TABLE'}
		<div class="table-wrapper">
			<table>
				<thead>
					<tr>
						{#if chart.series.length <= 1}
							<th>Label</th>
							<th>{chart.series[0]?.name ?? 'Value'}</th>
						{:else}
							<th>Label</th>
							{#each chart.series as series}
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
	{:else}
		<div bind:this={chartHost} class="vega-host"></div>
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

	:global(.vega-host .vega-actions) {
		display: none;
	}

	:global(.vega-host svg) {
		width: 100%;
		height: auto;
		overflow: visible;
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
