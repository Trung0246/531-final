export type DatasetType = 'EMAIL_ARCHIVE' | 'CSV_TEXT' | 'GENERIC_FILES';
export type ChartMode = 'BAR' | 'LINE' | 'TABLE';

export interface DatasetView {
	id: string;
	name: string;
	description: string | null;
	datasetType: DatasetType;
	hdfsPath: string;
	registeredAt: string;
}

export interface DashboardSummaryItem {
	label: string;
	value: string;
}

export interface DashboardPoint {
	label: string;
	value: number;
}

export interface DashboardSeries {
	name: string;
	points: DashboardPoint[];
}

export interface DashboardChart {
	id: string;
	title: string;
	type: ChartMode;
	series: DashboardSeries[];
}

export interface DashboardListPanel {
	title: string;
	items: string[];
}

export interface DashboardTableRow {
	cells: string[];
}

export interface DashboardTablePanel {
	title: string;
	columns: string[];
	rows: DashboardTableRow[];
}

export interface DashboardView {
	datasetId: string;
	datasetName: string;
	datasetType: DatasetType;
	hdfsPath: string;
	generatedAt: string;
	maxFiles: number;
	summaryItems: DashboardSummaryItem[];
	charts: DashboardChart[];
	listPanel: DashboardListPanel | null;
	tablePanel: DashboardTablePanel | null;
}

export interface RegisterDatasetInput {
	name: string;
	description: string;
	datasetType: DatasetType;
	hdfsPath: string;
}
