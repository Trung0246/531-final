export type DatasetType = 'EMAIL_ARCHIVE' | 'CSV_TEXT' | 'GENERIC_FILES';
export type ChartMode = 'BAR' | 'LINE' | 'TABLE' | 'HISTOGRAM' | 'DONUT' | 'MISSINGNESS';

export interface DatasetView {
	id: string;
	name: string;
	description: string | null;
	datasetType: DatasetType;
	hdfsPath: string;
	registeredAt: string;
	hdfsPathAlreadyExisted: boolean | null;
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
	availableModes: ChartMode[];
	semanticType: string | null;
}

export interface DashboardColumnProfile {
	name: string;
	type: string;
	sampleValues: string[];
	blankCount: number;
	nonBlankCount: number;
	distinctCount: number;
	topValues: DashboardPoint[];
	histogramBuckets: DashboardPoint[];
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
	columnProfiles: DashboardColumnProfile[];
	listPanel: DashboardListPanel | null;
	tablePanel: DashboardTablePanel | null;
}

export interface DashboardProgressEvent {
	datasetId: string;
	stage: string;
	message: string;
	scannedFiles: number;
	totalFiles: number;
	processedRows: number;
	failedFiles: number;
	files: DashboardFileProgress[];
	charts: DashboardChart[];
	dashboard: DashboardView | null;
	complete: boolean;
}

export interface DashboardFileProgress {
	path: string;
	name: string;
	status: 'queued' | 'processing' | 'complete' | 'failed' | string;
	processedRows: number;
	message: string;
}

export interface RegisterDatasetInput {
	name: string;
	description: string;
	hdfsPath: string;
}

export interface ImportLocalDirectoryInput {
	datasetId: string;
	datasetType: DatasetType;
	localDirectory: string;
	targetSubdirectory: string;
}

export interface HdfsFileDescriptor {
	path: string;
	name: string;
	directory: boolean;
	length: number;
	modificationTime: string;
}
