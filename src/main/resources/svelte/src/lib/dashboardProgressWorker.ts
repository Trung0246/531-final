import type { DashboardProgressEvent } from './types';

type WorkerCommand =
	| { type: 'connect'; url: string }
	| { type: 'close' };

type WorkerUpdate =
	| { type: 'summary'; progress: DashboardProgressEvent }
	| { type: 'progress'; progress: DashboardProgressEvent }
	| { type: 'error'; message: string }
	| { type: 'closed' };

const FLUSH_INTERVAL_MS = 700;

let socket: WebSocket | null = null;
let latestProgress: DashboardProgressEvent | null = null;
let flushTimer: ReturnType<typeof setTimeout> | null = null;

function post(update: WorkerUpdate) {
	self.postMessage(update);
}

function toSummary(progress: DashboardProgressEvent): DashboardProgressEvent {
	return {
		...progress,
		charts: [],
		dashboard: null
	};
	}

function clearFlushTimer() {
	if (flushTimer) {
		clearTimeout(flushTimer);
		flushTimer = null;
	}
}

function flushLatest() {
	clearFlushTimer();
	if (!latestProgress) {
		return;
	}
	post({ type: 'progress', progress: latestProgress });
	latestProgress = null;
}

function scheduleFlush(progress: DashboardProgressEvent) {
	post({ type: 'summary', progress: toSummary(progress) });
	if (!progress.charts?.length && !progress.dashboard && !progress.complete) {
		return;
	}
	latestProgress = progress;
	if (progress.complete) {
		flushLatest();
		return;
	}
	if (!flushTimer) {
		flushTimer = setTimeout(flushLatest, FLUSH_INTERVAL_MS);
	}
}

function closeSocket() {
	clearFlushTimer();
	latestProgress = null;
	if (socket) {
		socket.onclose = null;
		socket.onerror = null;
		socket.onmessage = null;
		socket.close();
		socket = null;
	}
}

function connect(url: string) {
	closeSocket();
	socket = new WebSocket(url);
	socket.onmessage = (event) => {
		try {
			scheduleFlush(JSON.parse(event.data) as DashboardProgressEvent);
		} catch {
			// Ignore non-JSON control frames.
		}
	};
	socket.onerror = () => post({ type: 'error', message: 'Live dashboard progress is unavailable; the dashboard request is still running.' });
	socket.onclose = () => post({ type: 'closed' });
}

self.onmessage = (event: MessageEvent<WorkerCommand>) => {
	if (event.data.type === 'connect') {
		connect(event.data.url);
		return;
	}
	closeSocket();
};
