<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import Pnl from '$lib/components/ChartPanel.svelte';
	import { graphqlRequest } from '$lib/graphql';
	import type {
		ChartMode as Mode,
		DashboardChart as Chart,
		DashboardProgressEvent as Prog,
		DashboardView as Dash,
		DatasetView as Ds,
		HdfsFileDescriptor as FileD,
		ImportLocalDirectoryInput as ImpIn,
		RegisterDatasetInput as RegIn
	} from '$lib/types';

	type MsgTyp = 'info' | 'success' | 'error';
	type MsgSt = { text: string; type: MsgTyp };
	type UpTyp = 'EMAIL_ARCHIVE' | 'CSV_TEXT' | 'GENERIC_FILES';

	const q_ds = `
		query Datasets {
			datasets {
				id
				name
				description
				datasetType
				hdfsPath
				registeredAt
				hdfsPathAlreadyExisted
				pendingLocalImport
				pendingLocalImports {
					localPath
					targetSubdirectory
				}
			}
		}
	`;

	const q_dsh = `
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

	const m_reg = `
		mutation RegisterDataset($input: RegisterDatasetInput!) {
			registerDataset(input: $input) {
				id
				name
				description
				datasetType
				hdfsPath
				registeredAt
				hdfsPathAlreadyExisted
				pendingLocalImport
				pendingLocalImports {
					localPath
					targetSubdirectory
				}
			}
		}
	`;

	const all_md: Mode[] = ['BAR', 'LINE', 'TABLE'];
	const val_lim = 40;
	const k_ds = 'datasetviz:selectedDatasetId';
	const k_rows = 'datasetviz:updateEveryRows';
	const k_full = 'datasetviz:fullDashboardUpdateEveryRows';
	const ms_tick = 1000;

	let ds = $state<Ds[]>([]);
	let files = $state<FileD[]>([]);
	let dash = $state<Dash | null>(null);
	let mode_by = $state<Record<string, Mode>>({});
	let pick_by = $state<Record<string, string[]>>({});
	let ds_id = $state('');
	let max_fls = $state(5000);
	let row_int = $state(25000);
	let full_rw = $state(500);
	let refr = $state(false);

	let reging = $state(false);
	let imping = $state(false);
	let upling = $state(false);
	let deling = $state(false);
	let ds_deling = $state(false);
	let dash_ld = $state(false);
	let ds_ld = $state(false);

	let msg = $state<MsgSt>({ text: '', type: 'info' });
	let prog = $state<Prog | null>(null);
	let p_msg = $state('');
	let p_scan = $state(0);
	let p_tot = $state(0);
	let p_rows = $state(0);
	let p_fail = $state(0);
	let p_files = $state<Prog['files']>([]);

	let sock: WebSocket | null = null;
	let last_up = 0;
	let live = $state(false);
	let act_id = '';
	let seq = 0;

	let form = $state<RegIn>({
		name: '',
		description: '',
		hdfsPath: ''
	});

	let imp = $state<ImpIn>({
		datasetId: '',
		datasetType: 'CSV_TEXT',
		localDirectory: ''
	});

	let up_sub = $state('');
	let up_type = $state<UpTyp>('CSV_TEXT');
	let up_fls = $state<File[]>([]);
	let del_p = $state('');

	let sel_ds = $derived(ds.find((d) => d.id === ds_id) ?? null);
	let can_ld = $derived(sel_ds?.datasetType === 'CSV_TEXT' || sel_ds?.datasetType === 'EMAIL_ARCHIVE');
	let data_ok = $derived(files.length > 0 || (sel_ds?.pendingLocalImports?.length ?? 0) > 0);
	let pend = $derived(sel_ds?.pendingLocalImports ?? []);

	function msg_set(text: string, type: MsgTyp = 'info') {
		msg = { text, type };
	}

	function err_msg(err: unknown) {
		return err instanceof Error ? err.message : 'Unexpected error';
	}

	function norm_hp(path: string) {
		const out = path.trim().replaceAll('\\', '/');
		const idx = out.indexOf('://');
		if (idx < 0) return out;

		const from = out.indexOf('/', idx + 3);
		return from >= 0 ? out.slice(from) : '/';
	}

	function int_get(key: string, base: number): number {
		const raw = localStorage.getItem(key);
		const num = raw == null ? Number.NaN : Number.parseInt(raw, 10);
		return Number.isFinite(num) && num > 0 ? num : base;
	}

	function prog_set(p: Prog) {
		p_msg = p.message;
		p_scan = p.scannedFiles;
		p_tot = p.totalFiles;
		p_rows = p.processedRows;
		p_fail = p.failedFiles;
		p_files = p.files;
	}

	function prog_clr() {
		p_msg = '';
		p_scan = 0;
		p_tot = 0;
		p_rows = 0;
		p_fail = 0;
		p_files = [];
		live = false;
	}

	function sum_int(label: string): number {
		const raw = dash?.summaryItems.find((it) => it.label === label)?.value ?? '';
		const num = Number.parseInt(raw.replaceAll(',', ''), 10);
		return Number.isFinite(num) ? num : 0;
	}

	function sum_vis() {
		if (!dash) return [];
		if (!prog) return dash.summaryItems;

		return dash.summaryItems.map((it) => {
			if (it.label === 'Scanned files') return { ...it, value: String(p_scan) };
			if (it.label === 'Processed rows') return { ...it, value: String(p_rows) };
			if (it.label === 'Failed files') return { ...it, value: String(p_fail) };
			return it;
		});
	}

	function chart_up(charts: Chart[], force = false) {
		if (!dash || charts.length === 0) return;

		const now = performance.now();
		if (!force && now - last_up < ms_tick) return;

		const by_id = new Map(charts.map((c) => [c.id, c]));
		dash = {
			...dash,
			charts: dash.charts.map((c) => {
				const next = by_id.get(c.id);
				return next ? { ...next, type: c.type } : c;
			})
		};
		last_up = now;
	}

	function ws_cls() {
		if (!sock) return;

		sock.onclose = null;
		sock.onerror = null;
		sock.onmessage = null;
		sock.close();
		sock = null;
	}

	function prog_on(p: Prog) {
		if (p.datasetId !== ds_id || p.stage === 'connected') return;

		prog_set(p);
		prog = { ...p, charts: [], dashboard: null };
		if (p.dashboard || p.charts?.length) live = true;

		if (p.dashboard) {
			dash = p.dashboard;
			last_up = performance.now();
		} else if (!dash && p.charts?.length && sel_ds) {
			dash = {
				datasetId: sel_ds.id,
				datasetName: sel_ds.name,
				datasetType: sel_ds.datasetType,
				hdfsPath: sel_ds.hdfsPath,
				generatedAt: new Date().toISOString(),
				maxFiles: max_fls,
				summaryItems: [],
				charts: p.charts,
				columnProfiles: [],
				listPanel: null,
				tablePanel: null
			};
			last_up = performance.now();
		} else if (p.charts?.length) {
			chart_up(p.charts, p.complete);
		}

		if (p.charts?.length) {
			requestAnimationFrame(() => {
				window.dispatchEvent(new CustomEvent<Chart[]>('dashboard-charts:update', { detail: p.charts }));
			});
		}

		msg_set(p.message, p.complete ? 'success' : 'info');
	}

	function ws_open(id: string) {
		ws_cls();

		const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
		const url = `${proto}//${window.location.host}/ws/dashboard-progress?datasetId=${encodeURIComponent(id)}`;
		const ws = new WebSocket(url);

		sock = ws;
		ws.onmessage = (ev) => {
			try {
				prog_on(JSON.parse(ev.data) as Prog);
			} catch {
				// Ignore non-JSON control frames.
			}
		};
		ws.onerror = () => msg_set('Live dashboard progress is unavailable; the dashboard request is still running.', 'info');
	}

	async function api_req<T>(url: string, init: RequestInit = {}): Promise<T> {
		const res = await fetch(url, init);
		if (res.status === 204) return undefined as T;

		const kind = res.headers.get('content-type') ?? '';
		const body = kind.includes('application/json') ? await res.json() : await res.text();

		if (!res.ok) {
			throw new Error(typeof body === 'string' ? body : body.detail ?? res.statusText);
		}
		return body as T;
	}

	function path_msg(d: Ds, verb: string) {
		return d.hdfsPathAlreadyExisted
			? `${verb} ${d.name}. Warning: HDFS path ${d.hdfsPath} already existed, so existing files may be reused or overwritten by imports.`
			: `${verb} ${d.name}. Created HDFS path ${d.hdfsPath}.`;
	}

	function vals(c: Chart): string[] {
		if (c.series.length > 1) return c.series.map((s) => s.name);

		if (c.series.length === 1) {
			return c.series[0].points.length <= val_lim ? c.series[0].points.map((p) => p.label) : [];
		}

		return [];
	}

	function picked(c: Chart): string[] {
		return pick_by[c.id] ?? vals(c);
	}

	function modes(c: Chart): Mode[] {
		return 'availableModes' in c && Array.isArray(c.availableModes) && c.availableModes.length ? c.availableModes as Mode[] : all_md;
	}

	function set_mode(id: string, mode: string) {
		mode_by = { ...mode_by, [id]: mode as Mode };
	}

	function on_mode(id: string, ev: Event): void {
		const el = ev.currentTarget;
		if (el instanceof HTMLSelectElement) set_mode(id, el.value);
	}

	function flip_val(c: Chart, val: string) {
		const all = vals(c);
		if (all.length === 0) return;

		const cur = picked(c);
		const next = cur.includes(val) ? cur.filter((x) => x !== val) : [...cur, val];

		pick_by = {
			...pick_by,
			[c.id]: next.length === 0 ? all : next
		};
	}

	function chart_do(c: Chart): Chart {
		const all = modes(c);
		const req = mode_by[c.id] ?? c.type;
		const type = all.includes(req) ? req : c.type;

		if (c.series.length > 1) {
			const keep = new Set(picked(c));
			return {
				...c,
				type,
				series: c.series.filter((s) => keep.has(s.name))
			};
		}

		if (c.series.length === 1) {
			const keep = pick_by[c.id];
			if (!keep || c.series[0].points.length > val_lim) return { ...c, type };

			const set = new Set(keep);
			return {
				...c,
				type,
				series: [
					{
						...c.series[0],
						points: c.series[0].points.filter((p) => set.has(p.label))
					}
				]
			};
		}

		return { ...c, type };
	}

	function sts(val: string) {
		if (val === 'processing') return 'run';
		if (val === 'complete') return 'done';
		if (val === 'failed') return 'fail';
		return val;
	}

	async function load_ds() {
		ds_ld = true;
		try {
			const data = await graphqlRequest<{ datasets: Ds[] }>(q_ds);
			ds = data.datasets;

			if (ds.length > 0 && !ds.some((d) => d.id === ds_id)) ds_id = ds[0].id;

			if (ds.length === 0) {
				ds_id = '';
				dash = null;
				files = [];
			}

			if (ds_id) await load_fls();
		} finally {
			ds_ld = false;
		}
	}

	async function load_fls() {
		if (!ds_id) {
			files = [];
			return;
		}

		try {
			files = await api_req<FileD[]>(`/api/datasets/${ds_id}/files?limit=200&recursive=true`);
		} catch (err) {
			files = [];
			msg_set(err_msg(err), 'error');
		}
	}

	async function sel_chg() {
		seq++;
		localStorage.setItem(k_ds, ds_id);
		dash = null;
		prog = null;
		dash_ld = false;
		del_p = '';

		prog_clr();
		ws_cls();
		await load_fls();

		if (ds_id) ws_open(ds_id);
	}

	async function load_dash() {
		if (!ds_id) {
			msg_set('Register or select a dataset before loading analytics.', 'error');
			return;
		}
		if (!can_ld) {
			msg_set('Import or upload files as CSV_TEXT or EMAIL_ARCHIVE before loading analytics.', 'error');
			return;
		}
		if (!data_ok) {
			msg_set('This dataset has no files in HDFS. Import or upload files before loading analytics.', 'error');
			return;
		}

		dash_ld = true;

		const my_seq = ++seq;
		const had_p = pend.length > 0;
		const req_id = ds_id;

		act_id = ds_id;
		prog = null;
		prog_clr();
		localStorage.setItem(k_rows, String(row_int));
		localStorage.setItem(k_full, String(full_rw));
		ws_open(ds_id);
		msg_set('Loading dashboard...', 'info');

		try {
			const data = await graphqlRequest<{ dashboard: Dash }>(q_dsh, {
				datasetId: ds_id,
				maxFiles: max_fls,
				updateEveryRows: row_int,
				fullDashboardUpdateEveryRows: full_rw,
				refresh: refr
			});

			if (my_seq !== seq || ds_id !== req_id) return;

			dash = data.dashboard;
			p_msg = 'Dashboard analytics ready.';
			p_scan = sum_int('Scanned files');
			p_rows = sum_int('Processed rows');
			p_fail = sum_int('Failed files');

			const currentProg = prog;
			if (currentProg) {
				prog = Object.assign({}, currentProg, { message: p_msg, complete: true });
			}

			mode_by = {};
			pick_by = {};

			if (had_p) await load_ds();

			msg_set(`Loaded ${data.dashboard.datasetName}.`, 'success');
		} catch (err) {
			if (my_seq !== seq) return;
			msg_set(err_msg(err), 'error');
		} finally {
			if (my_seq === seq) {
				dash_ld = false;
				act_id = '';
				ws_cls();
			}
		}
	}

	async function reg_ds() {
		reging = true;
		msg_set('Registering dataset...', 'info');

		try {
			const data = await graphqlRequest<{ registerDataset: Ds }>(m_reg, { input: form });

			await load_ds();
			ds_id = data.registerDataset.id;
			localStorage.setItem(k_ds, ds_id);

			form = { name: '', description: '', hdfsPath: '' };
			dash = null;
			await load_fls();

			msg_set(path_msg(data.registerDataset, 'Registered dataset'), 'success');
		} catch (err) {
			msg_set(err_msg(err), 'error');
		} finally {
			reging = false;
		}
	}

	async function imp_loc() {
		if (!ds_id) {
			msg_set('Create or select a dataset before importing files.', 'error');
			return;
		}

		imping = true;
		msg_set('Importing server directory into the selected dataset...', 'info');

		try {
			files = await api_req<FileD[]>('/api/datasets/import-local', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ ...imp, datasetId: ds_id })
			});

			imp = {
				datasetId: ds_id,
				datasetType: imp.datasetType,
				localDirectory: ''
			};

			dash = null;
			await load_ds();

			msg_set(
				imp.datasetType === 'EMAIL_ARCHIVE'
					? `Queued email archive import for ${sel_ds?.name ?? 'dataset'}. Files will be copied when the dashboard loads.`
					: `Imported directory into ${sel_ds?.name ?? 'dataset'}.`,
				'success'
			);
		} catch (err) {
			msg_set(err_msg(err), 'error');
		} finally {
			imping = false;
		}
	}

	function file_chg(ev: Event) {
		const el = ev.currentTarget;
		if (el instanceof HTMLInputElement) up_fls = Array.from(el.files ?? []);
	}

	async function imp_rem() {
		if (!ds_id) {
			msg_set('Create or select a dataset before uploading files.', 'error');
			return;
		}
		if (up_fls.length === 0) {
			msg_set('Choose at least one file to upload.', 'error');
			return;
		}

		upling = true;
		msg_set('Uploading files into the selected dataset...', 'info');

		try {
			const fd = new FormData();
			up_fls.forEach((f) => fd.append('files', f));

			if (up_sub.trim()) fd.append('targetSubdirectory', up_sub.trim());
			fd.append('datasetType', up_type);

			files = await api_req<FileD[]>(`/api/datasets/${ds_id}/import-remote`, {
				method: 'POST',
				body: fd
			});

			dash = null;
			await load_ds();
			msg_set(`Uploaded ${up_fls.length} file(s) into ${sel_ds?.name ?? 'dataset'}.`, 'success');
		} catch (err) {
			msg_set(err_msg(err), 'error');
		} finally {
			upling = false;
		}
	}

	async function del_file(path = del_p) {
		if (!ds_id || !path) {
			msg_set('Select a dataset file to delete.', 'error');
			return;
		}

		deling = true;
		msg_set('Deleting dataset file...', 'info');

		try {
			const clean = norm_hp(path);

			await api_req<void>(`/api/datasets/${ds_id}/files?path=${encodeURIComponent(clean)}`, {
				method: 'DELETE'
			});

			del_p = '';
			dash = null;
			await load_fls();

			msg_set('Deleted file from dataset.', 'success');
		} catch (err) {
			msg_set(err_msg(err), 'error');
		} finally {
			deling = false;
		}
	}

	async function del_ds() {
		const cur = sel_ds;
		if (!cur) {
			msg_set('Select a dataset to delete.', 'error');
			return;
		}
		if (!confirm(`Delete dataset "${cur.name}" and all files under ${cur.hdfsPath}?`)) return;

		ds_deling = true;
		msg_set('Deleting dataset and its files...', 'info');

		try {
			await api_req<void>(`/api/datasets/${cur.id}`, { method: 'DELETE' });

			seq++;
			if (ds_id === cur.id) {
				ds_id = '';
				localStorage.removeItem(k_ds);
			}
			dash = null;
			files = [];
			prog = null;
			prog_clr();
			ws_cls();

			await load_ds();
			msg_set(`Deleted dataset ${cur.name}.`, 'success');
		} catch (err) {
			msg_set(err_msg(err), 'error');
		} finally {
			ds_deling = false;
		}
	}

	onMount(async () => {
		try {
			ds_id = localStorage.getItem(k_ds) ?? '';
			row_int = int_get(k_rows, row_int);
			full_rw = int_get(k_full, full_rw);

			await load_ds();

			if (ds_id) ws_open(ds_id);

			msg_set('Ready. Register a dataset or load one from the list.', 'success');
		} catch (err) {
			msg_set(err_msg(err), 'error');
		}
	});

	onDestroy(() => {
		ws_cls();
	});
</script>

<svelte:head>
	<title>Dataset Visualization Dashboard</title>
	<meta
		name="description"
		content="Register HDFS datasets and render analytics dashboards from GraphQL-powered APIs."
	/>
</svelte:head>

<div class="page">
	<section class="hero panel">
		<div>
			<h1>Dataset visualizator</h1>
		</div>

		<div class="h-meta">
			<div>
				<span class="m-lbl">Datasets</span>
				<strong>{ds.length}</strong>
			</div>
			<div>
				<span class="m-lbl">Endpoint</span>
				<strong>/graphql</strong>
			</div>
			<div>
				<span class="m-lbl">Live channel</span>
				<strong>/ws/dashboard-progress</strong>
			</div>
		</div>
	</section>

	<section class="panel ctrl">
		<div class="p-head">
			<div>
				<p class="brow">Dataset manager</p>
			</div>
			<button class="pri-btn" type="button" onclick={reg_ds} disabled={reging}>
				{reging ? 'Registering…' : 'Register dataset'}
			</button>
		</div>

		<div class="f-grid">
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

		{#if ds.length > 0}
			<div class="divdr"></div>
			<div class="f-grid cmp-grd">
				<label>
					<span>Existing dataset</span>
					<select bind:value={ds_id} onchange={sel_chg}>
						{#each ds as d}
							<option value={d.id}>{d.name}</option>
						{/each}
					</select>
				</label>
				<label class="wide">
					<span>Dataset HDFS root</span>
					<input value={sel_ds?.hdfsPath ?? ''} readonly />
				</label>
				<div class="b-row wide">
					<button class="del-btn" type="button" onclick={del_ds} disabled={ds_deling || dash_ld || !sel_ds}>
						{ds_deling ? 'Deleting…' : 'Delete selected dataset'}
					</button>
				</div>
			</div>
		{:else}
			<div class="note">Register a new dataset.</div>
		{/if}
	</section>

	{#if sel_ds}
		<section class="panel ctrl">
			<div class="p-head">
				<div>
					<p class="brow">File manager</p>
					<h2>Import, replace, or delete files</h2>
				</div>
				<button class="sec-btn" type="button" onclick={load_fls}>Refresh files</button>
			</div>

			<div class="f-grid">
				<label>
					<span>Processing type</span>
					<select bind:value={imp.datasetType}>
						<option value="CSV_TEXT">CSV_TEXT</option>
						<option value="EMAIL_ARCHIVE">EMAIL_ARCHIVE</option>
						<option value="GENERIC_FILES">GENERIC_FILES</option>
					</select>
				</label>
				<label>
					<span>Server directory</span>
					<input bind:value={imp.localDirectory} placeholder="/mnt/main/trung/Text/Data" />
				</label>
				<div class="b-row wide">
					<button class="pri-btn" type="button" onclick={imp_loc} disabled={imping}>
						{imping ? 'Importing…' : 'Import server directory'}
					</button>
				</div>
				<label>
					<span>Client files</span>
					<input type="file" multiple onchange={file_chg} />
				</label>
				<label>
					<span>Processing type</span>
					<select bind:value={up_type}>
						<option value="CSV_TEXT">CSV_TEXT</option>
						<option value="EMAIL_ARCHIVE">EMAIL_ARCHIVE</option>
						<option value="GENERIC_FILES">GENERIC_FILES</option>
					</select>
				</label>
				<label>
					<span>Upload subdirectory</span>
					<input bind:value={up_sub} placeholder="optional/subdir" />
				</label>
				<div class="b-row wide">
					<button class="pri-btn" type="button" onclick={imp_rem} disabled={upling}>
						{upling ? 'Uploading…' : `Upload ${up_fls.length || ''} file${up_fls.length === 1 ? '' : 's'}`}
					</button>
				</div>
			</div>

			<div class="f-list">
				<div class="f-head">
					<span>Dataset files</span>
					<strong>{files.length}</strong>
				</div>

				{#if files.length === 0 && pend.length > 0}
					<p>{pend.length} email archive folder{pend.length === 1 ? '' : 's'} queued.</p>
					<ul class="pend cmp">
						{#each pend as p}
							<li>
								<strong>{p.localPath}</strong>
								{#if p.targetSubdirectory}
									<span>to {p.targetSubdirectory}</span>
								{/if}
							</li>
						{/each}
					</ul>
				{:else if files.length === 0}
					<p>No files imported yet.</p>
				{:else}
					{#each files as f}
						<div class="f-row">
							<div>
								<strong>{f.name}</strong>
								<span>{f.path}</span>
							</div>
							<button class="del-btn" type="button" onclick={() => del_file(f.path)} disabled={deling}>
								Delete
							</button>
						</div>
					{/each}
				{/if}
			</div>
		</section>

		<section class="panel ctrl">
			<div class="p-head">
				<div>
					<p class="brow">Dashboard manager</p>
					<h2>Process and visualize data</h2>
				</div>
				<button
					class="pri-btn"
					type="button"
					onclick={load_dash}
					disabled={dash_ld || ds_ld || !can_ld || !data_ok}
				>
					{dash_ld ? 'Loading…' : 'Load dashboard'}
				</button>
			</div>

			<div class="f-grid cmp-grd">
				<label>
					<span>Max files</span>
					<input bind:value={max_fls} type="number" min="1" />
				</label>
				<label>
					<span>Update every rows</span>
					<input bind:value={row_int} type="number" min="50" step="50" />
				</label>
				<label>
					<span>Full dashboard update rows</span>
					<input bind:value={full_rw} type="number" min="50" step="50" />
				</label>
				<label class="chk">
					<input bind:checked={refr} type="checkbox" />
					<span>Force refresh</span>
				</label>
			</div>

			{#if dash_ld || prog}
				<div class="p-panel">
					<div class="p-copy">
						<strong>{p_msg || 'Starting dashboard load...'}</strong>
						<span>
							{#if prog?.stage === 'importing'}
								{p_scan} files copied · showing {p_files.length} recent files
							{:else}
								{p_scan}/{p_tot} files · {p_rows} rows · {p_fail} failed
							{/if}
						</span>
					</div>

					{#if p_files.length}
						<div class="fp-grid">
							{#each p_files as f}
								<article class={`fp-card ${sts(f.status)}`}>
									<div>
										<strong>{f.name}</strong>
										<span>{f.status}</span>
									</div>
									<p>{prog?.stage === 'importing' ? f.path : `${f.processedRows} rows processed`}</p>
									<small>{f.message}</small>
								</article>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		</section>
	{/if}

	<div class={`msg ${msg.type}`}>{msg.text}</div>

	{#if dash}
		<section class="s-grid">
			{#each sum_vis() as it}
				<article class="s-card panel">
					<span class="m-lbl">{it.label}</span>
					<strong>{it.value}</strong>
				</article>
			{/each}
		</section>

		{#if dash.columnProfiles.length > 0}
			<section class="panel prv">
				<div class="p-head sm-head">
					<div>
						<p class="brow">Column preview</p>
						<h2>First 10 values by detected type</h2>
					</div>
					<span class="prv-cnt">{dash.columnProfiles.length} columns</span>
				</div>

				<div class="col-grd">
					{#each dash.columnProfiles as col}
						<article class="col-crd">
							<div>
								<strong>{col.name}</strong>
								<span>{col.type}</span>
							</div>

							{#if col.sampleValues.length > 0}
								<ol>
									{#each col.sampleValues as val}
										<li>{val}</li>
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

		<section class="ch-grid">
			{#each dash.charts as c (`${c.id}:${mode_by[c.id] ?? c.type}`)}
				{@const fs = vals(c)}
				{@const ms = modes(c)}
				{@const out = chart_do(c)}

				<article class="panel ch-card">
					<div class="tools">
						<label>
							<span>Visualization</span>
							<select value={mode_by[c.id] ?? c.type} onchange={(ev: Event) => on_mode(c.id, ev)}>
								{#each ms as m}
									<option value={m}>{m}</option>
								{/each}
							</select>
						</label>

						{#if fs.length > 0}
							<div class="focus">
								<span>Focus</span>
								<div class="chips">
									{#each fs as val}
										<button
											type="button"
											class:sel={picked(c).includes(val)}
											onclick={() => flip_val(c, val)}
										>
											{val}
										</button>
									{/each}
								</div>
							</div>
						{/if}
					</div>

					{#key out.type}
						<Pnl chart={out} mode={out.type} />
					{/key}
				</article>
			{/each}
		</section>

		<section class="btm-grd">
			{#if dash.listPanel}
				<article class="panel lst">
					<div class="p-head sm-head">
						<div>
							<p class="brow">List panel</p>
							<h2>{dash.listPanel.title}</h2>
						</div>
					</div>
					<ul>
						{#each dash.listPanel.items as it}
							<li>{it}</li>
						{/each}
					</ul>
				</article>
			{/if}

			{#if dash.tablePanel}
				<article class="panel tbl">
					<div class="p-head sm-head">
						<div>
							<p class="brow">Table panel</p>
							<h2>{dash.tablePanel.title}</h2>
						</div>
					</div>

					<div class="t-wrap">
						<table>
							<thead>
								<tr>
									{#each dash.tablePanel.columns as col}
										<th>{col}</th>
									{/each}
								</tr>
							</thead>
							<tbody>
								{#each dash.tablePanel.rows as row}
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
	{:else if sel_ds}
		<section class="panel empty">
			<h2>No dashboard loaded</h2>
			<p>Import files if needed, then use the dashboard manager to process and visualize this dataset.</p>
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

	.page {
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

	.brow {
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

	.h-meta {
		min-width: 260px;
		display: grid;
		gap: 0.85rem;
		grid-template-columns: repeat(3, minmax(0, 1fr));
	}

	.m-lbl {
		display: block;
		font-size: 0.75rem;
		text-transform: uppercase;
		letter-spacing: 0.08em;
		color: #8b94b4;
		margin-bottom: 0.35rem;
	}

	.h-meta strong,
	.s-card strong {
		font-size: 1rem;
		font-weight: 600;
	}

	.ctrl,
	.empty {
		padding: 1.4rem;
	}

	.p-head {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: center;
		margin-bottom: 1rem;
	}

	.sm-head {
		margin-bottom: 1.1rem;
	}

	.f-grid {
		display: grid;
		gap: 0.9rem;
		grid-template-columns: repeat(2, minmax(0, 1fr));
	}

	.note {
		margin: -0.2rem 0 1rem;
		color: #aab7d8;
		line-height: 1.5;
	}

	.d-note {
		margin: 1rem 0 0;
	}

	.warn {
		padding: 0.85rem 1rem;
		border-radius: 0.9rem;
		background: rgba(255, 196, 87, 0.1);
		border: 1px solid rgba(255, 196, 87, 0.22);
		color: #ffe0a3;
	}

	.pend {
		margin: 0.7rem 0 0;
		padding: 0;
		list-style: none;
		display: grid;
		gap: 0.45rem;
	}

	.pend.cmp {
		margin-top: 0.55rem;
	}

	.pend li {
		display: grid;
		gap: 0.15rem;
		padding: 0.55rem 0.65rem;
		border-radius: 0.75rem;
		background: rgba(255, 255, 255, 0.04);
		border: 1px solid rgba(255, 255, 255, 0.08);
	}

	.pend strong {
		color: #eef2ff;
		word-break: break-all;
	}

	.pend span {
		color: #aab7d8;
	}

	.divdr {
		height: 1px;
		margin: 1.2rem 0;
		background: rgba(255, 255, 255, 0.08);
	}

	.wide {
		grid-column: 1 / -1;
	}

	.b-row {
		display: flex;
		gap: 0.75rem;
		align-items: center;
	}

	.cmp-grd {
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

	.chk {
		display: flex;
		align-items: center;
		gap: 0.7rem;
		padding-bottom: 0.2rem;
	}

	.chk input {
		width: 1rem;
		height: 1rem;
		margin: 0;
	}

	.pri-btn {
		border: 0;
		border-radius: 999px;
		padding: 0.85rem 1.2rem;
		background: linear-gradient(135deg, #6ea8fe, #8d5cf6);
		color: white;
		font-weight: 600;
		cursor: pointer;
	}

	.sec-btn,
	.del-btn {
		border-radius: 999px;
		padding: 0.75rem 1rem;
		font-weight: 600;
		cursor: pointer;
	}

	.sec-btn {
		border: 1px solid rgba(255, 255, 255, 0.12);
		background: rgba(255, 255, 255, 0.04);
		color: #dfe7ff;
	}

	.del-btn {
		border: 1px solid rgba(255, 123, 123, 0.45);
		background: rgba(255, 123, 123, 0.1);
		color: #ffd1d1;
	}

	.pri-btn:disabled,
	.sec-btn:disabled,
	.del-btn:disabled {
		opacity: 0.65;
		cursor: wait;
	}

	.f-list {
		margin-top: 1.25rem;
		display: grid;
		gap: 0.65rem;
	}

	.f-head,
	.f-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
	}

	.f-head {
		color: #aab7d8;
	}

	.f-row {
		padding: 0.8rem 0.9rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
	}

	.f-row div {
		display: grid;
		gap: 0.25rem;
		min-width: 0;
	}

	.f-row span {
		color: #8b94b4;
		font-size: 0.82rem;
		overflow-wrap: anywhere;
	}

	.p-panel {
		margin-top: 1rem;
		padding: 1rem;
		border-radius: 1rem;
		background: rgba(110, 168, 254, 0.08);
		border: 1px solid rgba(110, 168, 254, 0.18);
		display: grid;
		gap: 0.8rem;
	}

	.p-copy {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		color: #cddcff;
		flex-wrap: wrap;
	}

	.p-copy span {
		color: #8fb2ff;
	}

	.stream {
		padding: 0.75rem 0.85rem;
		border-radius: 0.8rem;
		background: rgba(88, 214, 141, 0.1);
		color: #bdf3d0;
	}

	.fp-grid {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
		gap: 0.7rem;
	}

	.fp-card {
		padding: 0.85rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
		border: 1px solid rgba(255, 255, 255, 0.08);
		display: grid;
		gap: 0.45rem;
	}

	.fp-card div {
		display: flex;
		justify-content: space-between;
		gap: 0.75rem;
		align-items: start;
	}

	.fp-card span {
		padding: 0.18rem 0.55rem;
		border-radius: 999px;
		background: rgba(255, 255, 255, 0.08);
		color: #dfe7ff;
		font-size: 0.72rem;
		font-weight: 700;
		text-transform: uppercase;
	}

	.fp-card p,
	.fp-card small {
		margin: 0;
		color: #aab7d8;
	}

	.fp-card.run {
		border-color: rgba(110, 168, 254, 0.5);
		background: rgba(110, 168, 254, 0.1);
	}

	.fp-card.done {
		border-color: rgba(88, 214, 141, 0.38);
	}

	.fp-card.fail {
		border-color: rgba(255, 123, 123, 0.5);
		background: rgba(255, 123, 123, 0.1);
	}

	.msg {
		padding: 0.95rem 1.1rem;
		border-radius: 1rem;
		border: 1px solid rgba(255, 255, 255, 0.08);
	}

	.msg.info {
		background: rgba(110, 168, 254, 0.12);
		color: #cddcff;
	}

	.msg.success {
		background: rgba(88, 214, 141, 0.12);
		color: #bdf3d0;
	}

	.msg.error {
		background: rgba(255, 123, 123, 0.12);
		color: #ffd1d1;
	}

	.s-grid {
		display: grid;
		gap: 0.9rem;
		grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
	}

	.s-card {
		padding: 1rem;
	}

	.prv {
		padding: 1.25rem;
	}

	.prv-cnt {
		color: #8fb2ff;
		font-weight: 700;
	}

	.col-grd {
		display: grid;
		gap: 0.85rem;
		grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
		max-height: 520px;
		overflow: auto;
		padding-right: 0.25rem;
	}

	.col-crd {
		padding: 0.95rem;
		border-radius: 1rem;
		background: rgba(255, 255, 255, 0.04);
		border: 1px solid rgba(255, 255, 255, 0.06);
		display: grid;
		gap: 0.75rem;
	}

	.col-crd div {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
	}

	.col-crd span {
		padding: 0.2rem 0.55rem;
		border-radius: 999px;
		background: rgba(141, 92, 246, 0.16);
		color: #d8ccff;
		font-size: 0.72rem;
		font-weight: 700;
	}

	.col-crd ol {
		margin: 0;
		padding-left: 1.35rem;
		display: grid;
		gap: 0.35rem;
		color: #dfe7ff;
		font-size: 0.86rem;
	}

	.col-crd li,
	.col-crd p {
		overflow-wrap: anywhere;
	}

	.col-crd p {
		margin: 0;
		color: #8b94b4;
	}

	.ch-grid {
		display: grid;
		gap: 1rem;
		grid-template-columns: repeat(2, minmax(0, 1fr));
	}

	.ch-card,
	.lst,
	.tbl {
		padding: 1.25rem;
	}

	.tools {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		align-items: start;
		margin-bottom: 1rem;
		flex-wrap: wrap;
	}

	.tools label {
		min-width: 180px;
	}

	.focus {
		display: grid;
		gap: 0.45rem;
		color: #aab7d8;
	}

	.focus > span {
		font-size: 0.88rem;
	}

	.chips {
		display: flex;
		gap: 0.5rem;
		flex-wrap: wrap;
	}

	.chips button {
		padding: 0.45rem 0.75rem;
		border-radius: 999px;
		border: 1px solid rgba(255, 255, 255, 0.1);
		background: rgba(255, 255, 255, 0.04);
		color: #b9c6e7;
		font-size: 0.84rem;
		font-weight: 500;
	}

	.chips button.sel {
		background: rgba(110, 168, 254, 0.2);
		border-color: rgba(110, 168, 254, 0.55);
		color: #eef2ff;
	}

	.btm-grd {
		display: grid;
		gap: 1rem;
		grid-template-columns: minmax(280px, 0.95fr) minmax(0, 1.35fr);
	}

	.lst ul {
		list-style: none;
		padding: 0;
		margin: 0;
		display: grid;
		gap: 0.7rem;
	}

	.lst li {
		padding: 0.8rem 0.9rem;
		border-radius: 0.9rem;
		background: rgba(255, 255, 255, 0.04);
		color: #d9e2ff;
	}

	.t-wrap {
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

	.empty {
		text-align: center;
		padding-block: 3rem;
	}

	.empty p {
		max-width: 44rem;
		margin: 0.9rem auto 0;
		color: #aab7d8;
		line-height: 1.6;
	}

	@media (max-width: 1100px) {
		.ch-grid,
		.btm-grd,
		.cmp-grd,
		.hero,
		.h-meta {
			grid-template-columns: 1fr;
			display: grid;
		}

		.hero {
			align-items: start;
		}
	}

	@media (max-width: 720px) {
		.page {
			padding-inline: 0.9rem;
		}

		.f-grid,
		.ch-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
