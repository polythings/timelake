/* tslint:disable */
/* eslint-disable */

/**
 * One Parquet file, read through the byte ranges the host fetches.
 */
export class ParquetFile {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Adds fetched bytes starting at offset `start`.
     */
    addRange(start: number, bytes: Uint8Array): void;
    /**
     * Bytes needed from the end of the file for the footer, given at least its last 8 bytes.
     */
    static footerLength(tail: Uint8Array): number;
    /**
     * `tail` must contain the whole footer (the last `footerLength` bytes or more).
     */
    constructor(size: number, tail: Uint8Array);
    /**
     * Selects row groups and columns for `query`; returns the byte ranges to fetch,
     * `[[start, end), …]`.
     */
    plan(query: any): any;
}

/**
 * Collects a query's result across files: rows, time-bucket aggregates when `query.bucket` is
 * set, or the newest row per series when `query.latestBy` is.
 */
export class Query {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * The result as a JSON array string.
     */
    finish(): string;
    constructor(query: any);
    /**
     * Decodes the file's planned ranges and adds its matching rows to the result.
     */
    push(file: ParquetFile): void;
}

/**
 * A table at one version: its metadata plus the data files of the current snapshot.
 */
export class Table {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Parses the manifest list; returns the paths of the data manifests to fetch and add. Throws
     * for a table with delete files, which this reader doesn't apply.
     */
    addManifestList(bytes: Uint8Array): string[];
    /**
     * Adds the live data files of one data manifest. Add them in the order `addManifestList`
     * returned them: entries take their sequence numbers from the manifest's.
     */
    addManifest(bytes: Uint8Array): void;
    /**
     * All live data files: `[{path, size, records, partition}]`.
     */
    files(): any;
    /**
     * A table from its head object (`metadata/timelake-head`). Throws for a head this reader
     * can't decode (another codec or layout); read the table through its metadata instead.
     */
    static fromHead(bytes: Uint8Array): Table;
    /**
     * Contract, schema and snapshot summary (a plain object).
     */
    info(): any;
    constructor(metadata_json: string);
    /**
     * Opens a data file from the footer the head carried; undefined if it didn't carry one.
     */
    openFile(path: string, size: number): ParquetFile | undefined;
    /**
     * Data files that may contain rows matching `query`, in the same shape as `files()`. Throws
     * for a query naming a column the table doesn't have.
     */
    planFiles(query: any): any;
    /**
     * Runs `query` on the last-value index the head carried; returns the result as a JSON array
     * string, or undefined when the index wasn't in the head.
     */
    queryLastValues(query: any): string | undefined;
    /**
     * The current snapshot's last-value index (newest row per series), if it has one.
     */
    readonly lastValueIndex: string | undefined;
    /**
     * The current snapshot's manifest list, if the table has a snapshot.
     */
    readonly manifestList: string | undefined;
    /**
     * The table version the head describes (`vN.metadata.json`); undefined for `new Table`.
     */
    readonly version: number | undefined;
}

/**
 * Writes one table: keeps the version it last loaded or committed, so a steady writer doesn't
 * reload the table before each append (a commit that loses to another writer reloads it).
 */
export class Writer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Appends rows given as an Arrow IPC stream.
     */
    appendIpc(ipc: Uint8Array, options: any): Promise<any>;
    /**
     * Appends rows given as newline-delimited JSON objects. Timestamps are epoch milliseconds or
     * ISO strings (millisecond precision; use `appendIpc` for microseconds).
     */
    appendJson(rows_json: string, options: any): Promise<any>;
    /**
     * Runs maintenance: retention, compaction (within `maxBytes` read, if given), and snapshot
     * expiry, as the table's settings say.
     */
    maintain(options: any): Promise<any>;
    /**
     * A writer of the table at `location` (its root URI) through `store`.
     */
    constructor(store: any, location: string);
    /**
     * The table's columns (name, type, required), loading the table if needed.
     */
    schema(): Promise<any>;
}

/**
 * The most heap bytes in use at once so far.
 */
export function heapPeakBytes(): number;

/**
 * Bytes of WebAssembly memory the module has grown to. It never shrinks, so this is the peak of
 * everything the reader and writer did so far (what counts against a Worker's memory limit, with
 * the JavaScript heap).
 */
export function memoryBytes(): number;

/**
 * Version of this crate.
 */
export function version(): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_parquetfile_free: (a: number, b: number) => void;
    readonly __wbg_query_free: (a: number, b: number) => void;
    readonly __wbg_table_free: (a: number, b: number) => void;
    readonly __wbg_writer_free: (a: number, b: number) => void;
    readonly heapPeakBytes: () => number;
    readonly memoryBytes: () => number;
    readonly parquetfile_addRange: (a: number, b: number, c: number, d: number) => [number, number];
    readonly parquetfile_footerLength: (a: number, b: number) => [number, number, number];
    readonly parquetfile_new: (a: number, b: number, c: number) => [number, number, number];
    readonly parquetfile_plan: (a: number, b: any) => [number, number, number];
    readonly query_finish: (a: number) => [number, number, number, number];
    readonly query_new: (a: any) => [number, number, number];
    readonly query_push: (a: number, b: number) => [number, number];
    readonly table_addManifest: (a: number, b: number, c: number) => [number, number];
    readonly table_addManifestList: (a: number, b: number, c: number) => [number, number, number, number];
    readonly table_files: (a: number) => [number, number, number];
    readonly table_fromHead: (a: number, b: number) => [number, number, number];
    readonly table_info: (a: number) => [number, number, number];
    readonly table_lastValueIndex: (a: number) => [number, number];
    readonly table_manifestList: (a: number) => [number, number];
    readonly table_new: (a: number, b: number) => [number, number, number];
    readonly table_openFile: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly table_planFiles: (a: number, b: any) => [number, number, number];
    readonly table_queryLastValues: (a: number, b: any) => [number, number, number, number];
    readonly table_version: (a: number) => [number, number];
    readonly version: () => [number, number];
    readonly writer_appendIpc: (a: number, b: number, c: number, d: any) => any;
    readonly writer_appendJson: (a: number, b: number, c: number, d: any) => any;
    readonly writer_maintain: (a: number, b: any) => any;
    readonly writer_new: (a: any, b: number, c: number) => number;
    readonly writer_schema: (a: number) => any;
    readonly rust_zstd_wasm_shim_calloc: (a: number, b: number) => number;
    readonly rust_zstd_wasm_shim_free: (a: number) => void;
    readonly rust_zstd_wasm_shim_malloc: (a: number) => number;
    readonly rust_zstd_wasm_shim_memcmp: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memcpy: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memmove: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_memset: (a: number, b: number, c: number) => number;
    readonly rust_zstd_wasm_shim_qsort: (a: number, b: number, c: number, d: number) => void;
    readonly wasm_bindgen_bedb11be96d84d49___convert__closures_____invoke___js_sys_b3794bb173fea9cc___Function_fn_wasm_bindgen_bedb11be96d84d49___JsValue_____wasm_bindgen_bedb11be96d84d49___sys__Undefined___js_sys_b3794bb173fea9cc___Function_fn_wasm_bindgen_bedb11be96d84d49___JsValue_____wasm_bindgen_bedb11be96d84d49___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_bedb11be96d84d49___convert__closures_____invoke___wasm_bindgen_bedb11be96d84d49___JsValue__core_608f92abc48d28da___result__Result_____wasm_bindgen_bedb11be96d84d49___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __externref_drop_slice: (a: number, b: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
