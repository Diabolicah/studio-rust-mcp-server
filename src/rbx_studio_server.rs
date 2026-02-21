use crate::error::Result;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{extract::State, Json};
use color_eyre::eyre::{Error, OptionExt};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        CallToolResult, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    schemars, tool, tool_handler, tool_router, ErrorData, ServerHandler,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::Duration;
use uuid::Uuid;

pub const STUDIO_PLUGIN_PORT: u16 = 44755;
const LONG_POLL_DURATION: Duration = Duration::from_secs(15);
const VERSION_CHECK_CACHE_TTL: Duration = Duration::from_secs(10);
const PLUGIN_PROTOCOL_VERSION: u32 = 1;
const MIN_COMPATIBLE_PLUGIN_VERSION: &str = "0.1.0";
const MAX_PENDING_REQUESTS: usize = 512;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ToolArguments {
    args: ToolArgumentValues,
    id: Option<Uuid>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RunCommandResponse {
    response: String,
    id: Uuid,
}

pub struct AppState {
    process_queue: VecDeque<ToolArguments>,
    output_map: HashMap<Uuid, mpsc::UnboundedSender<Result<String>>>,
    waiter: watch::Receiver<()>,
    trigger: watch::Sender<()>,
}
pub type PackedState = Arc<Mutex<AppState>>;

impl AppState {
    pub fn new() -> Self {
        let (trigger, waiter) = watch::channel(());
        Self {
            process_queue: VecDeque::new(),
            output_map: HashMap::new(),
            waiter,
            trigger,
        }
    }
}

impl ToolArguments {
    fn new(args: ToolArgumentValues) -> (Self, Uuid) {
        Self { args, id: None }.with_id()
    }

    fn with_id(self) -> (Self, Uuid) {
        let id = Uuid::new_v4();
        (
            Self {
                args: self.args,
                id: Some(id),
            },
            id,
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
struct PluginInfoPayload {
    plugin_version: String,
    plugin_protocol_version: u32,
    min_compatible_server_version: String,
    plugin_ready: bool,
    studio_mode: String,
    place_id: i64,
}

#[derive(Debug, Clone)]
struct VersionCheckCache {
    checked_at: Instant,
    plugin_info: PluginInfoPayload,
}

#[derive(Clone)]
pub struct RBXStudioServer {
    state: PackedState,
    safe_mode: bool,
    version_cache: Arc<Mutex<Option<VersionCheckCache>>>,
    tool_router: ToolRouter<Self>,
}

#[tool_handler]
impl ServerHandler for RBXStudioServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "Roblox_Studio".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("Roblox Studio MCP Server".to_string()),
                icons: None,
                website_url: None,
            },
            instructions: Some(
                "Use query_instances and run_code for read operations; insert_model and destructive run_code patterns require allow_destructive=true and one 'Approve 1 Destructive' click in Studio when safe mode is enabled."
                    .to_string(),
            ),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct RunCode {
    #[schemars(description = "Code to run")]
    command: String,
    #[schemars(
        description = "Required for destructive commands when safe mode is enabled. Also requires a one-time Studio approval."
    )]
    allow_destructive: Option<bool>,
    #[schemars(description = "Optional timeout in milliseconds for this tool call")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct InsertModel {
    #[schemars(description = "Query to search for the model")]
    query: String,
    #[schemars(
        description = "Required when safe mode is enabled. Also requires a one-time Studio approval."
    )]
    allow_destructive: Option<bool>,
    #[schemars(description = "Optional timeout in milliseconds for this tool call")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct GetConsoleOutput {}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct GetStudioMode {}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct GetPluginInfo {}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct HealthCheck {}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct StartStopPlay {
    #[schemars(
        description = "Mode to start or stop, must be start_play, stop, or run_server. Don't use run_server unless you are sure no client/player is needed."
    )]
    mode: String,
    #[schemars(description = "Optional timeout in milliseconds for this tool call")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct RunScriptInPlayMode {
    #[schemars(description = "Code to run")]
    code: String,
    #[schemars(description = "Timeout in seconds, defaults to 100 seconds")]
    timeout: Option<u32>,
    #[schemars(description = "Mode to run in, must be start_play or run_server")]
    mode: String,
    #[schemars(description = "Optional timeout in milliseconds for this tool call")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct QueryInstances {
    #[schemars(description = "Root path to query from, e.g. game.Workspace")]
    path: Option<String>,
    #[schemars(description = "Roblox QueryDescendants selector string. Defaults to *")]
    query_selector: Option<String>,
    #[schemars(description = "Optional class filter (IsA)")]
    class_name: Option<String>,
    #[schemars(description = "Optional case-insensitive name contains filter")]
    name_contains: Option<String>,
    #[schemars(description = "Optional CollectionService tag filter")]
    tag: Option<String>,
    #[schemars(description = "Optional attribute equality filters")]
    attributes: Option<HashMap<String, String>>,
    #[schemars(description = "Maximum results, defaults to 100")]
    max_results: Option<u32>,
    #[schemars(description = "Pagination cursor returned by previous query_instances call")]
    cursor: Option<String>,
    #[schemars(description = "Optional list of property names to include in each result")]
    select_properties: Option<Vec<String>>,
    #[schemars(description = "Whether to include descendants, defaults to true")]
    include_descendants: Option<bool>,
    #[schemars(description = "Optional timeout in milliseconds for this tool call")]
    timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
struct ConsumeDestructiveApproval {}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema, Clone)]
enum ToolArgumentValues {
    RunCode(RunCode),
    InsertModel(InsertModel),
    GetConsoleOutput(GetConsoleOutput),
    StartStopPlay(StartStopPlay),
    RunScriptInPlayMode(RunScriptInPlayMode),
    GetStudioMode(GetStudioMode),
    QueryInstances(QueryInstances),
    GetPluginInfo(GetPluginInfo),
    ConsumeDestructiveApproval(ConsumeDestructiveApproval),
}

#[tool_router]
impl RBXStudioServer {
    pub fn new(state: PackedState, safe_mode: bool) -> Self {
        Self {
            state,
            safe_mode,
            version_cache: Arc::new(Mutex::new(None)),
            tool_router: Self::tool_router(),
        }
    }

    fn json_success(payload: Value) -> CallToolResult {
        CallToolResult::structured(payload)
    }

    fn json_error(code: &str, message: impl Into<String>, details: Option<Value>) -> CallToolResult {
        let mut payload = json!({
            "code": code,
            "message": message.into(),
        });
        if let Some(details) = details {
            payload["details"] = details;
        }
        CallToolResult::structured_error(payload)
    }

    async fn require_destructive_approval(
        &self,
        allow_destructive: Option<bool>,
        operation: &str,
    ) -> std::result::Result<(), String> {
        if !self.safe_mode {
            return Ok(());
        }

        if !allow_destructive.unwrap_or(false) {
            return Err(format!(
                "safe_mode blocked {operation}. Pass allow_destructive=true and click 'Approve 1 Destructive' in Roblox Studio before retrying."
            ));
        }

        let raw = self
            .dispatch_tool_to_plugin(
                ToolArgumentValues::ConsumeDestructiveApproval(ConsumeDestructiveApproval {}),
                Some(2_000),
                true,
            )
            .await
            .map_err(|e| {
                format!("safe_mode could not verify destructive approval for {operation}: {e}")
            })?;

        let parsed: Value = serde_json::from_str(&raw).map_err(|e| {
            format!(
                "safe_mode got invalid approval payload from plugin for {operation}: {e}. Raw: {raw}"
            )
        })?;
        if parsed["approved"].as_bool().unwrap_or(false) {
            return Ok(());
        }

        let reason = parsed["reason"]
            .as_str()
            .unwrap_or("No pending approval available in Studio.");
        Err(format!("safe_mode blocked {operation}. {reason}"))
    }

    #[tool(
        description = "Runs Luau in Roblox Studio and always returns JSON: { returns: [], logs: [], warnings: [], errors: [] }"
    )]
    async fn run_code(
        &self,
        Parameters(args): Parameters<RunCode>,
    ) -> Result<CallToolResult, ErrorData> {
        if Self::looks_destructive(&args.command) {
            if let Err(message) = self
                .require_destructive_approval(args.allow_destructive, "run_code destructive operation")
                .await
            {
                return Ok(Self::json_error("SAFE_MODE_BLOCKED", message, None));
            }
        }

        self.forward_tool(
            ToolArgumentValues::RunCode(args.clone()),
            args.timeout_ms,
            true,
        )
        .await
    }

    #[tool(
        description = "Inserts a model from the Roblox marketplace into the workspace. In safe mode, requires allow_destructive=true and one toolbar approval click. Returns the inserted model name."
    )]
    async fn insert_model(
        &self,
        Parameters(args): Parameters<InsertModel>,
    ) -> Result<CallToolResult, ErrorData> {
        if let Err(message) = self
            .require_destructive_approval(args.allow_destructive, "insert_model")
            .await
        {
            return Ok(Self::json_error("SAFE_MODE_BLOCKED", message, None));
        }

        self.forward_tool(
            ToolArgumentValues::InsertModel(args.clone()),
            args.timeout_ms,
            true,
        )
        .await
    }

    #[tool(description = "Get the console output from Roblox Studio.")]
    async fn get_console_output(
        &self,
        Parameters(_args): Parameters<GetConsoleOutput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::GetConsoleOutput(GetConsoleOutput {}),
            None,
            true,
        )
        .await
    }

    #[tool(description = "Start or stop play mode or run the server.")]
    async fn start_stop_play(
        &self,
        Parameters(args): Parameters<StartStopPlay>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::StartStopPlay(args.clone()),
            args.timeout_ms,
            true,
        )
        .await
    }

    #[tool(
        description = "Run a script in play mode and auto-stop after script finishes or timeout."
    )]
    async fn run_script_in_play_mode(
        &self,
        Parameters(args): Parameters<RunScriptInPlayMode>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::RunScriptInPlayMode(args.clone()),
            args.timeout_ms,
            true,
        )
        .await
    }

    #[tool(
        description = "Get the current studio mode. The result will be one of start_play, run_server, or stop."
    )]
    async fn get_studio_mode(
        &self,
        Parameters(_args): Parameters<GetStudioMode>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::GetStudioMode(GetStudioMode {}),
            None,
            true,
        )
        .await
    }

    #[tool(
        description = "Read-only DataModel inspector. Supports filters by path/class/name/tag/attributes plus cursor pagination and select_properties projection."
    )]
    async fn query_instances(
        &self,
        Parameters(args): Parameters<QueryInstances>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::QueryInstances(args.clone()),
            args.timeout_ms,
            true,
        )
        .await
    }

    #[tool(description = "Returns plugin handshake metadata as JSON")]
    async fn get_plugin_info(
        &self,
        Parameters(_args): Parameters<GetPluginInfo>,
    ) -> Result<CallToolResult, ErrorData> {
        self.forward_tool(
            ToolArgumentValues::GetPluginInfo(GetPluginInfo {}),
            Some(2_000),
            false,
        )
        .await
    }

    #[tool(
        description = "Returns { connected, plugin_ready, studio_mode, place_id, server_version, plugin_version }."
    )]
    async fn health_check(
        &self,
        Parameters(_args): Parameters<HealthCheck>,
    ) -> Result<CallToolResult, ErrorData> {
        let payload = self.build_health_payload().await;
        Ok(Self::json_success(payload))
    }

    async fn forward_tool(
        &self,
        args: ToolArgumentValues,
        timeout_ms: Option<u64>,
        check_versions: bool,
    ) -> Result<CallToolResult, ErrorData> {
        let result = self
            .dispatch_tool_to_plugin(args, timeout_ms, check_versions)
            .await;

        Ok(match result {
            Ok(text) => Self::json_success(json!({ "result": text })),
            Err(err) => Self::json_error(
                "DISPATCH_FAILED",
                err.to_string(),
                None,
            ),
        })
    }

    async fn dispatch_tool_to_plugin(
        &self,
        args: ToolArgumentValues,
        timeout_ms: Option<u64>,
        check_versions: bool,
    ) -> Result<String, ErrorData> {
        if check_versions {
            self.ensure_version_compatible().await?;
        }
        self.dispatch_tool_to_plugin_unchecked(args, timeout_ms)
            .await
    }

    async fn dispatch_tool_to_plugin_unchecked(
        &self,
        args: ToolArgumentValues,
        timeout_ms: Option<u64>,
    ) -> Result<String, ErrorData> {
        let (command, id) = ToolArguments::new(args);
        tracing::debug!("Running command: {:?}", command);

        let (tx, mut rx) = mpsc::unbounded_channel::<Result<String>>();
        let trigger = {
            let mut state = self.state.lock().await;
            if state.process_queue.len() >= MAX_PENDING_REQUESTS {
                return Err(ErrorData::internal_error(
                    format!(
                        "Too many pending requests ({}). Studio plugin may be disconnected or overloaded.",
                        state.process_queue.len()
                    ),
                    None,
                ));
            }
            state.process_queue.push_back(command);
            state.output_map.insert(id, tx);
            state.trigger.clone()
        };

        trigger
            .send(())
            .map_err(|e| ErrorData::internal_error(format!("Unable to trigger send {e}"), None))?;

        let recv_result = if let Some(timeout_ms) = timeout_ms {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), rx.recv()).await {
                Ok(result) => result,
                Err(_) => {
                    {
                        let mut state = self.state.lock().await;
                        state.output_map.remove(&id);
                        state.process_queue.retain(|pending| pending.id != Some(id));
                    }
                    return Err(ErrorData::internal_error(
                        format!("Tool call timed out after {timeout_ms}ms"),
                        None,
                    ));
                }
            }
        } else {
            rx.recv().await
        };

        let result = match recv_result {
            Some(result) => result,
            None => {
                let mut state = self.state.lock().await;
                state.output_map.remove(&id);
                state.process_queue.retain(|pending| pending.id != Some(id));
                return Err(ErrorData::internal_error("Couldn't receive response", None));
            }
        };

        {
            let mut state = self.state.lock().await;
            state.output_map.remove_entry(&id);
        }

        tracing::debug!("Sending to MCP: {result:?}");
        result.map_err(|err| ErrorData::internal_error(err.to_string(), None))
    }

    async fn fetch_plugin_info(&self, use_cache: bool) -> Result<PluginInfoPayload, ErrorData> {
        if use_cache {
            if let Some(cache) = self.version_cache.lock().await.clone() {
                if cache.checked_at.elapsed() < VERSION_CHECK_CACHE_TTL {
                    return Ok(cache.plugin_info);
                }
            }
        }

        let raw = self
            .dispatch_tool_to_plugin_unchecked(
                ToolArgumentValues::GetPluginInfo(GetPluginInfo {}),
                Some(2_000),
            )
            .await?;

        let plugin_info: PluginInfoPayload = serde_json::from_str(&raw).map_err(|e| {
            ErrorData::internal_error(
                format!("Invalid GetPluginInfo payload from plugin: {e}. Raw: {raw}"),
                None,
            )
        })?;

        {
            let mut cache = self.version_cache.lock().await;
            *cache = Some(VersionCheckCache {
                checked_at: Instant::now(),
                plugin_info: plugin_info.clone(),
            });
        }

        Ok(plugin_info)
    }

    async fn build_health_payload(&self) -> Value {
        let server_version = env!("CARGO_PKG_VERSION").to_string();
        match self.fetch_plugin_info(false).await {
            Ok(info) => json!({
                "connected": true,
                "plugin_ready": info.plugin_ready,
                "studio_mode": info.studio_mode,
                "place_id": info.place_id,
                "server_version": server_version,
                "plugin_version": info.plugin_version,
            }),
            Err(_) => json!({
                "connected": false,
                "plugin_ready": false,
                "studio_mode": "unknown",
                "place_id": 0,
                "server_version": server_version,
                "plugin_version": "unknown",
            }),
        }
    }

    async fn ensure_version_compatible(&self) -> Result<(), ErrorData> {
        let plugin_info = self.fetch_plugin_info(true).await?;
        if let Some(message) = Self::build_version_mismatch(&plugin_info) {
            return Err(ErrorData::internal_error(message, None));
        }
        Ok(())
    }

    fn build_version_mismatch(plugin_info: &PluginInfoPayload) -> Option<String> {
        let server_version = env!("CARGO_PKG_VERSION");
        let plugin_version = plugin_info.plugin_version.as_str();

        if plugin_info.plugin_protocol_version != PLUGIN_PROTOCOL_VERSION {
            return Some(format!(
                "Version mismatch: server protocol={PLUGIN_PROTOCOL_VERSION}, plugin protocol={}. Update both server and plugin.",
                plugin_info.plugin_protocol_version
            ));
        }

        if server_version != plugin_version {
            return Some(format!(
                "Version mismatch: server_version={server_version}, plugin_version={plugin_version}. Update both to matching versions."
            ));
        }

        if !Self::version_gte(plugin_version, MIN_COMPATIBLE_PLUGIN_VERSION) {
            return Some(format!(
                "Version mismatch: plugin_version={plugin_version} is below server minimum compatible version={MIN_COMPATIBLE_PLUGIN_VERSION}."
            ));
        }

        if !Self::version_gte(server_version, &plugin_info.min_compatible_server_version) {
            return Some(format!(
                "Version mismatch: server_version={server_version} is below plugin min_compatible_server_version={}",
                plugin_info.min_compatible_server_version
            ));
        }

        None
    }

    fn version_gte(version: &str, min_version: &str) -> bool {
        match (
            Self::parse_version_triplet(version),
            Self::parse_version_triplet(min_version),
        ) {
            (Some(a), Some(b)) => a >= b,
            _ => false,
        }
    }

    fn parse_version_triplet(version: &str) -> Option<(u64, u64, u64)> {
        let clean = version.split('-').next()?;
        let mut it = clean.split('.').map(|s| s.parse::<u64>().ok());
        Some((it.next()??, it.next()??, it.next()??))
    }

    fn looks_destructive(command: &str) -> bool {
        let lowered = command.to_ascii_lowercase();
        let destructive_markers = [
            ":destroy",
            ":remove(",
            ".parent =",
            ":clearallchildren",
            ":bulkmoveto",
            "instance.new",
            ":pivotto",
        ];

        destructive_markers
            .iter()
            .any(|marker| lowered.contains(marker))
    }
}

pub async fn request_handler(State(state): State<PackedState>) -> Result<impl IntoResponse> {
    let timeout = tokio::time::timeout(LONG_POLL_DURATION, async {
        let mut waiter = { state.lock().await.waiter.clone() };
        loop {
            {
                let mut state = state.lock().await;
                if let Some(task) = state.process_queue.pop_front() {
                    return Ok::<ToolArguments, Error>(task);
                }
            }
            waiter.changed().await?
        }
    })
    .await;

    match timeout {
        Ok(result) => Ok(Json(result?).into_response()),
        _ => Ok((StatusCode::LOCKED, String::new()).into_response()),
    }
}

pub async fn response_handler(
    State(state): State<PackedState>,
    Json(payload): Json<RunCommandResponse>,
) -> Result<impl IntoResponse> {
    tracing::debug!("Received reply from studio {payload:?}");
    let tx = { state.lock().await.output_map.remove(&payload.id) };
    if let Some(tx) = tx {
        tx.send(Ok(payload.response))?;
    } else {
        tracing::debug!("Dropping late response for unknown id {}", payload.id);
    }
    Ok(())
}

pub async fn proxy_handler(
    State(state): State<PackedState>,
    Json(command): Json<ToolArguments>,
) -> Result<impl IntoResponse> {
    let id = command.id.ok_or_eyre("Got proxy command with no id")?;
    tracing::debug!("Received request to proxy {command:?}");

    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let mut state = state.lock().await;
        if state.process_queue.len() >= MAX_PENDING_REQUESTS {
            return Err(Error::msg(format!(
                "Too many pending requests ({}). Plugin may be overloaded.",
                state.process_queue.len()
            ))
            .into());
        }
        state.process_queue.push_back(command);
        state.output_map.insert(id, tx);
    }

    let response = rx.recv().await.ok_or_eyre("Couldn't receive response")??;
    {
        let mut state = state.lock().await;
        state.output_map.remove_entry(&id);
    }

    tracing::debug!("Sending back to dud: {response:?}");
    Ok(Json(RunCommandResponse { response, id }))
}

pub async fn dud_proxy_loop(state: PackedState, exit: Receiver<()>) {
    let client = reqwest::Client::new();

    let mut waiter = { state.lock().await.waiter.clone() };
    while exit.is_empty() {
        let entry = { state.lock().await.process_queue.pop_front() };
        if let Some(entry) = entry {
            let Some(id) = entry.id else {
                tracing::error!("Proxy entry missing request id");
                continue;
            };

            let tx = { state.lock().await.output_map.remove(&id) };
            let Some(tx) = tx else {
                tracing::warn!("No waiting receiver found for proxied id {id}");
                continue;
            };

            let res = client
                .post(format!("http://127.0.0.1:{STUDIO_PLUGIN_PORT}/proxy"))
                .json(&entry)
                .send()
                .await;

            let result = match res {
                Ok(res) => {
                    if !res.status().is_success() {
                        Err(Error::msg(format!(
                            "Proxy returned non-success status {}",
                            res.status()
                        ))
                        .into())
                    } else {
                        res.json::<RunCommandResponse>()
                            .await
                            .map(|r| r.response)
                            .map_err(Into::into)
                    }
                }
                Err(err) => Err(err.into()),
            };

            if tx.send(result).is_err() {
                tracing::warn!("Failed to deliver proxied response for id {id}");
            }
        } else if waiter.changed().await.is_err() {
            break;
        }
    }
}
