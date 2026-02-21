export type InsertModelArgs = {
	query: string,
}

export type RunCodeArgs = {
	command: string,
}

export type TestMode = "start_play" | "run_server"

export type StartStopPlayMode = TestMode | "stop"

export type StartStopPlayArgs = {
	mode: StartStopPlayMode,
}

export type RunScriptInPlayModeArgs = {
	code: string,
	timeout: number?,
	mode: TestMode,
	timeout_ms: number?,
}

export type QueryInstancesArgs = {
	path: string?,
	query_selector: string?,
	class_name: string?,
	name_contains: string?,
	tag: string?,
	max_results: number?,
	cursor: string?,
	select_properties: { string }?,
	include_descendants: boolean?,
	attributes: { [string]: any }?,
}

export type GetPluginInfoArgs = {}

export type ConsumeDestructiveApprovalArgs = {}

export type ToolArgs =
	{}
	| { InsertModel: InsertModelArgs }
	| { RunCode: RunCodeArgs }
	| { StartStopPlay: StartStopPlayArgs }
	| { RunScriptInPlayMode: RunScriptInPlayModeArgs }
	| { QueryInstances: QueryInstancesArgs }
	| { GetConsoleOutput: {} }
	| { GetStudioMode: {} }
	| { GetPluginInfo: GetPluginInfoArgs }
	| { ConsumeDestructiveApproval: ConsumeDestructiveApprovalArgs }

export type ToolFunction = (ToolArgs) -> string?

return {}
