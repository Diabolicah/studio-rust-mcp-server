local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)

export type GlobalVariables = {
	studioMode: Types.StartStopPlayMode,
	transportState: string,
	destructiveApprovalsRemaining: number,
	playSessionInFlight: boolean,
}

local GlobalVariables = {
	studioMode = "stop",
	transportState = "connecting",
	destructiveApprovalsRemaining = 0,
	playSessionInFlight = false,
}

return GlobalVariables
