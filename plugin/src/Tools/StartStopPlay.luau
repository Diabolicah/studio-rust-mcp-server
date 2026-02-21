local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)
local StudioTestService = game:GetService("StudioTestService")
local ConsoleOutput = require(Main.Utils.ConsoleOutput)
local GameStopUtil = require(Main.Utils.GameStopUtil)
local GlobalVariables = require(Main.Utils.GlobalVariables)

local function hasActiveSession(): boolean
	return GlobalVariables.studioMode ~= "stop" or GlobalVariables.playSessionInFlight
end

local function startPlay(): string?
	if hasActiveSession() then
		return "Play session already active or starting"
	end

	ConsoleOutput.reset()
	GlobalVariables.studioMode = "start_play"
	GlobalVariables.playSessionInFlight = true
	task.spawn(function()
		local ok, err = pcall(function()
			StudioTestService:ExecutePlayModeAsync({})
		end)
		GlobalVariables.playSessionInFlight = false
		if not ok then
			warn("[MCP] start_play failed: " .. tostring(err))
		end
	end)
	return "Started play"
end

local function stop(): string?
	GameStopUtil.stopPlay()
	GlobalVariables.studioMode = "stop"
	GlobalVariables.playSessionInFlight = false
	return "Stopped"
end

local function runServer(): string?
	if hasActiveSession() then
		return "Play session already active or starting"
	end

	GlobalVariables.studioMode = "run_server"
	GlobalVariables.playSessionInFlight = true
	task.spawn(function()
		local ok, err = pcall(function()
			StudioTestService:ExecuteRunModeAsync({})
		end)
		GlobalVariables.playSessionInFlight = false
		if not ok then
			warn("[MCP] run_server failed: " .. tostring(err))
		end
	end)
	return "Ran server"
end

local function handleStartStopPlay(args: Types.ToolArgs): string?
	if not args["StartStopPlay"] then
		return nil
	end

	local startStopPlayArgs: Types.StartStopPlayArgs = args["StartStopPlay"]
	if type(startStopPlayArgs.mode) ~= "string" then
		error("Missing mode in StartStopPlay")
	end

	if startStopPlayArgs.mode == "start_play" then
		return startPlay()
	elseif startStopPlayArgs.mode == "stop" then
		return stop()
	elseif startStopPlayArgs.mode == "run_server" then
		return runServer()
	end

	return "Invalid mode in StartStopPlay, must be start_play, stop, or run_server"
end

return handleStartStopPlay :: Types.ToolFunction
