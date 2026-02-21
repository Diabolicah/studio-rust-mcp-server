local Main = script:FindFirstAncestor("MCPStudioPlugin")
local ConsoleOutput = require(Main.Utils.ConsoleOutput)
local DataModelType = require(Main.Utils.DataModelType)
local GameStopUtil = require(Main.Utils.GameStopUtil)
local MockWebSocketService = require(Main.MockWebSocketService)
local PluginUtils = require(Main.Utils.PluginUtils)
local Types = require(Main.Types)
local GlobalVariables = require(Main.Utils.GlobalVariables)

local ChangeHistoryService = game:GetService("ChangeHistoryService")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")
local StudioService = game:GetService("StudioService")

local URI = "http://localhost:44755"
local RECEIVE_ENDPOINT = "/request"
local SEND_ENDPOINT = "/response"

PluginUtils.plugin = plugin

local datamodelType = DataModelType.getDataModelType()

if datamodelType == "Server" then
	task.spawn(GameStopUtil.monitorForStopPlay)
end

if RunService:IsRunning() then
	return
end

local connMessageOut = ConsoleOutput.startListener()
plugin.Unloading:Connect(function()
	connMessageOut:Disconnect()
end)

local old_warn = warn
local function log(...)
	if false then
		old_warn(...)
	end
end

local function fetchBuiltinTools(): { [string]: Types.ToolFunction }
	-- Keep an explicit allowlist so stale ModuleScripts do not become active accidentally.
	return {
		RunCode = require(Main.Tools.RunCode) :: Types.ToolFunction,
		InsertModel = require(Main.Tools.InsertModel) :: Types.ToolFunction,
		GetConsoleOutput = require(Main.Tools.GetConsoleOuput) :: Types.ToolFunction,
		StartStopPlay = require(Main.Tools.StartStopPlay) :: Types.ToolFunction,
		RunScriptInPlayMode = require(Main.Tools.RunScriptInPlayMode) :: Types.ToolFunction,
		GetStudioMode = require(Main.Tools.GetStudioMode) :: Types.ToolFunction,
		QueryInstances = require(Main.Tools.QueryInstances) :: Types.ToolFunction,
		GetPluginInfo = require(Main.Tools.GetPluginInfo) :: Types.ToolFunction,
		ConsumeDestructiveApproval = require(Main.Tools.ConsumeDestructiveApproval) :: Types.ToolFunction,
	}
end

local toolsByName: { [string]: Types.ToolFunction } = fetchBuiltinTools()
local mutatingTools: { [string]: boolean } = {
	RunCode = true,
	InsertModel = true,
	StartStopPlay = true,
	RunScriptInPlayMode = true,
}

local function extractToolName(args: any): (string?, string?)
	if type(args) ~= "table" then
		return nil, "Invalid message args"
	end
	local toolName = next(args)
	if type(toolName) ~= "string" then
		return nil, "Missing tool payload"
	end
	if next(args, toolName) ~= nil then
		return nil, "Expected exactly one tool payload"
	end
	return toolName, nil
end

local function setTransportState(state: string, _reason: string?)
	if GlobalVariables.transportState == state then
		return
	end
	GlobalVariables.transportState = state
end

local function connectWebSocket()
	local client = MockWebSocketService:CreateClient(URI)
	client:SetReceiveEndpoint(RECEIVE_ENDPOINT)
	client:SetSendEndpoint(SEND_ENDPOINT)
	setTransportState("connecting")

	client.Opened:Once(function()
		setTransportState("open")
		log("[MCP] Connection opened")
	end)

	client.Closed:Once(function()
		setTransportState("closed")
		log("[MCP] Connection closed")
	end)

	client.TransportStateChanged:Connect(function(state, reason)
		setTransportState(state, reason)
	end)

	client.MessageReceived:Connect(function(message)
		task.spawn(function()
			log("[MCP] Message received")

			local decodedOk, bodyOrError = pcall(function()
				return HttpService:JSONDecode(message)
			end)
			if not decodedOk or type(bodyOrError) ~= "table" then
				old_warn("[MCP] Invalid JSON request payload")
				return
			end

			local body = bodyOrError
			if type(body.id) ~= "string" then
				old_warn("[MCP] Request payload missing id")
				return
			end

			local id: string = body.id
			local responseSent = false
			local function sendResponseOnce(response: string)
				if not responseSent then
					log("[MCP] Sending response:" .. response)
					responseSent = true
					client:Send({
						id = id,
						response = response,
					})
				end
			end

			local args: Types.ToolArgs = body.args
			local toolName, extractError = extractToolName(args)
			if not toolName then
				sendResponseOnce("Error handling request: " .. tostring(extractError))
				return
			end

			local tool = toolsByName[toolName]
			if not tool then
				sendResponseOnce("No tool found to handle request: " .. toolName)
				return
			end

			local recording = nil
			if mutatingTools[toolName] then
				recording = ChangeHistoryService:TryBeginRecording("StudioMCP:" .. toolName)
			end
			local success, response = pcall(tool, args)

			if recording then
				ChangeHistoryService:FinishRecording(recording, Enum.FinishRecordingOperation.Commit)
			end

			if success and response then
				sendResponseOnce(response)
			elseif success then
				sendResponseOnce("Error handling request: tool returned no response")
			else
				sendResponseOnce("Error handling request: " .. tostring(response))
			end
			log("[MCP] Successfully handled request")
		end)
	end)

	return client
end

local function getButtonImage()
	local ok, response = pcall(function()
		return StudioService:GetClassIcon("PackageLink").Image
	end)
	return ok and response or "rbxasset://textures/ui/GuiImagePlaceholder.png"
end

local function approveOneDestructiveOperation()
	GlobalVariables.destructiveApprovalsRemaining += 1
	print(
		string.format(
			"MCP destructive approval granted. Remaining approvals: %d",
			GlobalVariables.destructiveApprovalsRemaining
		)
	)
end

local currentClient: MockWebSocketService.MockWebSocketClient? = connectWebSocket() -- nil for default off
print("The MCP Studio plugin is ready for prompts.")

local toolbar = plugin:CreateToolbar("MCP")
local toggleButton = toolbar:CreateButton("Toggle MCP", "Toggle connection to the server", getButtonImage())
local approveButton = toolbar:CreateButton(
	"Approve 1 Destructive",
	"Allow exactly one destructive MCP operation",
	getButtonImage()
)
toggleButton.ClickableWhenViewportHidden = true
approveButton.ClickableWhenViewportHidden = true
toggleButton:SetActive(currentClient ~= nil)

toggleButton.Click:Connect(function()
	if not currentClient then
		currentClient = connectWebSocket()
		print("The MCP Studio plugin is ready for prompts.")
	else
		currentClient:Close()
		currentClient = nil
		print("The MCP Studio plugin is stopped.")
	end
end)

approveButton.Click:Connect(function()
	approveOneDestructiveOperation()
end)
