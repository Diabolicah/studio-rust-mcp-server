local Main = script:FindFirstAncestor("MCPStudioPlugin")
local DataModelType = require(Main.Utils.DataModelType)
local GlobalVariables = require(Main.Utils.GlobalVariables)
local Types = require(Main.Types)

local HttpService = game:GetService("HttpService")

local PLUGIN_VERSION = "0.1.0"
local PLUGIN_PROTOCOL_VERSION = 1
local MIN_COMPATIBLE_SERVER_VERSION = "0.1.0"

local function handleGetPluginInfo(args: Types.ToolArgs): string?
	if not args["GetPluginInfo"] then
		return nil
	end

	local payload = {
		plugin_version = PLUGIN_VERSION,
		plugin_protocol_version = PLUGIN_PROTOCOL_VERSION,
		min_compatible_server_version = MIN_COMPATIBLE_SERVER_VERSION,
		plugin_ready = true,
		studio_mode = GlobalVariables.studioMode,
		place_id = game.PlaceId,
		transport_state = GlobalVariables.transportState,
		data_model_type = DataModelType.getDataModelType(),
	}
	return HttpService:JSONEncode(payload)
end

return handleGetPluginInfo :: Types.ToolFunction
