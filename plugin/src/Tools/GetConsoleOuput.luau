local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)

local ConsoleOutput = require(Main.Utils.ConsoleOutput)

local function handleGetConsoleOutput(args: Types.ToolArgs): string?
	if not args["GetConsoleOutput"] then
		return nil
	end

	local result = ConsoleOutput.getOutput()
	return result
end

return handleGetConsoleOutput :: Types.ToolFunction
