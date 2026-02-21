local Main = script:FindFirstAncestor("MCPStudioPlugin")
local GlobalVariables = require(Main.Utils.GlobalVariables)
local Types = require(Main.Types)

local HttpService = game:GetService("HttpService")

local function handleConsumeDestructiveApproval(args: Types.ToolArgs): string?
	if not args["ConsumeDestructiveApproval"] then
		return nil
	end

	local approved = GlobalVariables.destructiveApprovalsRemaining > 0
	if approved then
		GlobalVariables.destructiveApprovalsRemaining -= 1
	end

	return HttpService:JSONEncode({
		approved = approved,
		remaining = GlobalVariables.destructiveApprovalsRemaining,
		reason = if approved
			then "Approved"
			else "No pending approval. Click 'Approve 1 Destructive' in the MCP toolbar first.",
	})
end

return handleConsumeDestructiveApproval :: Types.ToolFunction
