local LogService = game:GetService("LogService")

local DEBUG_TOOL_MAX_OUTPUT = 10000

local ConsoleOutput = {
	_lines = {} :: { string },
	_length = 0,
}

function ConsoleOutput.reset()
	table.clear(ConsoleOutput._lines)
	ConsoleOutput._length = 0
end

function ConsoleOutput.getOutput(): string
	return table.concat(ConsoleOutput._lines, "\n")
end

local function appendMessage(message: string)
	local separatorLength = if ConsoleOutput._length == 0 then 0 else 1
	local newTotal = ConsoleOutput._length + separatorLength + #message
	if newTotal > DEBUG_TOOL_MAX_OUTPUT then
		return
	end
	table.insert(ConsoleOutput._lines, message)
	ConsoleOutput._length = newTotal
end

function ConsoleOutput.startListener()
	local connMessageOut = LogService.MessageOut:Connect(function(message: string, _: Enum.MessageType)
		appendMessage(message)
	end)
	return connMessageOut
end

return ConsoleOutput
