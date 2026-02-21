local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)

local HttpService = game:GetService("HttpService")

local function getTableType(arg)
	local hasArray = false
	local hasTable = false
	for key, _value in arg do
		if typeof(key) == "number" then
			hasArray = true
		else
			hasTable = true
		end
		if hasArray and hasTable then
			break
		end
	end
	return hasArray, hasTable
end

local function serializeValue(arg: any, visited: { [table]: boolean }): any
	local argType = typeof(arg)
	if argType == "table" then
		if visited[arg] then
			return "<cycle>"
		end
		visited[arg] = true

		local _, isTable = getTableType(arg)
		local serialized = {}
		for key, value in arg do
			local serializedKey = serializeValue(key, visited)
			serialized[if isTable then tostring(serializedKey) else serializedKey] = serializeValue(value, visited)
		end

		visited[arg] = nil
		return serialized
	end

	if argType == "Instance" or argType == "userdata" then
		return tostring(arg)
	end

	return arg
end

local function packedToSerializable(packed: { [any]: any }): { any }
	local n = packed.n or #packed
	local values = table.create(n)
	for i = 1, n do
		values[i] = serializeValue(packed[i], {})
	end
	return values
end

local function packedToStrings(packed: { [any]: any }): { string }
	local values = packedToSerializable(packed)
	local strValues = table.create(#values)
	for i, value in values do
		if typeof(value) == "table" then
			strValues[i] = HttpService:JSONEncode(value)
		else
			strValues[i] = tostring(value)
		end
	end
	return strValues
end

local function runCode(command: string): string
	local textOutput = ""
	local logs = {}
	local warnings = {}
	local errors = {}
	local returnedValues = {}
	local succeeded = true
	local runtimeError = nil

	local function addRecord(level: string, ...)
		local packed = table.pack(...)
		local values = packedToStrings(packed)
		local message = table.concat(values, "\t")
		table.insert(logs, {
			level = level,
			message = message,
			ts = os.clock(),
		})
		return message
	end

	local function appendText(header: string, ...)
		local message = addRecord(header, ...)
		textOutput ..= "[" .. header .. "] " .. message .. "\n"
	end

	local function executeCode()
		local chunk = loadstring(command) :: any
		local chunkfenv = getfenv(chunk)

		local oldPrint = print
		chunkfenv.print = function(...)
			oldPrint(...)
			appendText("output", ...)
		end

		local oldWarn = warn
		chunkfenv.warn = function(...)
			oldWarn(...)
			local message = appendText("warning", ...)
			table.insert(warnings, {
				level = "warning",
				message = message,
				ts = os.clock(),
			})
		end

		local oldError = error
		chunkfenv.error = function(...)
			local message = appendText("error", ...)
			table.insert(errors, {
				level = "error",
				message = message,
				ts = os.clock(),
			})
			oldError(...)
		end

		local results = table.pack(chunk())
		returnedValues = packedToSerializable(results)
		appendText("returned_results", table.unpack(results, 1, results.n or #results))
	end

	local function buildPayload(): string
		local payload = {
			success = succeeded,
			error = runtimeError,
			returns = returnedValues,
			logs = logs,
			warnings = warnings,
			errors = errors,
			text_output = textOutput,
		}
		return HttpService:JSONEncode(payload)
	end

	local function runAndCapture(): string
		local ok, resultOrError = pcall(executeCode)
		if not ok then
			succeeded = false
			runtimeError = tostring(resultOrError)
			appendText("unexpected_error", runtimeError)
			table.insert(errors, {
				level = "unexpected_error",
				message = runtimeError,
				ts = os.clock(),
			})
		end
		return buildPayload()
	end

	return runAndCapture()
end

local function handleRunCode(args: Types.ToolArgs): string?
	if not args["RunCode"] then
		return nil
	end

	local runCodeArgs: Types.RunCodeArgs = args["RunCode"]
	if type(runCodeArgs.command) ~= "string" then
		error("Missing command in RunCode")
	end

	return runCode(runCodeArgs.command)
end

return handleRunCode :: Types.ToolFunction
