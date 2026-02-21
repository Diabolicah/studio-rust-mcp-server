local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)

local HttpService = game:GetService("HttpService")

type RunCodeEnvelope = {
	returns: { string },
	logs: { string },
	warnings: { string },
	errors: { string },
}

local function encodeEnvelope(envelope: RunCodeEnvelope): string
	local ok, result = pcall(function()
		return HttpService:JSONEncode(envelope)
	end)
	if ok then
		return result
	end

	return HttpService:JSONEncode({
		returns = {},
		logs = {},
		warnings = {},
		errors = { "Failed to encode run_code envelope: " .. tostring(result) },
	})
end

local function runCodeWithOutput(command: string): string
	local envelope: RunCodeEnvelope = {
		returns = {},
		logs = {},
		warnings = {},
		errors = {},
	}

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

	local function serializeTable(arg): any
		if typeof(arg) == "table" then
			local _, isTable = getTableType(arg)

			local newArg = {}
			for key, value in arg do
				local newKey = serializeTable(key)
				newArg[if isTable then tostring(newKey) else newKey] = serializeTable(value)
			end
			return newArg
		elseif type(arg) == "userdata" then
			return tostring(arg) or "UNABLE_TO_SERIALIZE"
		end
		return arg
	end

	local function deepClone(t, cache)
		local clone = {}
		for key, value in t do
			local newKey = key
			if typeof(key) == "table" then
				if not cache[key] then
					cache[key] = deepClone(key, cache)
				end
				newKey = cache[key]
			end

			local newValue = value
			if typeof(value) == "table" then
				if not cache[value] then
					cache[value] = deepClone(value, cache)
				end
				newValue = cache[value]
			end

			clone[newKey] = newValue
		end
		return clone
	end

	local function toStrTable(t: { any }): { string }
		local clonedTable = deepClone(t, {})
		local strTable = table.create(#clonedTable)
		for i, arg in clonedTable do
			local serializedArg = serializeTable(arg)
			if typeof(serializedArg) == "table" then
				local ok, encoded = pcall(function()
					return HttpService:JSONEncode(serializedArg)
				end)
				strTable[i] = if ok then encoded else tostring(serializedArg)
			elseif serializedArg == nil then
				strTable[i] = "nil"
			else
				strTable[i] = tostring(serializedArg)
			end
		end
		return strTable
	end

	local function addToEnvelope(target: { string }, ...)
		local strResults = toStrTable(table.pack(...))
		table.insert(target, table.concat(strResults, "\t"))
	end

	local function executeCode()
		local chunk, loadErr = loadstring(command)
		if not chunk then
			addToEnvelope(envelope.errors, loadErr)
			return
		end

		local chunkfenv = getfenv(chunk)

		local oldPrint = print
		chunkfenv.print = function(...)
			oldPrint(...)
			addToEnvelope(envelope.logs, ...)
		end

		local oldWarn = warn
		chunkfenv.warn = function(...)
			oldWarn(...)
			addToEnvelope(envelope.warnings, ...)
		end

		local oldError = error
		chunkfenv.error = function(...)
			addToEnvelope(envelope.errors, ...)
			oldError(...)
		end

		local results = table.pack(chunk())
		if #results > 0 then
			addToEnvelope(envelope.returns, table.unpack(results))
		end

		return results
	end

	local ok, errorMessage = pcall(executeCode)
	if not ok then
		addToEnvelope(envelope.errors, errorMessage)
	end

	return encodeEnvelope(envelope)
end

local function handleRunCode(args: Types.ToolArgs): string?
	if not args["RunCode"] then
		return nil
	end

	local runCodeArgs: Types.RunCodeArgs = args["RunCode"]
	if type(runCodeArgs.command) ~= "string" then
		return encodeEnvelope({
			returns = {},
			logs = {},
			warnings = {},
			errors = { "Missing command in RunCode" },
		})
	end

	return runCodeWithOutput(runCodeArgs.command)
end

return handleRunCode :: Types.ToolFunction
