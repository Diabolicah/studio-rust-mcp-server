local Main = script:FindFirstAncestor("MCPStudioPlugin")
local Types = require(Main.Types)

local CollectionService = game:GetService("CollectionService")
local HttpService = game:GetService("HttpService")

local DEFAULT_MAX_RESULTS = 100
local MAX_RESULTS_LIMIT = 1000

local function resolvePath(path: string?): Instance?
	if path == nil or path == "" or path == "game" then
		return game
	end

	local cleanedPath = path:gsub("^game%.", "")
	local current: Instance = game
	for segment in cleanedPath:gmatch("[^%.]+") do
		local nextInstance = current:FindFirstChild(segment)
		if not nextInstance then
			return nil
		end
		current = nextInstance
	end
	return current
end

local function normalizeMaxResults(value: number?): number
	if type(value) ~= "number" then
		return DEFAULT_MAX_RESULTS
	end
	local rounded = math.floor(value)
	if rounded < 1 then
		return 1
	end
	if rounded > MAX_RESULTS_LIMIT then
		return MAX_RESULTS_LIMIT
	end
	return rounded
end

local function normalizeCursor(value: string?): number
	if type(value) ~= "string" or value == "" then
		return 0
	end
	local parsed = tonumber(value)
	if parsed == nil then
		return 0
	end
	local rounded = math.floor(parsed)
	if rounded < 0 then
		return 0
	end
	return rounded
end

local function stringifyAttribute(value: any): string
	if typeof(value) == "table" then
		return HttpService:JSONEncode(value)
	end
	return tostring(value)
end

local function matchesAttributeFilter(instance: Instance, attributes: { [string]: any }?): boolean
	if not attributes then
		return true
	end
	for key, expectedValue in attributes do
		local actualValue = instance:GetAttribute(key)
		if actualValue == nil then
			return false
		end
		if stringifyAttribute(actualValue) ~= stringifyAttribute(expectedValue) then
			return false
		end
	end
	return true
end

local function gatherCandidates(root: Instance, querySelector: string?, tag: string?, includeDescendants: boolean): { Instance }
	local function queryAllDescendants(): { Instance }
		local selector = if type(querySelector) == "string" and querySelector ~= "" then querySelector else "*"
		local ok, queried = pcall(function()
			return root:QueryDescendants(selector)
		end)
		if ok and type(queried) == "table" then
			return queried
		end
		return root:GetDescendants()
	end

	if tag and tag ~= "" then
		local descendantsLookup = {}
		if includeDescendants then
			for _, descendant in queryAllDescendants() do
				descendantsLookup[descendant] = true
			end
		end
		descendantsLookup[root] = true

		local tagged = CollectionService:GetTagged(tag)
		local candidates = {}
		for _, instance in tagged do
			if descendantsLookup[instance] then
				table.insert(candidates, instance)
			end
		end
		return candidates
	end

	if not includeDescendants then
		return { root }
	end

	local descendants = queryAllDescendants()
	table.insert(descendants, 1, root)
	return descendants
end

local function projectProperties(instance: Instance, selectProperties: { string }?): { [string]: any }?
	if type(selectProperties) ~= "table" or #selectProperties == 0 then
		return nil
	end

	local projected = {}
	for _, propertyName in selectProperties do
		if type(propertyName) == "string" and propertyName ~= "" then
			local ok, value = pcall(function()
				return (instance :: any)[propertyName]
			end)
			if ok then
				local valueType = typeof(value)
				if valueType == "table" then
					projected[propertyName] = HttpService:JSONEncode(value)
				elseif valueType == "Instance" or valueType == "userdata" then
					projected[propertyName] = tostring(value)
				else
					projected[propertyName] = value
				end
			end
		end
	end
	return projected
end

local function handleQueryInstances(args: Types.ToolArgs): string?
	if not args["QueryInstances"] then
		return nil
	end

	local queryArgs: Types.QueryInstancesArgs = args["QueryInstances"]
	local root = resolvePath(queryArgs.path)
	if not root then
		error("Invalid path in QueryInstances")
	end

	local className = queryArgs.class_name
	local nameContains = if type(queryArgs.name_contains) == "string" then string.lower(queryArgs.name_contains) else nil
	local tag = if type(queryArgs.tag) == "string" then queryArgs.tag else nil
	local includeDescendants = queryArgs.include_descendants ~= false
	local maxResults = normalizeMaxResults(queryArgs.max_results)
	local cursorOffset = normalizeCursor(queryArgs.cursor)
	local selectProperties = queryArgs.select_properties

	local matched = {}
	for _, instance in gatherCandidates(root, queryArgs.query_selector, tag, includeDescendants) do
		if className and className ~= "" and not instance:IsA(className) then
			continue
		end
		if nameContains and not string.find(string.lower(instance.Name), nameContains, 1, true) then
			continue
		end
		if not matchesAttributeFilter(instance, queryArgs.attributes) then
			continue
		end

		table.insert(matched, {
			name = instance.Name,
			class_name = instance.ClassName,
			full_name = instance:GetFullName(),
			parent = if instance.Parent then instance.Parent:GetFullName() else nil,
			tags = CollectionService:GetTags(instance),
			properties = projectProperties(instance, selectProperties),
		})
	end

	table.sort(matched, function(a, b)
		return a.full_name < b.full_name
	end)

	local totalCount = #matched
	local startIndex = math.min(cursorOffset + 1, totalCount + 1)
	local endIndex = math.min(cursorOffset + maxResults, totalCount)
	local results = {}
	for i = startIndex, endIndex do
		table.insert(results, matched[i])
	end

	local nextCursor = nil
	local truncated = endIndex < totalCount
	if truncated then
		nextCursor = tostring(endIndex)
	end

	return HttpService:JSONEncode({
		root = root:GetFullName(),
		total_count = totalCount,
		count = #results,
		truncated = truncated,
		cursor = tostring(cursorOffset),
		next_cursor = nextCursor,
		select_properties = selectProperties,
		results = results,
	})
end

return handleQueryInstances :: Types.ToolFunction
