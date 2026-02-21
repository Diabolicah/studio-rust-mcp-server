local HttpService = game:GetService("HttpService")
local Main = script:FindFirstAncestor("MCPStudioPlugin")
local GlobalVariables = require(Main.Utils.GlobalVariables)

local MockWebSocketClient = {}
MockWebSocketClient.__index = MockWebSocketClient

local EnumWebSocketState = {
	Connecting = "Connecting",
	Open = "Open",
	Reconnecting = "Reconnecting",
	Closing = "Closing",
	Closed = "Closed",
}

local CloseableStates = {
	[EnumWebSocketState.Connecting] = true,
	[EnumWebSocketState.Open] = true,
	[EnumWebSocketState.Reconnecting] = true,
}

local POLL_WAIT_TIME = 1
local RECONNECT_MAX_WAIT = 5

export type MockWebSocketClient = {
	Send: (self: MockWebSocketClient, data: any) -> (),
	Close: (self: MockWebSocketClient) -> (),
	SetReceiveEndpoint: (self: MockWebSocketClient, endpoint: string) -> (),
	SetSendEndpoint: (self: MockWebSocketClient, endpoint: string) -> (),
	SetTransportState: (self: MockWebSocketClient, state: string, reason: string?) -> (),
	Opened: RBXScriptSignal,
	Closed: RBXScriptSignal,
	MessageReceived: RBXScriptSignal,
	TransportStateChanged: RBXScriptSignal,
	ConnectionState: typeof(EnumWebSocketState.Connecting),
	TransportState: string,
}

type MockWebSocketClientPrivate = MockWebSocketClient & {
	new: (uri: string) -> MockWebSocketClient,
	_OpenImpl: (self: MockWebSocketClient) -> (),
	_uri: string,
	_receiveEndpoint: string,
	_sendEndpoint: string,
	_pollTask: thread?,
	_TransportStateChangedEvent: BindableEvent,
	_OpenedEvent: BindableEvent,
	_ClosedEvent: BindableEvent,
	_MessageReceivedEvent: BindableEvent,
	_consecutiveFailures: number,
	_lastHeartbeat: number,
	_destroyed: boolean,
	_DestroyEvents: (self: MockWebSocketClientPrivate) -> (),
}

function MockWebSocketClient.new(uri: string): MockWebSocketClient
	local self: MockWebSocketClientPrivate = setmetatable({}, MockWebSocketClient) :: any

	self._uri = uri
	self._receiveEndpoint = ""
	self._sendEndpoint = ""
	self._pollTask = nil :: thread?
	self._consecutiveFailures = 0
	self._lastHeartbeat = os.clock()
	self._destroyed = false

	self._OpenedEvent = Instance.new("BindableEvent")
	self.Opened = self._OpenedEvent.Event

	self._ClosedEvent = Instance.new("BindableEvent")
	self.Closed = self._ClosedEvent.Event

	self._MessageReceivedEvent = Instance.new("BindableEvent")
	self.MessageReceived = self._MessageReceivedEvent.Event

	self._TransportStateChangedEvent = Instance.new("BindableEvent")
	self.TransportStateChanged = self._TransportStateChangedEvent.Event

	self.ConnectionState = EnumWebSocketState.Connecting
	self.TransportState = "connecting"

	task.defer(self._OpenImpl, self)

	return self
end

local function doRequest(url: string, method: "GET" | "POST", body: any)
	local ok, response = pcall(function()
		return HttpService:RequestAsync({
			Url = url,
			Method = method,
			Headers = {
				["Content-Type"] = "application/json",
			},
			Body = if body then HttpService:JSONEncode(body) else nil,
			Compress = Enum.HttpCompression.None,
		})
	end)

	return if ok then response else nil
end

function MockWebSocketClient._DestroyEvents(self: MockWebSocketClientPrivate)
	if self._destroyed then
		return
	end
	self._destroyed = true
	self._OpenedEvent:Destroy()
	self._ClosedEvent:Destroy()
	self._MessageReceivedEvent:Destroy()
	self._TransportStateChangedEvent:Destroy()
end

function MockWebSocketClient._OpenImpl(self: MockWebSocketClientPrivate)
	if self.ConnectionState ~= EnumWebSocketState.Connecting or self._destroyed then
		return
	end

	self.ConnectionState = EnumWebSocketState.Open
	self:SetTransportState("open")
	self._OpenedEvent:Fire()

	self._pollTask = task.spawn(function()
		while self.ConnectionState == EnumWebSocketState.Open or self.ConnectionState == EnumWebSocketState.Reconnecting do
			local response = doRequest(self._uri .. self._receiveEndpoint, "GET")

			if response then
				if response.StatusCode == 200 and response.Success then
					self._consecutiveFailures = 0
					self._lastHeartbeat = os.clock()
					self:SetTransportState("open")
					self._MessageReceivedEvent:Fire(response.Body)
					continue
				elseif response.StatusCode == 423 then
					self._consecutiveFailures = 0
					self._lastHeartbeat = os.clock()
					self:SetTransportState("open")
					continue
				end
			end

			self._consecutiveFailures += 1
			if GlobalVariables.studioMode ~= "stop" then
				-- Play/run transitions can temporarily fail localhost polling.
				-- Avoid transport churn logs while an intentional mode switch is active.
				task.wait(POLL_WAIT_TIME)
				continue
			end
			if self._consecutiveFailures == 1 then
				self:SetTransportState("degraded", "request_failed")
			elseif self._consecutiveFailures >= 3 then
				self:SetTransportState("reconnecting", "heartbeat_lost")
				self.ConnectionState = EnumWebSocketState.Reconnecting
				task.wait(math.min(POLL_WAIT_TIME * self._consecutiveFailures, RECONNECT_MAX_WAIT))
				if self.ConnectionState == EnumWebSocketState.Reconnecting then
					self.ConnectionState = EnumWebSocketState.Open
				end
			else
				task.wait(POLL_WAIT_TIME)
			end
		end
	end)
end

function MockWebSocketClient.Send(self: MockWebSocketClientPrivate, data: any)
	local response = doRequest(self._uri .. self._sendEndpoint, "POST", data)
	if not response or not response.Success or response.StatusCode < 200 or response.StatusCode >= 300 then
		self:SetTransportState("degraded", "send_failed")
	end
end

function MockWebSocketClient.Close(self: MockWebSocketClientPrivate)
	if self.ConnectionState == EnumWebSocketState.Closed then
		return
	end

	if CloseableStates[self.ConnectionState] then
		self.ConnectionState = EnumWebSocketState.Closing
	end

	if self._pollTask and coroutine.status(self._pollTask) ~= "dead" then
		task.cancel(self._pollTask)
		self._pollTask = nil :: any
	end

	self.ConnectionState = EnumWebSocketState.Closed
	self:SetTransportState("closed")
	self._ClosedEvent:Fire()
	self:_DestroyEvents()
end

function MockWebSocketClient.SetTransportState(self: MockWebSocketClientPrivate, state: string, reason: string?)
	if self.TransportState == state then
		return
	end
	self.TransportState = state
	self._TransportStateChangedEvent:Fire(state, reason)
end

-- START DEVIATION: These methods are not present in the real WebSocketClient instance
function MockWebSocketClient.SetReceiveEndpoint(self: MockWebSocketClientPrivate, endpoint: string)
	self._receiveEndpoint = endpoint
end

function MockWebSocketClient.SetSendEndpoint(self: MockWebSocketClientPrivate, endpoint: string)
	self._sendEndpoint = endpoint
end
-- END DEVIATION

local MockWebSocketService = {}
MockWebSocketService.__index = MockWebSocketService

type MockWebSocketService = {
	CreateClient: (self: MockWebSocketService, uri: string) -> MockWebSocketClient,
}

function MockWebSocketService.CreateClient(_: MockWebSocketService, uri: string)
	return MockWebSocketClient.new(uri)
end

return MockWebSocketService
