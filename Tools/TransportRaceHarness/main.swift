@_spi(TransportTesting) import SwiftPhoenixClient
import Foundation

@available(macOS 10.15, iOS 13, *)
enum HarnessError: Error {
  case stub
  case failed(String)
}

@available(macOS 10.15, iOS 13, *)
final class RecordingTransportDelegate: PhoenixTransportDelegate {
  private let lock = NSLock()
  private var _events: [String] = []
  private var _messages: [String] = []
  
  var onOpenHandler: ((URLResponse?) -> Void)?
  var onErrorHandler: ((Error, URLResponse?) -> Void)?
  var onCloseHandler: ((Int, String?) -> Void)?
  
  var events: [String] {
    lock.lock(); defer { lock.unlock() }
    return _events
  }
  
  var messages: [String] {
    lock.lock(); defer { lock.unlock() }
    return _messages
  }
  
  func record(_ event: String) {
    lock.lock()
    _events.append(event)
    lock.unlock()
  }
  
  func onOpen(response: URLResponse?) {
    record("open")
    onOpenHandler?(response)
  }
  
  func onError(error: Error, response: URLResponse?) {
    record("error")
    onErrorHandler?(error, response)
  }
  
  func onMessage(message: String) {
    lock.lock()
    _messages.append(message)
    lock.unlock()
    record("message")
  }
  
  func onClose(code: Int, reason: String?) {
    record("close:\(code)")
    onCloseHandler?(code, reason)
  }
}

@available(macOS 10.15, iOS 13, *)
func makeTransport() -> URLSessionTransport {
  URLSessionTransport(url: URL(string: "ws://localhost:1/socket/websocket")!)
}

@available(macOS 10.15, iOS 13, *)
func wait(_ semaphore: DispatchSemaphore, name: String) throws {
  if semaphore.wait(timeout: .now() + 2) != .success {
    throw HarnessError.failed("timed out waiting for \(name)")
  }
}

@available(macOS 10.15, iOS 13, *)
func assert(_ condition: Bool, _ message: String) throws {
  if !condition { throw HarnessError.failed(message) }
}

@available(macOS 10.15, iOS 13, *)
func testTeardownDoesNotOverlapErrorCallback() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  let errorStarted = DispatchSemaphore(value: 0)
  let continueError = DispatchSemaphore(value: 0)
  let disconnectStarted = DispatchSemaphore(value: 0)
  let disconnectFinished = DispatchSemaphore(value: 0)
  var disconnectCompleted = false
  
  delegate.onErrorHandler = { _, _ in
    errorStarted.signal()
    continueError.wait()
  }
  
  transport.delegate = delegate
  transport.test_simulateConnecting()
  
  DispatchQueue.global(qos: .userInitiated).async {
    transport.test_inject(.completeWithError(HarnessError.stub, nil))
  }
  
  try wait(errorStarted, name: "error callback")
  
  DispatchQueue.global(qos: .userInitiated).async {
    disconnectStarted.signal()
    transport.delegate = nil
    transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
    disconnectCompleted = true
    disconnectFinished.signal()
  }
  
  try wait(disconnectStarted, name: "disconnect start")
  try assert(!disconnectCompleted, "disconnect overlapped in-flight error callback")
  try assert(delegate.events == ["error"], "unexpected events during blocked error: \(delegate.events)")
  
  continueError.signal()
  try wait(disconnectFinished, name: "disconnect finish")
  try assert(disconnectCompleted, "disconnect never completed")
  try assert(transport.delegate == nil, "delegate was not cleared")
  try assert(transport.readyState == .closing, "expected closing, got \(transport.readyState)")
}

@available(macOS 10.15, iOS 13, *)
func testReentrantOnOpen() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  var observedState: PhoenixTransportReadyState?
  
  delegate.onOpenHandler = { _ in
    observedState = transport.readyState
    transport.send(data: Data("[]".utf8))
  }
  
  transport.delegate = delegate
  transport.test_simulateConnecting()
  transport.test_inject(.open(nil))
  
  try assert(observedState == .open, "onOpen could not read readyState")
  try assert(delegate.events == ["open"], "unexpected events: \(delegate.events)")
}

@available(macOS 10.15, iOS 13, *)
func testErrorDisconnectDoesNotDuplicateClose() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  delegate.onErrorHandler = { _, _ in
    transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: "from error")
  }
  
  transport.delegate = delegate
  transport.test_simulateConnecting()
  transport.test_inject(.completeWithError(HarnessError.stub, nil))
  
  try assert(delegate.events == ["error"], "duplicate close delivered: \(delegate.events)")
  try assert(transport.readyState == .closing, "expected closing")
}

@available(macOS 10.15, iOS 13, *)
func testSingleTerminalSequence() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  transport.delegate = delegate
  transport.test_simulateOpen()
  
  let group = DispatchGroup()
  group.enter()
  DispatchQueue.global(qos: .userInitiated).async {
    transport.test_inject(.receiveFailure(HarnessError.stub))
    group.leave()
  }
  group.enter()
  DispatchQueue.global(qos: .userInitiated).async {
    transport.test_inject(.completeWithError(HarnessError.stub, nil))
    group.leave()
  }
  group.enter()
  DispatchQueue.global(qos: .userInitiated).async {
    transport.test_inject(.close(code: 1000, reason: "server"))
    group.leave()
  }
  
  if group.wait(timeout: .now() + 2) != .success {
    throw HarnessError.failed("timed out racing terminal events")
  }
  
  let expected = ["error", "close:\(Socket.CloseCode.abnormal.rawValue)"]
  try assert(delegate.events == expected, "expected \(expected), got \(delegate.events)")
  try assert(transport.readyState == .closed, "expected closed")
}

@available(macOS 10.15, iOS 13, *)
func testStaleGenerationEventsAreDropped() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  transport.delegate = delegate
  
  let generation1 = transport.test_simulateConnecting()
  _ = transport.test_simulateConnecting()
  
  transport.test_inject(.open(nil), generation: generation1)
  transport.test_inject(.receiveMessage(.string("stale")), generation: generation1)
  transport.test_inject(.completeWithError(HarnessError.stub, nil), generation: generation1)
  
  try assert(delegate.events.isEmpty, "stale events were delivered: \(delegate.events)")
  try assert(delegate.messages.isEmpty, "stale message was delivered")
  try assert(transport.readyState == .connecting, "stale events mutated state")
}

@available(macOS 10.15, iOS 13, *)
func testReceiveAfterDisconnectIsIgnored() throws {
  let transport = makeTransport()
  let delegate = RecordingTransportDelegate()
  transport.delegate = delegate
  transport.test_simulateOpen()
  
  transport.test_inject(.receiveMessage(.string("before")))
  try assert(delegate.messages == ["before"], "missing live receive")
  let rearmAttempts = transport.test_receiveRearmAttempts
  try assert(rearmAttempts == 1, "expected one receive re-arm, got \(rearmAttempts)")
  
  transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
  transport.test_inject(.receiveMessage(.string("after")))
  transport.test_inject(.receiveFailure(URLError(.cancelled)))
  
  try assert(delegate.messages == ["before"], "delivered receive after disconnect: \(delegate.messages)")
  try assert(transport.test_receiveRearmAttempts == rearmAttempts, "receive was re-armed after disconnect")
  try assert(delegate.events == ["message"], "unexpected terminal events: \(delegate.events)")
}

@available(macOS 10.15, iOS 13, *)
func testDeallocationAfterBlockedCallback() throws {
  weak var weakTransport: URLSessionTransport?
  weak var weakDelegate: RecordingTransportDelegate?
  let errorStarted = DispatchSemaphore(value: 0)
  let continueError = DispatchSemaphore(value: 0)
  let injectFinished = DispatchSemaphore(value: 0)
  let teardownFinished = DispatchSemaphore(value: 0)
  
  autoreleasepool {
    let transport = makeTransport()
    let delegate = RecordingTransportDelegate()
    delegate.onErrorHandler = { _, _ in
      errorStarted.signal()
      continueError.wait()
    }
    weakTransport = transport
    weakDelegate = delegate
    transport.delegate = delegate
    transport.test_simulateConnecting()
    
    DispatchQueue.global(qos: .userInitiated).async {
      transport.test_inject(.completeWithError(HarnessError.stub, nil))
      injectFinished.signal()
    }
    
    _ = errorStarted.wait(timeout: .now() + 2)
    
    DispatchQueue.global(qos: .userInitiated).async {
      transport.delegate = nil
      transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
      teardownFinished.signal()
    }
    
    continueError.signal()
    _ = injectFinished.wait(timeout: .now() + 2)
    _ = teardownFinished.wait(timeout: .now() + 2)
  }
  
  try assert(weakTransport == nil, "transport leaked after teardown")
  try assert(weakDelegate == nil, "delegate leaked after teardown")
}

@available(macOS 10.15, iOS 13, *)
func testSocketDisconnectRacesTransportFailure() throws {
  let socket = Socket(endPoint: "ws://localhost:1/socket", transport: { url in
    URLSessionTransport(url: url)
  })
  socket.skipHeartbeat = true
  socket.logger = { _ in }
  socket.onError { _ in }
  socket.onClose { _, _ in }
  
  for _ in 0..<50 {
            let transport = makeTransport()
            socket.test_connection = transport
            transport.delegate = socket
            transport.test_simulateOpen()
    
    let group = DispatchGroup()
    group.enter()
    DispatchQueue.global(qos: .userInitiated).async {
      transport.test_inject(.completeWithError(HarnessError.stub, nil))
      group.leave()
    }
    group.enter()
    DispatchQueue.global(qos: .userInitiated).async {
      socket.disconnect()
      group.leave()
    }
    group.enter()
    DispatchQueue.global(qos: .userInitiated).async {
      transport.test_inject(.receiveFailure(HarnessError.stub))
      group.leave()
    }
    
    if group.wait(timeout: .now() + 2) != .success {
      throw HarnessError.failed("socket race timed out")
    }
            try assert(socket.test_connection == nil, "socket connection survived teardown")
  }
}

@available(macOS 10.15, iOS 13, *)
func runHarness() throws {
  let tests: [(String, () throws -> Void)] = [
    ("teardown does not overlap error callback", testTeardownDoesNotOverlapErrorCallback),
    ("reentrant onOpen reads readyState and sends", testReentrantOnOpen),
    ("onError disconnect does not duplicate close", testErrorDisconnectDoesNotDuplicateClose),
    ("single terminal sequence", testSingleTerminalSequence),
    ("stale generation events are dropped", testStaleGenerationEventsAreDropped),
    ("receive after disconnect is ignored", testReceiveAfterDisconnectIsIgnored),
    ("deallocates after blocked callback drains", testDeallocationAfterBlockedCallback),
    ("socket disconnect races transport failure", testSocketDisconnectRacesTransportFailure),
  ]
  
  for (name, test) in tests {
    try test()
    print("ok - \(name)")
  }
  
  print("All \(tests.count) transport race tests passed.")
}

if #available(macOS 10.15, iOS 13, *) {
  do {
    try runHarness()
  } catch {
    fputs("Transport race harness failed: \(error)\n", stderr)
    exit(1)
  }
} else {
  fputs("Transport race harness requires macOS 10.15+\n", stderr)
  exit(1)
}
