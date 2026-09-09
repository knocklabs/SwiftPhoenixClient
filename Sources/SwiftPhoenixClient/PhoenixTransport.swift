// Copyright (c) 2021 David Stump <david@davidstump.net>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import Foundation


//----------------------------------------------------------------------
// MARK: - Transport Protocol
//----------------------------------------------------------------------
/**
 Defines a `Socket`'s Transport layer.
 */
// sourcery: AutoMockable
public protocol PhoenixTransport {
  
  /// The current `ReadyState` of the `Transport` layer
  var readyState: PhoenixTransportReadyState { get }
  
  /// Delegate for the `Transport` layer
  var delegate: PhoenixTransportDelegate? { get set }
  
  /**
   Connect to the server
   
   - Parameters:
   - headers: Headers to include in the URLRequests when opening the Websocket connection. Can be empty [:]
   */
  func connect(with headers: [String: Any])
  
  /**
   Disconnect from the server.
   
   - Parameters:
   - code: Status code as defined by <ahref="http://tools.ietf.org/html/rfc6455#section-7.4">Section 7.4 of RFC 6455</a>.
   - reason: Reason why the connection is closing. Optional.
   */
  func disconnect(code: Int, reason: String?)
  
  /**
   Sends a message to the server.
   
   - Parameter data: Data to send.
   */
  func send(data: Data)
}


//----------------------------------------------------------------------
// MARK: - Transport Delegate Protocol
//----------------------------------------------------------------------
/**
 Delegate to receive notifications of events that occur in the `Transport` layer
 
 `URLSessionTransport` delivers these callbacks serially, on an unspecified background thread,
 and never while holding the lock that guards its own state. Calling back into the `Socket` or
 the transport from a callback is therefore safe.
 
 Clearing `delegate` waits for any in-flight callback to return, so once it completes no
 further callback will arrive. Blocking inside a callback delays subsequent callbacks and any
 concurrent `delegate` assignment, but not the transport's state transitions.
 */
public protocol PhoenixTransportDelegate {
  
  /**
   Notified when the `Transport` opens.
   
   - Parameter response: Response from the server indicating that the WebSocket handshake was successful and the connection has been upgraded to webSockets
   */
  func onOpen(response: URLResponse?)
  
  /**
   Notified when the `Transport` receives an error.
   
   - Parameter error: Client-side error from the underlying `Transport` implementation
   - Parameter response: Response from the server, if any, that occurred with the Error
   
   */
  func onError(error: Error, response: URLResponse?)
  
  /**
   Notified when the `Transport` receives a message from the server.
   
   - Parameter message: Message received from the server
   */
  func onMessage(message: String)
  
  /**
   Notified when the `Transport` closes.
   
   - Parameter code: Code that was sent when the `Transport` closed
   - Parameter reason: A concise human-readable prose explanation for the closure
   */
  func onClose(code: Int, reason: String?)
}

//----------------------------------------------------------------------
// MARK: - Transport Ready State Enum
//----------------------------------------------------------------------
/**
 Available `ReadyState`s of a `Transport` layer.
 */
public enum PhoenixTransportReadyState {
  
  /// The `Transport` is opening a connection to the server.
  case connecting
  
  /// The `Transport` is connected to the server.
  case open
  
  /// The `Transport` is closing the connection to the server.
  case closing
  
  /// The `Transport` has disconnected from the server.
  case closed
  
}

//----------------------------------------------------------------------
// MARK: - Default Websocket Transport Implementation
//----------------------------------------------------------------------
/**
 A `Transport` implementation that relies on URLSession's native WebSocket
 implementation.
 
 This implementation ships default with SwiftPhoenixClient however
 SwiftPhoenixClient supports earlier OS versions using one of the submodule
 `Transport` implementations. Or you can create your own implementation using
 your own WebSocket library or implementation.
 */
@available(macOS 10.15, iOS 13, watchOS 6, tvOS 13, *)
open class URLSessionTransport: NSObject, PhoenixTransport, URLSessionWebSocketDelegate {
  
  /// Guards all mutable transport state.
  ///
  /// Held only for state reads and writes, never across a delegate callback. Delivering a
  /// callback while holding this would make it a lock with unbounded hold time: `Socket`'s
  /// open/close handlers call into the process-global `Defaults.heartbeatQueue`, while the
  /// heartbeat handler runs on that queue and reads `readyState` from here, so the two would
  /// deadlock. Reentrant calls run inline so a callback can read state, send, or disconnect.
  private let eventQueue = DispatchQueue(label: "com.phoenix.transport.events")
  private let eventQueueKey = DispatchSpecificKey<Void>()
  
  /// Serializes delegate callbacks with each other and with changes to `delegate`.
  ///
  /// Taken without `eventQueue` held, so it never participates in a cycle with a lock a
  /// callback might acquire. Clearing `delegate` blocks until an in-flight callback returns,
  /// which is the drain `Socket.disconnect()` depends on. Recursive so a callback that
  /// reentrantly disconnects or reassigns the delegate does not deadlock against itself.
  private let deliveryLock = NSRecursiveLock()
  
  /// Where receive results are handled. Keeps that work off both the Swift concurrency
  /// cooperative pool and `eventQueue`, so delivery can happen with no queue held.
  private let receiveQueue = DispatchQueue(label: "com.phoenix.transport.receive")
  
  /// The URL to connect to
  internal let url: URL
  
  /// The URLSession configuration
  internal let configuration: URLSessionConfiguration
    
  /// The underling URLSession. Assigned during `connect()`
  private var session: URLSession? = nil
  
  /// The ongoing task. Assigned during `connect()`
  private var task: URLSessionWebSocketTask? = nil

  /// Holds the current receive task
  private var receiveMessageTask: Task<Void, Never>?
  
  private var _readyState: PhoenixTransportReadyState = .closed
  private var _delegate: PhoenixTransportDelegate? = nil
  
  /// Identifies the current connection attempt.
  ///
  /// A cancelled URLSession task and the detached `receive()` task can both deliver events
  /// after a new connection has already been opened. Stamping every event with the generation
  /// it originated from lets those stragglers be dropped instead of being applied to, or
  /// tearing down, the live connection.
  private var generation: UInt64 = 0
  
  /// Whether this generation has already reported an error or close.
  ///
  /// The URLSession delegate and the receive task can both observe the same failure, and each
  /// would otherwise report it. This is deliberately not derived from `_readyState`, because
  /// the public `readyState` setter lets callers move the state to `.closed` without any event
  /// having been delivered.
  private var hasDeliveredTerminalEvent = false
  
  /// Whether `disconnect` was called, as opposed to the connection failing on its own.
  ///
  /// Cancelling a task surfaces as an error, so this distinguishes the cancellation we asked
  /// for from a genuine failure. Orthogonal to `hasDeliveredTerminalEvent`: this is set when
  /// the close is requested, that is set when an event is actually delivered.
  private var isClosingIntentionally = false
  
  /// Counts how many times a receive re-arm was attempted. Test observability only.
  private var receiveRearmAttempts = 0
  
  /**
   Initializes a `Transport` layer built using URLSession's WebSocket
   
   Example:
   
   ```swift
   let url = URL("wss://example.com/socket")
   let transport: Transport = URLSessionTransport(url: url)
   ```
     
   Using a custom `URLSessionConfiguration`

   ```swift
   let url = URL("wss://example.com/socket")
   let configuration = URLSessionConfiguration.default
   let transport: Transport = URLSessionTransport(url: url, configuration: configuration)
   ```
   
   - parameter url: URL to connect to
   - parameter configuration: Provide your own URLSessionConfiguration. Uses `.default` if none provided
   */
  public init(url: URL, configuration: URLSessionConfiguration = .default) {
  
    // URLSession requires that the endpoint be "wss" instead of "https".
    let endpoint = url.absoluteString
    let wsEndpoint = endpoint
      .replacingOccurrences(of: "http://", with: "ws://")
      .replacingOccurrences(of: "https://", with: "wss://")
    
    // Force unwrapping should be safe here since a valid URL came in and we just
    // replaced the protocol.
    self.url = URL(string: wsEndpoint)!
    self.configuration = configuration
    
    super.init()
    eventQueue.setSpecific(key: eventQueueKey, value: ())
  }
  
  deinit {
    syncOnEventQueue {
      self._delegate = nil
      self.cancelReceiveTask()
      self.session?.invalidateAndCancel()
      self.session = nil
      self.task = nil
    }
  }
  
  
  // MARK: - Transport
  public var readyState: PhoenixTransportReadyState {
    get { syncOnEventQueue { _readyState } }
    set { syncOnEventQueue { _readyState = newValue } }
  }
  
  public var delegate: PhoenixTransportDelegate? {
    get { syncOnEventQueue { _delegate } }
    set {
      // Waits for any in-flight callback so the caller can rely on no further delivery.
      deliveryLock.lock()
      defer { deliveryLock.unlock() }
      syncOnEventQueue { _delegate = newValue }
    }
  }
  
  public func connect(with headers: [String : Any]) {
    syncOnEventQueue {
      self.resetForNewConnection()
      self._readyState = .connecting
      
      // Deliberately not backed by `eventQueue`: URLSession callbacks must be free to take
      // that queue for state and then release it before delivering to the delegate.
      let operationQueue = OperationQueue()
      operationQueue.name = "com.phoenix.transport.session"
      operationQueue.maxConcurrentOperationCount = 1
      
      self.session = URLSession(configuration: self.configuration,
                                delegate: self,
                                delegateQueue: operationQueue)
      var request = URLRequest(url: url)
      
      headers.forEach { (key: String, value: Any) in
        guard let value = value as? String else { return }
        request.addValue(value, forHTTPHeaderField: key)
      }
      
      self.task = self.session?.webSocketTask(with: request)
      self.task?.resume()
    }
  }
  
  open func disconnect(code: Int, reason: String?) {
    /*
     TODO:
     1. Provide a "strict" mode that fails if an invalid close code is given
     2. If strict mode is disabled, default to CloseCode.invalid
     3. Provide default .normalClosure function
     */
    guard let closeCode = URLSessionWebSocketTask.CloseCode.init(rawValue: code) else {
      fatalError("Could not create a CloseCode with invalid code: [\(code)].")
    }
    
    syncOnEventQueue {
      self.isClosingIntentionally = true
      self._readyState = .closing
      self.cancelReceiveTask()
      self.task?.cancel(with: closeCode, reason: reason?.data(using: .utf8))
      self.session?.finishTasksAndInvalidate()
    }
  }
  
  open func send(data: Data) {
    let currentTask: URLSessionWebSocketTask? = syncOnEventQueue { task }
    currentTask?.send(.string(String(data: data, encoding: .utf8)!)) { (error) in
      // TODO: What is the behavior when an error occurs?
    }
  }
  
  
  // MARK: - URLSessionWebSocketDelegate
  open func urlSession(_ session: URLSession,
                       webSocketTask: URLSessionWebSocketTask,
                       didOpenWithProtocol protocol: String?) {
    // Read the generation under the queue, then release it: the handlers re-check it before
    // acting, so a generation that changes in between just means the event is stale.
    handleOpen(response: webSocketTask.response,
               generation: currentGeneration(),
               session: session,
               task: webSocketTask)
  }
  
  open func urlSession(_ session: URLSession,
                       webSocketTask: URLSessionWebSocketTask,
                       didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
                       reason: Data?) {
    handleClose(code: closeCode.rawValue,
                reason: reason.flatMap { String(data: $0, encoding: .utf8) },
                generation: currentGeneration(),
                session: session,
                task: webSocketTask)
  }
  
  open func urlSession(_ session: URLSession,
                       task: URLSessionTask,
                       didCompleteWithError error: Error?) {
    // The task has terminated. Inform the delegate that the transport has closed abnormally
    // if this was caused by an error.
    guard let err = error else { return }
    
    handleFailure(err,
                  response: task.response,
                  generation: currentGeneration(),
                  session: session,
                  task: task)
  }
  
  
  // MARK: - Private
  @discardableResult
  private func syncOnEventQueue<T>(_ work: () -> T) -> T {
    if DispatchQueue.getSpecific(key: eventQueueKey) != nil {
      return work()
    }
    return eventQueue.sync(execute: work)
  }
  
  /// Retires the previous connection attempt and starts a new generation.
  private func resetForNewConnection() {
    generation += 1
    hasDeliveredTerminalEvent = false
    isClosingIntentionally = false
    cancelReceiveTask()
    session?.finishTasksAndInvalidate()
    session = nil
    task = nil
  }
  
  private func currentGeneration() -> UInt64 {
    syncOnEventQueue { generation }
  }
  
  private func cancelReceiveTask() {
    receiveMessageTask?.cancel()
    receiveMessageTask = nil
  }
  
  /// Whether an event still belongs to the live connection attempt.
  ///
  /// `session` and `task` are nil for events that carry no URLSession identity, in which case
  /// the generation alone decides.
  private func isCurrent(_ eventGeneration: UInt64,
                         session: URLSession?,
                         task: URLSessionTask?) -> Bool {
    guard eventGeneration == generation else { return false }
    if let session = session, session !== self.session { return false }
    if let task = task, task !== self.task { return false }
    return true
  }
  
  /// Whether an error is just the acknowledgement of a close we asked for.
  private func isExpectedTeardownError(_ error: Error) -> Bool {
    isClosingIntentionally && isCancellationError(error)
  }
  
  private func isCancellationError(_ error: Error) -> Bool {
    if error is CancellationError { return true }
    if let urlError = error as? URLError, urlError.code == .cancelled { return true }
    
    let nsError = error as NSError
    if nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled { return true }
    if nsError.domain == NSPOSIXErrorDomain && (nsError.code == 89 || nsError.code == 57) {
      return true
    }
    return false
  }
  
  /// Delivers to whichever delegate is current at delivery time, with the event queue released.
  ///
  /// The delegate is read *inside* the delivery lock, which the `delegate` setter also takes.
  /// Reading it before acquiring the lock would let a callback fire against a delegate that had
  /// since been cleared, racing whatever the clearing caller went on to do.
  private func deliver(_ body: (PhoenixTransportDelegate) -> Void) {
    deliveryLock.lock()
    defer { deliveryLock.unlock() }
    guard let delegate = syncOnEventQueue({ _delegate }) else { return }
    body(delegate)
  }
  
  private func handleOpen(response: URLResponse?,
                          generation eventGeneration: UInt64,
                          session: URLSession?,
                          task: URLSessionTask?) {
    let didOpen: Bool = syncOnEventQueue {
      guard isCurrent(eventGeneration, session: session, task: task) else { return false }
      // Only a connecting transport can open; re-entry would arm a second receive loop.
      guard _readyState == .connecting else { return false }
      
      _readyState = .open
      armReceive()
      return true
    }
    
    guard didOpen else { return }
    deliver { $0.onOpen(response: response) }
  }
  
  private func handleClose(code: Int,
                           reason: String?,
                           generation eventGeneration: UInt64,
                           session: URLSession?,
                           task: URLSessionTask?) {
    let didClose: Bool = syncOnEventQueue {
      guard isCurrent(eventGeneration, session: session, task: task) else { return false }
      guard !hasDeliveredTerminalEvent else { return false }
      
      hasDeliveredTerminalEvent = true
      _readyState = .closed
      cancelReceiveTask()
      return true
    }
    
    guard didClose else { return }
    deliver { $0.onClose(code: code, reason: reason) }
  }
  
  private func handleFailure(_ error: Error,
                             response: URLResponse?,
                             generation eventGeneration: UInt64,
                             session: URLSession?,
                             task: URLSessionTask?) {
    let didFail: Bool = syncOnEventQueue {
      guard isCurrent(eventGeneration, session: session, task: task) else { return false }
      guard !hasDeliveredTerminalEvent, !isExpectedTeardownError(error) else { return false }
      
      hasDeliveredTerminalEvent = true
      _readyState = .closed
      cancelReceiveTask()
      return true
    }
    
    guard didFail else { return }
    
    // One acquisition for both callbacks, so nothing interleaves between error and close.
    deliveryLock.lock()
    defer { deliveryLock.unlock() }
    
    guard let delegate = syncOnEventQueue({ _delegate }) else { return }
    delegate.onError(error: error, response: response)
    
    // `onError` may have reentrantly disconnected or reconnected. A user-initiated close
    // reports itself, so stacking an abnormal close on top of it would double-report.
    let closeDelegate: PhoenixTransportDelegate? = syncOnEventQueue {
      guard isCurrent(eventGeneration, session: session, task: task),
            !isClosingIntentionally else { return nil }
      return _delegate
    }
    
    closeDelegate?.onClose(code: Socket.CloseCode.abnormal.rawValue,
                           reason: error.localizedDescription)
  }
  
  private func handleReceiveResult(_ result: Result<URLSessionWebSocketTask.Message, Error>,
                                   generation eventGeneration: UInt64,
                                   task: URLSessionWebSocketTask?) {
    switch result {
    case .success(let message):
      let text: String? = syncOnEventQueue {
        guard isCurrent(eventGeneration, session: nil, task: task), _readyState == .open else {
          return nil
        }
        switch message {
        case .data:
          print("Data received. This method is unsupported by the Client")
          return nil
        case .string(let text):
          return text
        default:
          fatalError("Nil message received.")
        }
      }
      
      guard let text else { return }
      
      deliver { $0.onMessage(message: text) }
      
      // `onMessage` may have reentrantly disconnected or reconnected.
      syncOnEventQueue {
        guard isCurrent(eventGeneration, session: nil, task: task), _readyState == .open else {
          return
        }
        receiveRearmAttempts += 1
        armReceive()
      }
      
    case .failure(let error):
      handleFailure(error,
                    response: nil,
                    generation: eventGeneration,
                    session: nil,
                    task: task)
    }
  }
  
  /// Must be called with the event queue held.
  private func armReceive() {
    let currentGeneration = generation
    guard let currentTask = task, _readyState == .open else { return }
    
    cancelReceiveTask()
    receiveMessageTask = Task { [weak self] in
      let result: Result<URLSessionWebSocketTask.Message, Error>
      do {
        result = .success(try await currentTask.receive())
      } catch {
        result = .failure(error)
      }
      guard let self else { return }
      // Hand off rather than blocking here: blocking a cooperative-pool thread is unsupported.
      self.receiveQueue.async {
        self.handleReceiveResult(result, generation: currentGeneration, task: currentTask)
      }
    }
  }
}


// MARK: - Test Seams
@available(macOS 10.15, iOS 13, watchOS 6, tvOS 13, *)
@_spi(TransportTesting)
extension URLSessionTransport {
  public enum TestEvent {
    case open(URLResponse?)
    case close(code: Int, reason: String?)
    case completeWithError(Error, URLResponse?)
    case receiveMessage(URLSessionWebSocketTask.Message)
    case receiveFailure(Error)
  }
  
  public var test_generation: UInt64 {
    syncOnEventQueue { generation }
  }
  
  public var test_receiveRearmAttempts: Int {
    syncOnEventQueue { receiveRearmAttempts }
  }
  
  @discardableResult
  public func test_simulateConnecting() -> UInt64 {
    syncOnEventQueue {
      self.resetForNewConnection()
      self._readyState = .connecting
      return self.generation
    }
  }
  
  @discardableResult
  public func test_simulateOpen() -> UInt64 {
    syncOnEventQueue {
      if self.generation == 0 {
        self.resetForNewConnection()
      }
      self._readyState = .open
      return self.generation
    }
  }
  
  /// Delivers `event` as though it came from URLSession or the receive task.
  ///
  /// Passing `generation` simulates a straggling event from a superseded connection attempt.
  /// Injected events carry no URLSession identity, so the generation alone gates them.
  public func test_inject(_ event: TestEvent, generation eventGeneration: UInt64? = nil) {
    let generation = eventGeneration ?? currentGeneration()
    switch event {
    case .open(let response):
      handleOpen(response: response, generation: generation, session: nil, task: nil)
    case .close(let code, let reason):
      handleClose(code: code, reason: reason, generation: generation, session: nil, task: nil)
    case .completeWithError(let error, let response):
      handleFailure(error, response: response, generation: generation, session: nil, task: nil)
    case .receiveMessage(let message):
      handleReceiveResult(.success(message), generation: generation, task: nil)
    case .receiveFailure(let error):
      handleReceiveResult(.failure(error), generation: generation, task: nil)
    }
  }
}
