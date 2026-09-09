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
 
 `URLSessionTransport` delivers these callbacks serially, while holding its internal event
 queue. Two consequences:
 
 - Blocking inside a callback stalls the transport, including its teardown.
 - Calling back into the `Socket` or the transport from a callback is safe and runs inline,
   but acquiring a lock of your own *synchronously* from a callback will deadlock if you also
   call `Socket` while holding that lock. Dispatch asynchronously in that case.
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
  
  /// Serializes transport state, lifecycle calls, URLSession callbacks, and receive results.
  /// Reentrant calls from inside a callback run inline so `onOpen`/`onError` can safely
  /// read `readyState`, send, or disconnect without deadlocking.
  private let eventQueue = DispatchQueue(label: "com.phoenix.transport.events")
  private let eventQueueKey = DispatchSpecificKey<Void>()
  
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
    set { syncOnEventQueue { _delegate = newValue } }
  }
  
  public func connect(with headers: [String : Any]) {
    syncOnEventQueue {
      self.resetForNewConnection()
      self._readyState = .connecting
      
      let operationQueue = OperationQueue()
      operationQueue.name = "com.phoenix.transport.session"
      operationQueue.maxConcurrentOperationCount = 1
      operationQueue.underlyingQueue = self.eventQueue
      
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
    syncOnEventQueue {
      self.handleOpen(response: webSocketTask.response,
                      generation: self.generation,
                      session: session,
                      task: webSocketTask)
    }
  }
  
  open func urlSession(_ session: URLSession,
                       webSocketTask: URLSessionWebSocketTask,
                       didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
                       reason: Data?) {
    syncOnEventQueue {
      self.handleClose(code: closeCode.rawValue,
                       reason: reason.flatMap { String(data: $0, encoding: .utf8) },
                       generation: self.generation,
                       session: session,
                       task: webSocketTask)
    }
  }
  
  open func urlSession(_ session: URLSession,
                       task: URLSessionTask,
                       didCompleteWithError error: Error?) {
    // The task has terminated. Inform the delegate that the transport has closed abnormally
    // if this was caused by an error.
    guard let err = error else { return }
    
    syncOnEventQueue {
      self.handleFailure(err,
                         response: task.response,
                         generation: self.generation,
                         session: session,
                         task: task)
    }
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
  
  private func handleOpen(response: URLResponse?,
                          generation eventGeneration: UInt64,
                          session: URLSession?,
                          task: URLSessionTask?) {
    guard isCurrent(eventGeneration, session: session, task: task) else { return }
    guard _readyState == .connecting || _readyState == .open else { return }
    
    _readyState = .open
    armReceive()
    _delegate?.onOpen(response: response)
  }
  
  private func handleClose(code: Int,
                           reason: String?,
                           generation eventGeneration: UInt64,
                           session: URLSession?,
                           task: URLSessionTask?) {
    guard isCurrent(eventGeneration, session: session, task: task) else { return }
    guard !hasDeliveredTerminalEvent else { return }
    
    hasDeliveredTerminalEvent = true
    _readyState = .closed
    cancelReceiveTask()
    _delegate?.onClose(code: code, reason: reason)
  }
  
  private func handleFailure(_ error: Error,
                             response: URLResponse?,
                             generation eventGeneration: UInt64,
                             session: URLSession?,
                             task: URLSessionTask?) {
    guard isCurrent(eventGeneration, session: session, task: task) else { return }
    guard !hasDeliveredTerminalEvent, !isExpectedTeardownError(error) else { return }
    
    hasDeliveredTerminalEvent = true
    _readyState = .closed
    cancelReceiveTask()
    
    _delegate?.onError(error: error, response: response)
    
    // `onError` may have reentrantly disconnected or reconnected. A user-initiated close
    // reports itself, so stacking an abnormal close on top of it would double-report.
    guard isCurrent(eventGeneration, session: session, task: task),
          !isClosingIntentionally,
          let delegate = _delegate else { return }
    
    delegate.onClose(code: Socket.CloseCode.abnormal.rawValue,
                     reason: error.localizedDescription)
  }
  
  private func handleReceiveResult(_ result: Result<URLSessionWebSocketTask.Message, Error>,
                                   generation eventGeneration: UInt64,
                                   task: URLSessionWebSocketTask?) {
    guard isCurrent(eventGeneration, session: nil, task: task) else { return }
    
    switch result {
    case .success(let message):
      guard _readyState == .open else { return }
      switch message {
      case .data:
        print("Data received. This method is unsupported by the Client")
      case .string(let text):
        _delegate?.onMessage(message: text)
      default:
        fatalError("Nil message received.")
      }
      
      // `onMessage` may have reentrantly disconnected or reconnected.
      guard isCurrent(eventGeneration, session: nil, task: task), _readyState == .open else { return }
      receiveRearmAttempts += 1
      armReceive()
      
    case .failure(let error):
      handleFailure(error,
                    response: nil,
                    generation: eventGeneration,
                    session: nil,
                    task: task)
    }
  }
  
  private func armReceive() {
    let currentGeneration = generation
    guard let currentTask = task, _readyState == .open else { return }
    receiveMessageTask = Task { [weak self] in
      let result: Result<URLSessionWebSocketTask.Message, Error>
      do {
        result = .success(try await currentTask.receive())
      } catch {
        result = .failure(error)
      }
      guard let self else { return }
      self.syncOnEventQueue {
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
    syncOnEventQueue {
      let generation = eventGeneration ?? self.generation
      switch event {
      case .open(let response):
        self.handleOpen(response: response, generation: generation, session: nil, task: nil)
      case .close(let code, let reason):
        self.handleClose(code: code, reason: reason, generation: generation, session: nil, task: nil)
      case .completeWithError(let error, let response):
        self.handleFailure(error, response: response, generation: generation, session: nil, task: nil)
      case .receiveMessage(let message):
        self.handleReceiveResult(.success(message), generation: generation, task: nil)
      case .receiveFailure(let error):
        self.handleReceiveResult(.failure(error), generation: generation, task: nil)
      }
    }
  }
}
