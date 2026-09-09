//
//  URLSessionTransportSpec.swift
//  SwiftPhoenixClientTests
//
//  Created by Daniel Rees on 4/1/21.
//  Copyright © 2021 SwiftPhoenixClient. All rights reserved.
//

import Foundation
import Quick
import Nimble
@testable import SwiftPhoenixClient

@available(iOS 13, macOS 10.15, *)
private final class RecordingTransportDelegate: PhoenixTransportDelegate {
  private let lock = NSLock()
  private var _events: [String] = []
  private var _messages: [String] = []
  
  var onOpenHandler: ((URLResponse?) -> Void)?
  var onErrorHandler: ((Error, URLResponse?) -> Void)?
  var onCloseHandler: ((Int, String?) -> Void)?
  var onMessageHandler: ((String) -> Void)?
  
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
    onMessageHandler?(message)
  }
  
  func onClose(code: Int, reason: String?) {
    record("close:\(code)")
    onCloseHandler?(code, reason)
  }
}

class URLSessionTransportSpec: QuickSpec {
  
  override func spec() {
    
    describe("init") {
      it("replaces http with ws protocols") {
        if #available(iOS 13, *) {
          expect(
            URLSessionTransport(url: URL(string:"http://localhost:4000/socket/websocket")!)
              .url.absoluteString
          ).to(equal("ws://localhost:4000/socket/websocket"))
          
          expect(
            URLSessionTransport(url: URL(string:"https://localhost:4000/socket/websocket")!)
              .url.absoluteString
          ).to(equal("wss://localhost:4000/socket/websocket"))
          
          expect(
            URLSessionTransport(url: URL(string:"ws://localhost:4000/socket/websocket")!)
              .url.absoluteString
          ).to(equal("ws://localhost:4000/socket/websocket"))
          
          expect(
            URLSessionTransport(url: URL(string:"wss://localhost:4000/socket/websocket")!)
              .url.absoluteString
          ).to(equal("wss://localhost:4000/socket/websocket"))
          
        } else {
          // Fallback on earlier versions
          expect("wrong iOS version").to(equal("You must run this test on an iOS 13 device"))
        }
      }
        
      it("accepts an override for the configuration") {
        if #available(iOS 13, *) {
          let configuration = URLSessionConfiguration.default
          expect(
            URLSessionTransport(url: URL(string:"wss://localhost:4000")!, configuration: configuration)
                .configuration
          ).to(equal(configuration))
        } else {
          // Fallback on earlier versions
          expect("wrong iOS version").to(equal("You must run this test on an iOS 13 device"))
        }
      }
    }
    
    describe("serialized teardown") {
      func makeTransport() -> URLSessionTransport {
        URLSessionTransport(url: URL(string: "ws://localhost:1/socket/websocket")!)
      }
      
      it("does not overlap teardown with an in-flight error callback") {
        if #available(iOS 13, *) {
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
            transport.test_inject(.completeWithError(TestError.stub, nil))
          }
          
          expect(errorStarted.wait(timeout: .now() + 2)).to(equal(.success))
          
          DispatchQueue.global(qos: .userInitiated).async {
            disconnectStarted.signal()
            transport.delegate = nil
            transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
            disconnectCompleted = true
            disconnectFinished.signal()
          }
          
          expect(disconnectStarted.wait(timeout: .now() + 2)).to(equal(.success))
          expect(disconnectCompleted).to(beFalse())
          expect(delegate.events).to(equal(["error"]))
          
          continueError.signal()
          expect(disconnectFinished.wait(timeout: .now() + 2)).to(equal(.success))
          expect(disconnectCompleted).to(beTrue())
          expect(transport.delegate).to(beNil())
          expect(transport.readyState).to(equal(.closing))
        }
      }
      
      it("allows reentrant readyState reads and sends from onOpen") {
        if #available(iOS 13, *) {
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
          
          expect(observedState).to(equal(.open))
          expect(delegate.events).to(equal(["open"]))
        }
      }
      
      it("does not deadlock or emit a duplicate close when onError disconnects") {
        if #available(iOS 13, *) {
          let transport = makeTransport()
          let delegate = RecordingTransportDelegate()
          
          delegate.onErrorHandler = { _, _ in
            transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: "from error")
          }
          
          transport.delegate = delegate
          transport.test_simulateConnecting()
          transport.test_inject(.completeWithError(TestError.stub, nil))
          
          expect(delegate.events).to(equal(["error"]))
          expect(transport.readyState).to(equal(.closing))
        }
      }
      
      it("delivers a single terminal sequence when receive failure and completeWithError race") {
        if #available(iOS 13, *) {
          let transport = makeTransport()
          let delegate = RecordingTransportDelegate()
          transport.delegate = delegate
          transport.test_simulateOpen()
          
          let group = DispatchGroup()
          group.enter()
          DispatchQueue.global(qos: .userInitiated).async {
            transport.test_inject(.receiveFailure(TestError.stub))
            group.leave()
          }
          group.enter()
          DispatchQueue.global(qos: .userInitiated).async {
            transport.test_inject(.completeWithError(TestError.stub, nil))
            group.leave()
          }
          group.enter()
          DispatchQueue.global(qos: .userInitiated).async {
            transport.test_inject(.close(code: 1000, reason: "server"))
            group.leave()
          }
          
          expect(group.wait(timeout: .now() + 2)).to(equal(.success))
          expect(delegate.events).to(equal(["error", "close:\(Socket.CloseCode.abnormal.rawValue)"]))
          expect(transport.readyState).to(equal(.closed))
        }
      }
      
      it("drops stale generation open, message, and error events") {
        if #available(iOS 13, *) {
          let transport = makeTransport()
          let delegate = RecordingTransportDelegate()
          transport.delegate = delegate
          
          let generation1 = transport.test_simulateConnecting()
          _ = transport.test_simulateConnecting()
          
          transport.test_inject(.open(nil), generation: generation1)
          transport.test_inject(.receiveMessage(.string("stale")), generation: generation1)
          transport.test_inject(.completeWithError(TestError.stub, nil), generation: generation1)
          
          expect(delegate.events).to(beEmpty())
          expect(delegate.messages).to(beEmpty())
          expect(transport.readyState).to(equal(.connecting))
        }
      }
      
      it("does not deliver a receive result or re-arm after disconnect") {
        if #available(iOS 13, *) {
          let transport = makeTransport()
          let delegate = RecordingTransportDelegate()
          transport.delegate = delegate
          transport.test_simulateOpen()
          
          transport.test_inject(.receiveMessage(.string("before")))
          expect(delegate.messages).to(equal(["before"]))
          let armCount = transport.test_receiveArmCount
          expect(armCount).to(equal(1))
          
          transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
          transport.test_inject(.receiveMessage(.string("after")))
          transport.test_inject(.receiveFailure(URLError(.cancelled)))
          
          expect(delegate.messages).to(equal(["before"]))
          expect(transport.test_receiveArmCount).to(equal(armCount))
          expect(delegate.events).to(equal(["message"]))
        }
      }
      
      it("releases the transport after a blocked error callback drains") {
        if #available(iOS 13, *) {
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
              transport.test_inject(.completeWithError(TestError.stub, nil))
              injectFinished.signal()
            }
            
            expect(errorStarted.wait(timeout: .now() + 2)).to(equal(.success))
            
            DispatchQueue.global(qos: .userInitiated).async {
              transport.delegate = nil
              transport.disconnect(code: Socket.CloseCode.normal.rawValue, reason: nil)
              teardownFinished.signal()
            }
            
            continueError.signal()
            expect(injectFinished.wait(timeout: .now() + 2)).to(equal(.success))
            expect(teardownFinished.wait(timeout: .now() + 2)).to(equal(.success))
          }
          
          expect(weakTransport).to(beNil())
          expect(weakDelegate).to(beNil())
        }
      }
      
      it("races socket disconnect with transport failure without crashing") {
        if #available(iOS 13, *) {
          let socket = Socket(endPoint: "ws://localhost:1/socket", transport: { url in
            URLSessionTransport(url: url)
          })
          socket.skipHeartbeat = true
          socket.logger = { _ in }
          socket.onError { _ in }
          socket.onClose { _, _ in }
          
          for _ in 0..<50 {
            let transport = makeTransport()
            socket.connection = transport
            transport.delegate = socket
            transport.test_simulateOpen()
            
            let group = DispatchGroup()
            group.enter()
            DispatchQueue.global(qos: .userInitiated).async {
              transport.test_inject(.completeWithError(TestError.stub, nil))
              group.leave()
            }
            group.enter()
            DispatchQueue.global(qos: .userInitiated).async {
              socket.disconnect()
              group.leave()
            }
            group.enter()
            DispatchQueue.global(qos: .userInitiated).async {
              transport.test_inject(.receiveFailure(TestError.stub))
              group.leave()
            }
            
            expect(group.wait(timeout: .now() + 2)).to(equal(.success))
            expect(socket.connection).to(beNil())
          }
        }
      }
    }
  }
}
