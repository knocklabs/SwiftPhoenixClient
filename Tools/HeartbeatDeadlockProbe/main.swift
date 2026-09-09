@_spi(TransportTesting) import SwiftPhoenixClient
import Foundation

// Reproduces H1 against the real library.
//
// Edge 1 (eventQueue -> heartbeatQueue): handleOpen holds eventQueue and calls
//   onOpen -> Socket.onConnectionOpen -> resetHeartbeat -> HeartbeatTimer.stop/start,
//   both of which are Defaults.heartbeatQueue.sync.
//
// Edge 2 (heartbeatQueue -> eventQueue): the heartbeat DispatchSourceTimer handler runs ON
//   heartbeatQueue -> sendHeartbeat -> `guard isConnected` -> connection.readyState,
//   which is eventQueue.sync.

@available(macOS 10.15, iOS 13, *)
func probe() -> Int32 {
    let transport = URLSessionTransport(url: URL(string: "ws://localhost:1/socket")!)
    let socket = Socket(endPoint: "ws://localhost:1/socket", transport: { _ in transport })
    socket.logger = { _ in }
    socket.test_connection = transport
    transport.delegate = socket
    transport.test_simulateConnecting()

    let heartbeatHolds = DispatchSemaphore(value: 0)
    let openStarted = DispatchSemaphore(value: 0)
    let done = DispatchGroup()

    // Thread B: occupy heartbeatQueue, then reach for eventQueue, as sendHeartbeat does.
    done.enter()
    DispatchQueue.global(qos: .userInitiated).async {
        Defaults.heartbeatQueue.sync {
            heartbeatHolds.signal()
            _ = openStarted.wait(timeout: .now() + 2)
            _ = transport.readyState        // eventQueue.sync
        }
        done.leave()
    }

    // Thread A: occupy eventQueue via a delegate callback, which reaches for heartbeatQueue.
    done.enter()
    DispatchQueue.global(qos: .userInitiated).async {
        _ = heartbeatHolds.wait(timeout: .now() + 2)
        openStarted.signal()
        transport.test_inject(.open(nil))   // eventQueue held across onOpen -> resetHeartbeat
        done.leave()
    }

    if done.wait(timeout: .now() + 6) == .success {
        print("NO DEADLOCK")
        return 0
    }
    print("DEADLOCK REPRODUCED: eventQueue and Defaults.heartbeatQueue are wedged")
    return 1
}

if #available(macOS 10.15, iOS 13, *) {
    exit(probe())
} else {
    print("requires macOS 10.15+")
    exit(2)
}
