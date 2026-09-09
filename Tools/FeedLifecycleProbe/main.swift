@_spi(TransportTesting) import SwiftPhoenixClient
import Foundation

// Validates Knock's FeedModule locking discipline against the *real* Socket and
// URLSessionTransport, which cannot be built on this machine (knock-swift needs UIKit).
//
// MiniFeedModule mirrors FeedModule exactly: a serial lifecycle queue held across socket
// calls (so connect/disconnect are atomic), with socket-callback-initiated teardown hopping
// onto the queue asynchronously.
//
// Running the same scenario with a synchronous hop must deadlock; that is what shows the
// asynchronous hop is load-bearing rather than incidental.

@available(macOS 10.15, iOS 13, *)
final class MiniFeedModule {
    private let lifecycleQueue = DispatchQueue(label: "com.knock.feed.lifecycle")
    private let lifecycleQueueKey = DispatchSpecificKey<Void>()
    private let socket: Socket
    private let handlesErrorSynchronously: Bool
    private let isAtomic: Bool
    private var feedChannel: Channel?
    private var isFeedConnected = false

    init(socket: Socket, handlesErrorSynchronously: Bool, isAtomic: Bool = true) {
        self.socket = socket
        self.handlesErrorSynchronously = handlesErrorSynchronously
        self.isAtomic = isAtomic
        lifecycleQueue.setSpecific(key: lifecycleQueueKey, value: ())

        _ = socket.delegateOnError(to: self) { (self, _) in
            if self.handlesErrorSynchronously {
                self.disconnectFromFeed()                       // the hazardous ordering
            } else {
                self.asyncOnLifecycleQueue { $0.disconnectFromFeed() }
            }
        }
    }

    private func syncOnLifecycleQueue<T>(_ work: () -> T) -> T {
        if DispatchQueue.getSpecific(key: lifecycleQueueKey) != nil { return work() }
        return lifecycleQueue.sync(execute: work)
    }

    private func asyncOnLifecycleQueue(_ work: @escaping (MiniFeedModule) -> Void) {
        lifecycleQueue.async { [weak self] in
            guard let self else { return }
            work(self)
        }
    }

    func connectToFeed() {
        guard isAtomic else { return claimThenActConnect() }
        syncOnLifecycleQueue {
            guard !isFeedConnected else { return }
            let channel = socket.channel("feeds:probe")
            feedChannel = channel
            _ = channel.join()
            isFeedConnected = true
            socket.connect()
        }
    }
    
    func disconnectFromFeed() {
        guard isAtomic else { return claimThenActDisconnect() }
        syncOnLifecycleQueue {
            if let channel = feedChannel {
                channel.leave()
                socket.remove(channel)
                feedChannel = nil
            }
            if isFeedConnected || socket.isConnected || socket.isConnecting {
                socket.disconnect()
            }
            isFeedConnected = false
        }
    }
    
    /// The superseded shape: state claimed under the queue, socket touched after releasing it.
    /// Kept only so the probe can show it really does leak a live socket.
    private func claimThenActConnect() {
        let channelToJoin: Channel? = syncOnLifecycleQueue {
            guard !isFeedConnected else { return nil }
            let channel = socket.channel("feeds:probe")
            feedChannel = channel
            isFeedConnected = true
            return channel
        }
        guard let channelToJoin else { return }
        _ = channelToJoin.join()
        socket.connect()
    }
    
    private func claimThenActDisconnect() {
        let channelToLeave: Channel? = syncOnLifecycleQueue {
            let channel = feedChannel
            feedChannel = nil
            isFeedConnected = false
            return channel
        }
        if let channelToLeave {
            channelToLeave.leave()
            socket.remove(channelToLeave)
        }
        socket.disconnect()
    }

    var isConnected: Bool { syncOnLifecycleQueue { isFeedConnected } }
    var hasChannel: Bool { syncOnLifecycleQueue { feedChannel != nil } }
}

@available(macOS 10.15, iOS 13, *)
func runScenario(handlesErrorSynchronously: Bool,
                 isAtomic: Bool = true) -> (deadlocked: Bool, leakedOpenSocket: Bool) {
    var leaked = false

    for _ in 0..<40 {
        let transport = URLSessionTransport(url: URL(string: "ws://127.0.0.1:1/socket")!)
        let socket = Socket(endPoint: "ws://127.0.0.1:1/socket", transport: { _ in transport })
        socket.logger = { _ in }
        socket.skipHeartbeat = false
        let module = MiniFeedModule(socket: socket,
                                    handlesErrorSynchronously: handlesErrorSynchronously,
                                    isAtomic: isAtomic)

        let group = DispatchGroup()
        for _ in 0..<6 {
            group.enter()
            DispatchQueue.global().async { module.connectToFeed(); group.leave() }
            group.enter()
            DispatchQueue.global().async {
                transport.test_inject(.completeWithError(URLError(.networkConnectionLost), nil))
                group.leave()
            }
            group.enter()
            DispatchQueue.global().async { module.disconnectFromFeed(); group.leave() }
        }

        if group.wait(timeout: .now() + 5) != .success {
            return (deadlocked: true, leakedOpenSocket: leaked)
        }

        module.disconnectFromFeed()
        // A module that reports itself disconnected must not have left a live socket behind.
        // `.closing` is fine: the cancel has already been issued, and the terminal `.closed`
        // only arrives from URLSession, which never confirms against a dead port.
        let isLive = transport.readyState == .open || transport.readyState == .connecting
        if !module.isConnected && !module.hasChannel && isLive {
            leaked = true
        }
    }

    return (deadlocked: false, leakedOpenSocket: leaked)
}

// One scenario per process: these scenarios deliberately wedge threads and abandon
// URLSessions, so sharing a process between them makes results depend on the leftovers.
@available(macOS 10.15, iOS 13, *)
func main() -> Int32 {
    switch CommandLine.arguments.dropFirst().first ?? "shipped" {
    case "shipped":
        let r = runScenario(handlesErrorSynchronously: false)
        print("shipped (atomic + async error hop) -> deadlocked=\(r.deadlocked) leakedOpenSocket=\(r.leakedOpenSocket)")
        return r.deadlocked || r.leakedOpenSocket ? 1 : 0

    case "sync-hop":
        let r = runScenario(handlesErrorSynchronously: true)
        print("sync error hop -> deadlocked=\(r.deadlocked)")
        return r.deadlocked ? 0 : 1        // deadlock is the expected finding here

    case "claim-then-act":
        let r = runScenario(handlesErrorSynchronously: false, isAtomic: false)
        print("claim-then-act -> leakedOpenSocket=\(r.leakedOpenSocket)")
        return r.leakedOpenSocket ? 0 : 1  // leak is the expected finding here

    default:
        print("usage: FeedLifecycleProbe [shipped|sync-hop|claim-then-act]")
        return 2
    }
}

if #available(macOS 10.15, iOS 13, *) {
    exit(main())
} else {
    print("requires macOS 10.15+")
    exit(2)
}
