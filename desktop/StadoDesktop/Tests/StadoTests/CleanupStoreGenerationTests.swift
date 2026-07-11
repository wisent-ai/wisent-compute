import Combine
import Foundation
import XCTest
@testable import Stado

@MainActor
final class CleanupStoreGenerationTests: XCTestCase {
    func testChangingDashboardDiscardsDelayedOldRefreshAndClearsOldState() async throws {
        let defaults = try XCTUnwrap(UserDefaults(suiteName: #function))
        defaults.removePersistentDomain(forName: #function)
        defer { defaults.removePersistentDomain(forName: #function) }
        defaults.set("https://old.example", forKey: "dashboardBaseURL")

        ControlledURLProtocol.gate.reset()
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ControlledURLProtocol.self]
        let session = URLSession(configuration: configuration)
        defer { session.invalidateAndCancel() }

        let store = CleanupStore(
            defaults: defaults,
            client: CleanupClient(session: session),
            startsPolling: false
        )

        let initialRequestStarted = expectation(description: "initial old-server request started")
        ControlledURLProtocol.gate.expectNextArrival(initialRequestStarted)
        let initialRefresh = Task { await store.refresh() }
        await fulfillment(of: [initialRequestStarted], timeout: 1)
        let initialRequest = try XCTUnwrap(ControlledURLProtocol.gate.takeNextRequest())
        XCTAssertEqual(initialRequest.request.url?.host, "old.example")
        ControlledURLProtocol.gate.respond(
            to: initialRequest,
            with: .json(responseJSON(service: "error", outcome: "old-error"))
        )
        await initialRefresh.value

        XCTAssertEqual(store.response?.report.outcome, "old-error")
        XCTAssertNotNil(store.lastUpdated)
        XCTAssertNotNil(store.errorMessage)

        let delayedOldRequestStarted = expectation(description: "delayed old-server request started")
        ControlledURLProtocol.gate.expectNextArrival(delayedOldRequestStarted)
        let delayedOldRefresh = Task { await store.refresh() }
        await fulfillment(of: [delayedOldRequestStarted], timeout: 1)
        let delayedOldRequest = try XCTUnwrap(ControlledURLProtocol.gate.takeNextRequest())
        XCTAssertEqual(delayedOldRequest.request.url?.host, "old.example")

        let newRequestStarted = expectation(description: "new-server request started")
        ControlledURLProtocol.gate.expectNextArrival(newRequestStarted)
        try store.saveDashboardURL("https://new.example")

        XCTAssertNil(store.response, "Changing servers must immediately remove the previous server's report")
        XCTAssertNil(store.lastUpdated, "Changing servers must immediately remove the previous server's timestamp")
        XCTAssertNil(store.errorMessage, "Changing servers must immediately remove the previous server's error")

        await fulfillment(of: [newRequestStarted], timeout: 1)
        let newRequest = try XCTUnwrap(ControlledURLProtocol.gate.takeNextRequest())
        XCTAssertEqual(newRequest.request.url?.host, "new.example", "A new-server refresh must start while the old request remains blocked")

        let newResponseApplied = expectation(description: "new server response applied")
        var observation: AnyCancellable?
        observation = store.$response.sink { response in
            if response?.report.outcome == "new-server" {
                newResponseApplied.fulfill()
            }
        }
        ControlledURLProtocol.gate.respond(
            to: newRequest,
            with: .json(responseJSON(service: "cleanup", outcome: "new-server"))
        )
        await fulfillment(of: [newResponseApplied], timeout: 1)
        withExtendedLifetime(observation) {}

        ControlledURLProtocol.gate.respond(
            to: delayedOldRequest,
            with: .json(responseJSON(service: "cleanup", outcome: "stale-old-server"))
        )
        await delayedOldRefresh.value

        XCTAssertEqual(store.response?.report.outcome, "new-server")
        XCTAssertNil(store.errorMessage)
    }

    private func responseJSON(service: String, outcome: String) -> String {
        """
        {
          "ok": true,
          "service": "\(service)",
          "report": {
            "version": 1,
            "duration_ms": 0,
            "outcome": "\(outcome)",
            "cleaners": {
              "huggingface_cache": {
                "scanned_items": 0,
                "eligible_items": 0,
                "deleted_items": 0,
                "expected_bytes": 0,
                "actual_free_delta_bytes": 0,
                "skipped": {}
              },
              "weles_recordings": {
                "scanned_items": 0,
                "eligible_items": 0,
                "deleted_items": 0,
                "expected_bytes": 0,
                "actual_free_delta_bytes": 0,
                "skipped": {}
              }
            },
            "caps": {
              "bytes": false,
              "items": false,
              "scan": false,
              "deadline": false
            },
            "lock_busy": false,
            "active_slot_count": 0,
            "errors": []
          }
        }
        """
    }
}

private final class RequestGate: @unchecked Sendable {
    struct Stub: Sendable {
        let statusCode: Int
        let data: Data

        static func json(_ body: String, statusCode: Int = 200) -> Stub {
            Stub(statusCode: statusCode, data: Data(body.utf8))
        }
    }

    struct Ticket: Sendable {
        fileprivate let id: UUID
        let request: URLRequest
    }

    private let condition = NSCondition()
    private var arrivals: [Ticket] = []
    private var arrivalExpectations: [XCTestExpectation] = []
    private var responses: [UUID: Stub] = [:]

    func reset() {
        condition.lock()
        defer { condition.unlock() }
        precondition(responses.isEmpty && arrivalExpectations.isEmpty)
        arrivals.removeAll()
    }

    func response(for request: URLRequest) -> Stub {
        condition.lock()
        let ticket = Ticket(id: UUID(), request: request)
        arrivals.append(ticket)
        if !arrivalExpectations.isEmpty {
            arrivalExpectations.removeFirst().fulfill()
        }

        while responses[ticket.id] == nil {
            condition.wait()
        }
        let stub = responses.removeValue(forKey: ticket.id)!
        condition.unlock()
        return stub
    }

    func expectNextArrival(_ expectation: XCTestExpectation) {
        condition.lock()
        arrivalExpectations.append(expectation)
        condition.unlock()
    }

    func takeNextRequest() -> Ticket? {
        condition.lock()
        defer { condition.unlock() }
        return arrivals.isEmpty ? nil : arrivals.removeFirst()
    }

    func respond(to ticket: Ticket, with stub: Stub) {
        condition.lock()
        responses[ticket.id] = stub
        condition.broadcast()
        condition.unlock()
    }
}

private final class ControlledURLProtocol: URLProtocol, @unchecked Sendable {
    static let gate = RequestGate()

    override class func canInit(with request: URLRequest) -> Bool { true }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let delivery = URLProtocolDelivery(protocolInstance: self, client: client)
        let request = request
        DispatchQueue.global().async {
            let stub = Self.gate.response(for: request)
            delivery.finish(request: request, stub: stub)
        }
    }

    override func stopLoading() {}
}

private final class URLProtocolDelivery: @unchecked Sendable {
    private let protocolInstance: URLProtocol
    private let client: URLProtocolClient?

    init(protocolInstance: URLProtocol, client: URLProtocolClient?) {
        self.protocolInstance = protocolInstance
        self.client = client
    }

    func finish(request: URLRequest, stub: RequestGate.Stub) {
        guard let url = request.url,
              let response = HTTPURLResponse(
                  url: url,
                  statusCode: stub.statusCode,
                  httpVersion: "HTTP/1.1",
                  headerFields: ["Content-Type": "application/json"]
              )
        else {
            client?.urlProtocol(protocolInstance, didFailWithError: URLError(.badURL))
            return
        }
        client?.urlProtocol(protocolInstance, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(protocolInstance, didLoad: stub.data)
        client?.urlProtocolDidFinishLoading(protocolInstance)
    }
}
