import Foundation
import Network

struct OperationsDashboardAddress: Equatable, Sendable {
    static let localDefault = "http://127.0.0.1:8765"

    let baseURL: URL

    init(_ value: String) throws {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard var components = URLComponents(string: trimmed),
              let scheme = components.scheme?.lowercased(),
              scheme == "http" || scheme == "https",
              let host = components.url?.host(percentEncoded: false),
              !host.isEmpty,
              components.user == nil,
              components.password == nil,
              components.query == nil,
              components.fragment == nil,
              scheme == "https" || Self.isLoopback(host)
        else {
            throw OperationsClientError.invalidDashboardURL
        }

        components.scheme = scheme
        if components.path == "/" {
            components.path = ""
        }
        while components.path.count > 1 && components.path.hasSuffix("/") {
            components.path.removeLast()
        }
        guard let normalized = components.url else {
            throw OperationsClientError.invalidDashboardURL
        }
        baseURL = normalized
    }

    var displayString: String { baseURL.absoluteString }

    var stateURL: URL { endpoint("api/state.json") }

    func endpoint(_ path: String) -> URL {
        path.split(separator: "/").reduce(baseURL) { partial, component in
            partial.appending(path: String(component))
        }
    }

    private static func isLoopback(_ host: String) -> Bool {
        if let address = IPv4Address(host) {
            return address.rawValue.first == 127
        }
        guard let address = IPv6Address(host) else { return false }
        let bytes = address.rawValue
        return bytes.count == 16 && bytes.dropLast().allSatisfy { $0 == 0 } && bytes.last == 1
    }
}

enum OperationsClientError: LocalizedError, Sendable {
    case invalidDashboardURL
    case invalidResponse
    case server(Int)
    case responseTooLarge
    case malformedState

    var errorDescription: String? {
        switch self {
        case .invalidDashboardURL:
            "Use HTTPS for remote dashboards. Plain HTTP is limited to IPv4 and IPv6 loopback addresses. Credentials, query parameters, and fragments are not accepted."
        case .invalidResponse:
            "The Stado dashboard returned an invalid response."
        case let .server(status):
            "The Stado dashboard returned HTTP \(status)."
        case .responseTooLarge:
            "The Stado state response exceeded the safe display limit."
        case .malformedState:
            "The Stado dashboard state does not match the supported interface."
        }
    }
}

actor OperationsClient {
    private let session: URLSession
    private let maximumResponseBytes = 5 * 1_024 * 1_024

    init() {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        configuration.urlCredentialStorage = nil
        configuration.timeoutIntervalForRequest = 20
        configuration.timeoutIntervalForResource = 30
        session = URLSession(configuration: configuration)
    }

    func fetchState(from address: OperationsDashboardAddress) async throws -> DashboardSnapshot {
        var request = URLRequest(url: address.stateURL)
        request.httpMethod = "GET"
        request.cachePolicy = .reloadIgnoringLocalCacheData
        request.setValue("application/json", forHTTPHeaderField: "Accept")

        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw OperationsClientError.invalidResponse
        }
        guard http.statusCode == 200 else {
            throw OperationsClientError.server(http.statusCode)
        }
        guard data.count <= maximumResponseBytes else {
            throw OperationsClientError.responseTooLarge
        }

        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        do {
            return try decoder.decode(DashboardSnapshot.self, from: data)
        } catch {
            throw OperationsClientError.malformedState
        }
    }
}
