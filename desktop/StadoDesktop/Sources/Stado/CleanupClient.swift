import Foundation
import Network

private enum DashboardTransport: String {
    case http
    case https
}

struct DashboardAddress: Equatable, Sendable {
    static let defaultString = "http://127.0.0.1:8765"
    private static func isLoopback(host: String) -> Bool {
        if let address = IPv4Address(host) {
            return address.rawValue.first == 127
        }
        guard let address = IPv6Address(host) else { return false }
        let bytes = address.rawValue
        return bytes.count == 16 && bytes.dropLast().allSatisfy { $0 == 0 } && bytes.last == 1
    }

    let baseURL: URL

    init(_ value: String) throws {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard var components = URLComponents(string: trimmed),
              let schemeName = components.scheme?.lowercased(),
              let transport = DashboardTransport(rawValue: schemeName),
              let host = components.url?.host(percentEncoded: false),
              !host.isEmpty,
              components.user == nil,
              components.password == nil,
              components.query == nil,
              components.fragment == nil,
              (transport == .https || Self.isLoopback(host: host))
        else {
            throw CleanupClientError.invalidDashboardURL
        }

        components.scheme = transport.rawValue
        if components.path == "/" {
            components.path = ""
        }
        while components.path.count > 1 && components.path.hasSuffix("/") {
            components.path.removeLast()
        }
        guard let url = components.url else {
            throw CleanupClientError.invalidDashboardURL
        }
        baseURL = url
    }

    var displayString: String { baseURL.absoluteString }

    var dashboardURL: URL { baseURL.appending(path: "") }

    func endpoint(_ path: String) -> URL {
        path.split(separator: "/").reduce(baseURL) { url, component in
            url.appending(path: String(component))
        }
    }
}

enum CleanupClientError: LocalizedError, Sendable {
    case invalidDashboardURL
    case invalidResponse
    case server(status: Int, message: String)
    case malformedReport

    var errorDescription: String? {
        switch self {
        case .invalidDashboardURL:
            "Use HTTPS for remote dashboards. Plain HTTP is limited to IPv4 and IPv6 loopback addresses; credentials, query parameters, and fragments are not allowed."
        case .invalidResponse:
            "The dashboard returned an invalid HTTP response."
        case let .server(status, message):
            message.isEmpty ? "The dashboard returned HTTP \(status)." : message
        case .malformedReport:
            "The dashboard returned a cleanup report that does not match the expected contract."
        }
    }
}

struct CleanupClient: Sendable {
    private let session: URLSession

    init(session: URLSession? = nil) {
        if let session {
            self.session = session
            return
        }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        configuration.urlCredentialStorage = nil
        configuration.timeoutIntervalForRequest = 45
        configuration.timeoutIntervalForResource = 75
        self.session = URLSession(configuration: configuration)
    }

    func currentReport(at address: DashboardAddress) async throws -> CleanupResponse {
        try await perform(currentReportRequest(at: address))
    }

    func runCleanup(at address: DashboardAddress) async throws -> CleanupResponse {
        try await perform(runCleanupRequest(at: address))
    }

    func currentReportRequest(at address: DashboardAddress) -> URLRequest {
        var request = URLRequest(url: address.endpoint("api/cleanup.json"))
        request.httpMethod = "GET"
        request.cachePolicy = .reloadIgnoringLocalCacheData
        return request
    }

    func runCleanupRequest(at address: DashboardAddress) -> URLRequest {
        var request = URLRequest(url: address.endpoint("api/cleanup/run"))
        request.httpMethod = "POST"
        request.setValue("cleanup", forHTTPHeaderField: "X-Stado-Action")
        request.httpBody = nil
        request.setValue("0", forHTTPHeaderField: "Content-Length")
        return request
    }

    private func perform(_ request: URLRequest) async throws -> CleanupResponse {
        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw CleanupClientError.invalidResponse
        }

        guard http.statusCode == 200 || http.statusCode == 403 || http.statusCode == 409 || http.statusCode == 500 else {
            throw CleanupClientError.server(status: http.statusCode, message: "")
        }
        do {
            return try JSONDecoder().decode(CleanupResponse.self, from: data)
        } catch {
            throw CleanupClientError.malformedReport
        }
    }
}
