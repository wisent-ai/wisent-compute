import Foundation

struct DashboardSnapshot: Decodable, Sendable {
    let ready: Bool
    let now: String?
    let bucket: String?
    let counts: JobCounts
    let byModelState: [String: JobCounts]
    let liveAgents: [WorkerNode]
    let staleAgents: [WorkerNode]
    let recentFailed: [FailedJob]
    let completedRecent: [CompletedJob]
    let throughput: Throughput
    let lastRefreshSeconds: Double?

    enum CodingKeys: String, CodingKey {
        case ready, now, bucket, counts, throughput
        case byModelState
        case liveAgents
        case staleAgents
        case recentFailed
        case completedRecent
        case lastRefreshSeconds
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        ready = try values.decodeIfPresent(Bool.self, forKey: .ready) ?? false
        now = try values.decodeIfPresent(String.self, forKey: .now)
        bucket = try values.decodeIfPresent(String.self, forKey: .bucket)
        counts = try values.decodeIfPresent(JobCounts.self, forKey: .counts) ?? .zero
        byModelState = try values.decodeIfPresent([String: JobCounts].self, forKey: .byModelState) ?? [:]
        liveAgents = try values.decodeIfPresent([WorkerNode].self, forKey: .liveAgents) ?? []
        staleAgents = try values.decodeIfPresent([WorkerNode].self, forKey: .staleAgents) ?? []
        recentFailed = try values.decodeIfPresent([FailedJob].self, forKey: .recentFailed) ?? []
        completedRecent = try values.decodeIfPresent([CompletedJob].self, forKey: .completedRecent) ?? []
        throughput = try values.decodeIfPresent(Throughput.self, forKey: .throughput) ?? .unavailable
        lastRefreshSeconds = try values.decodeIfPresent(Double.self, forKey: .lastRefreshSeconds)
    }
}

struct JobCounts: Decodable, Sendable {
    let queue: Int
    let running: Int
    let completed: Int
    let failed: Int

    static let zero = JobCounts(queue: 0, running: 0, completed: 0, failed: 0)

    init(queue: Int, running: Int, completed: Int, failed: Int) {
        self.queue = queue
        self.running = running
        self.completed = completed
        self.failed = failed
    }

    enum CodingKeys: String, CodingKey {
        case queue, running, completed, failed
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        queue = try values.decodeIfPresent(Int.self, forKey: .queue) ?? 0
        running = try values.decodeIfPresent(Int.self, forKey: .running) ?? 0
        completed = try values.decodeIfPresent(Int.self, forKey: .completed) ?? 0
        failed = try values.decodeIfPresent(Int.self, forKey: .failed) ?? 0
    }
}

struct WorkerNode: Decodable, Identifiable, Sendable {
    let consumerID: String?
    let kind: String?
    let freeSlots: [String: Int]
    let freeVRAMGB: Double?
    let totalVRAMGB: Double?
    let publishedAt: String?
    let ageSeconds: Double?

    var id: String {
        consumerID ?? "unknown-\(publishedAt ?? "worker")"
    }

    var displayName: String {
        guard let consumerID, !consumerID.isEmpty else { return "Unnamed worker" }
        return consumerID
    }

    var availableSlots: Int {
        freeSlots.values.reduce(0, +)
    }

    enum CodingKeys: String, CodingKey {
        case consumerID, kind, freeSlots, freeVRAMGB, totalVRAMGB, publishedAt, ageSeconds
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        consumerID = try values.decodeIfPresent(String.self, forKey: .consumerID)
        kind = try values.decodeIfPresent(String.self, forKey: .kind)
        freeSlots = try values.decodeIfPresent([String: Int].self, forKey: .freeSlots) ?? [:]
        freeVRAMGB = try values.decodeIfPresent(Double.self, forKey: .freeVRAMGB)
        totalVRAMGB = try values.decodeIfPresent(Double.self, forKey: .totalVRAMGB)
        publishedAt = try values.decodeIfPresent(String.self, forKey: .publishedAt)
        ageSeconds = try values.decodeIfPresent(Double.self, forKey: .ageSeconds)
    }
}

struct CompletedJob: Decodable, Identifiable, Sendable {
    let jobID: String
    let model: String?
    let task: String?
    let wallSeconds: Double?
    let completedAt: String?

    var id: String { jobID }

    enum CodingKeys: String, CodingKey {
        case jobID, model, task, wallSeconds, completedAt
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        jobID = try values.decodeIfPresent(String.self, forKey: .jobID) ?? "Unavailable"
        model = try values.decodeIfPresent(String.self, forKey: .model)
        task = try values.decodeIfPresent(String.self, forKey: .task)
        wallSeconds = try values.decodeIfPresent(Double.self, forKey: .wallSeconds)
        completedAt = try values.decodeIfPresent(String.self, forKey: .completedAt)
    }
}

struct FailedJob: Decodable, Identifiable, Sendable {
    let jobID: String
    let model: String?
    let task: String?
    let error: String?

    var id: String { jobID }

    enum CodingKeys: String, CodingKey {
        case jobID, model, task, error
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        jobID = try values.decodeIfPresent(String.self, forKey: .jobID) ?? "Unavailable"
        model = try values.decodeIfPresent(String.self, forKey: .model)
        task = try values.decodeIfPresent(String.self, forKey: .task)
        error = try values.decodeIfPresent(String.self, forKey: .error)
    }
}

struct Throughput: Decodable, Sendable {
    let averageWallSecondsPerCompletedJob: Double?
    let samples: Int
    let liveTotalFreeSlots: Int
    let projectedRemainingSeconds: Double?

    static let unavailable = Throughput(
        averageWallSecondsPerCompletedJob: nil,
        samples: 0,
        liveTotalFreeSlots: 0,
        projectedRemainingSeconds: nil
    )

    enum CodingKeys: String, CodingKey {
        case averageWallSecondsPerCompletedJob = "avgWallSecondsPerCompletedJob"
        case samples, liveTotalFreeSlots, projectedRemainingSeconds
    }

    init(
        averageWallSecondsPerCompletedJob: Double?,
        samples: Int,
        liveTotalFreeSlots: Int,
        projectedRemainingSeconds: Double?
    ) {
        self.averageWallSecondsPerCompletedJob = averageWallSecondsPerCompletedJob
        self.samples = samples
        self.liveTotalFreeSlots = liveTotalFreeSlots
        self.projectedRemainingSeconds = projectedRemainingSeconds
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        averageWallSecondsPerCompletedJob = try values.decodeIfPresent(Double.self, forKey: .averageWallSecondsPerCompletedJob)
        samples = try values.decodeIfPresent(Int.self, forKey: .samples) ?? 0
        liveTotalFreeSlots = try values.decodeIfPresent(Int.self, forKey: .liveTotalFreeSlots) ?? 0
        projectedRemainingSeconds = try values.decodeIfPresent(Double.self, forKey: .projectedRemainingSeconds)
    }
}

enum StadoFormat {
    private static let fractionalDateStrategy = Date.ISO8601FormatStyle(includingFractionalSeconds: true)
    private static let dateStrategy = Date.ISO8601FormatStyle()

    static func date(_ value: String?) -> Date? {
        guard let value else { return nil }
        return (try? fractionalDateStrategy.parse(value)) ?? (try? dateStrategy.parse(value))
    }

    static func duration(_ seconds: Double?) -> String {
        guard let seconds, seconds.isFinite, seconds >= 0 else { return "Unavailable" }
        if seconds < 60 {
            return "\(Int(seconds.rounded())) sec"
        }
        if seconds < 3_600 {
            return "\(Int((seconds / 60).rounded())) min"
        }
        return "\((seconds / 3_600).formatted(.number.precision(.fractionLength(0...1)))) hr"
    }

    static func decimal(_ value: Double?) -> String {
        guard let value, value.isFinite else { return "Unavailable" }
        return value.formatted(.number.precision(.fractionLength(0...1)))
    }
}
