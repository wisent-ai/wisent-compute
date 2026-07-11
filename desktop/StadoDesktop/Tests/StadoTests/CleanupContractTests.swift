import Foundation
import XCTest
@testable import Stado

final class CleanupContractTests: XCTestCase {
    func testDecodesCompleteCleanupEnvelopeWithoutLosingRegistryReportFields() throws {
        let response = try decodeResponse(
            """
            {
              "ok": true,
              "service": "cleanup",
              "report": {
                "version": 3,
                "mode": "enforce",
                "check_interval_seconds": 900,
                "started_at": "2026-07-11T14:15:16.250Z",
                "duration_ms": 1842,
                "outcome": "partial_error",
                "free_bytes_before": 1200000000,
                "free_bytes_after": 3400000000,
                "low_bytes": 2000000000,
                "target_bytes": 4000000000,
                "pressure_active": true,
                "cleaners": {
                  "huggingface_cache": {
                    "scanned_items": 11,
                    "eligible_items": 7,
                    "deleted_items": 4,
                    "expected_bytes": 2100000000,
                    "actual_free_delta_bytes": 2050000000,
                    "skipped": {
                      "active_slot": 2,
                      "too_recent": 1
                    }
                  },
                  "weles_recordings": {
                    "scanned_items": 5,
                    "eligible_items": 3,
                    "deleted_items": 2,
                    "expected_bytes": 400000000,
                    "actual_free_delta_bytes": 150000000,
                    "skipped": {
                      "retention_window": 2
                    }
                  }
                },
                "caps": {
                  "bytes": true,
                  "items": false,
                  "scan": true,
                  "deadline": false
                },
                "lock_busy": false,
                "active_slot_count": 2,
                "last_success_at": "2026-07-10T09:08:07Z",
                "errors": [
                  "huggingface_cache: cleanup failed",
                  "weles_recordings: scan unavailable"
                ]
              }
            }
            """
        )

        XCTAssertTrue(response.ok)
        XCTAssertEqual(response.service, "cleanup")

        let report = response.report
        XCTAssertEqual(report.version, 3)
        XCTAssertEqual(report.mode, "enforce")
        XCTAssertEqual(report.checkIntervalSeconds, 900)
        XCTAssertEqual(report.startedAt, "2026-07-11T14:15:16.250Z")
        XCTAssertEqual(report.durationMs, 1842)
        XCTAssertEqual(report.outcome, "partial_error")
        XCTAssertEqual(report.freeBytesBefore, 1_200_000_000)
        XCTAssertEqual(report.freeBytesAfter, 3_400_000_000)
        XCTAssertEqual(report.lowBytes, 2_000_000_000)
        XCTAssertEqual(report.targetBytes, 4_000_000_000)
        XCTAssertEqual(report.pressureActive, true)
        XCTAssertFalse(report.lockBusy)
        XCTAssertEqual(report.activeSlotCount, 2)
        XCTAssertEqual(report.lastSuccessAt, "2026-07-10T09:08:07Z")
        XCTAssertEqual(report.errors, [
            "huggingface_cache: cleanup failed",
            "weles_recordings: scan unavailable",
        ])
        XCTAssertEqual(report.reclaimedBytes, 2_200_000_000)

        let huggingFace = report.cleaners.huggingFaceCache
        XCTAssertEqual(huggingFace.scannedItems, 11)
        XCTAssertEqual(huggingFace.eligibleItems, 7)
        XCTAssertEqual(huggingFace.deletedItems, 4)
        XCTAssertEqual(huggingFace.expectedBytes, 2_100_000_000)
        XCTAssertEqual(huggingFace.actualFreeDeltaBytes, 2_050_000_000)
        XCTAssertEqual(huggingFace.skipped, ["active_slot": 2, "too_recent": 1])

        let recordings = report.cleaners.welesRecordings
        XCTAssertEqual(recordings.scannedItems, 5)
        XCTAssertEqual(recordings.eligibleItems, 3)
        XCTAssertEqual(recordings.deletedItems, 2)
        XCTAssertEqual(recordings.expectedBytes, 400_000_000)
        XCTAssertEqual(recordings.actualFreeDeltaBytes, 150_000_000)
        XCTAssertEqual(recordings.skipped, ["retention_window": 2])
        XCTAssertEqual(report.caps.activeLabels, ["byte limit", "scan limit"])
    }

    func testDecodesAbsentOptionalReportValuesAndEmptyCollections() throws {
        let response = try decodeResponse(minimalEnvelope(service: "cleanup", errors: "[]"))
        let report = response.report

        XCTAssertNil(report.mode)
        XCTAssertNil(report.checkIntervalSeconds)
        XCTAssertNil(report.startedAt)
        XCTAssertNil(report.freeBytesBefore)
        XCTAssertNil(report.freeBytesAfter)
        XCTAssertNil(report.lowBytes)
        XCTAssertNil(report.targetBytes)
        XCTAssertNil(report.pressureActive)
        XCTAssertNil(report.lastSuccessAt)
        XCTAssertEqual(report.cleaners.huggingFaceCache.skipped, [:])
        XCTAssertEqual(report.cleaners.welesRecordings.skipped, [:])
        XCTAssertEqual(report.caps.activeLabels, [])
        XCTAssertEqual(report.errors, [])
        XCTAssertEqual(report.reclaimedBytes, 0)
    }

    func testDecodesSanitizedServiceErrorEnvelopeAsReportData() throws {
        let response = try decodeResponse(
            minimalEnvelope(
                ok: false,
                service: "error",
                outcome: "runtime_error",
                errors: "[\"cleanup service unavailable\"]"
            )
        )

        XCTAssertFalse(response.ok)
        XCTAssertEqual(response.service, "error")
        XCTAssertEqual(response.report.outcome, "runtime_error")
        XCTAssertEqual(response.report.errors, ["cleanup service unavailable"])
    }

    func testRejectsEnvelopeWhenRequiredNestedReportFieldIsMissing() {
        let malformed = minimalEnvelope().replacingOccurrences(of: "\"duration_ms\": 0,", with: "")

        XCTAssertThrowsError(try decodeResponse(malformed)) { error in
            guard case DecodingError.keyNotFound(let key, _) = error else {
                return XCTFail("Expected keyNotFound, got \(error)")
            }
            XCTAssertEqual(key.stringValue, "duration_ms")
        }
    }

    func testByteAndDurationFormattingAtUnitBoundaries() async {
        let formatted = await MainActor.run {
            (
                DisplayFormat.bytes(nil),
                DisplayFormat.bytes(1_000),
                DisplayFormat.bytes(1_000_000),
                DisplayFormat.duration(milliseconds: 999),
                DisplayFormat.duration(milliseconds: 1_000),
                DisplayFormat.duration(milliseconds: 1_250)
            )
        }

        XCTAssertEqual(formatted.0, "Unavailable")
        XCTAssertEqual(formatted.1, "1 KB")
        XCTAssertEqual(formatted.2, "1 MB")
        XCTAssertEqual(formatted.3, "999 ms")
        XCTAssertEqual(formatted.4, "1.0 s")
        XCTAssertEqual(formatted.5, "1.2 s")
    }

    func testParsesTimestampsWithAndWithoutFractionalSecondsAndRejectsInvalidTime() async {
        let dates = await MainActor.run {
            (
                DisplayFormat.date("2026-07-11T14:15:16.250Z"),
                DisplayFormat.date("2026-07-11T14:15:16Z"),
                DisplayFormat.date("2026-07-11T14:15:16.250123+00:00"),
                DisplayFormat.date("not-a-timestamp"),
                DisplayFormat.date(nil)
            )
        }

        guard let fractionalDate = dates.0,
              let wholeSecondDate = dates.1,
              let pythonISOFormatDate = dates.2
        else {
            return XCTFail("Expected valid ISO-8601 timestamps to parse")
        }
        XCTAssertEqual(fractionalDate.timeIntervalSince1970, 1_783_779_316.25, accuracy: 0.001)
        XCTAssertEqual(wholeSecondDate.timeIntervalSince1970, 1_783_779_316, accuracy: 0.001)
        XCTAssertEqual(pythonISOFormatDate.timeIntervalSince1970, 1_783_779_316.250123, accuracy: 0.000_001)
        XCTAssertNil(dates.3)
        XCTAssertNil(dates.4)
    }

    func testDashboardAddressNormalizesSafeHTTPURLsAndBuildsCleanupEndpoint() throws {
        let cases = [
            ("  HTTPS://example.com/base///\n", "https://example.com/base", "https://example.com/base/api/cleanup.json"),
            ("http://127.0.0.1:8765/", "http://127.0.0.1:8765", "http://127.0.0.1:8765/api/cleanup.json"),
        ]

        for (input, expectedDisplay, expectedEndpoint) in cases {
            let address = try DashboardAddress(input)
            XCTAssertEqual(address.displayString, expectedDisplay, "input: \(input)")
            XCTAssertEqual(address.endpoint("api/cleanup.json").absoluteString, expectedEndpoint, "input: \(input)")
        }
    }

    func testDashboardAddressRejectsUnsafeOrAmbiguousURLs() {
        let invalidAddresses = [
            "",
            "example.com",
            "ftp://example.com",
            "http://",
            "https://user@example.com",
            "https://example.com?token=secret",
            "https://example.com/#section",
        ]

        for value in invalidAddresses {
            XCTAssertThrowsError(try DashboardAddress(value), "input should be rejected: \(value)") { error in
                guard case CleanupClientError.invalidDashboardURL = error else {
                    return XCTFail("Expected invalidDashboardURL for \(value), got \(error)")
                }
            }
        }
    }

    func testDashboardAddressAllowsOnlyIPLoopbackOverPlainHTTPAndAnyHostOverHTTPS() {
        let acceptedAddresses = [
            "http://127.23.45.67:8765",
            "http://[::1]:8765",
            "https://dashboard.example.com",
        ]

        for value in acceptedAddresses {
            do {
                let address = try DashboardAddress(value)
                XCTAssertEqual(address.displayString, value, "input should be accepted: \(value)")
            } catch {
                XCTFail("input should be accepted: \(value), got \(error)")
            }
        }
    }

    func testDashboardAddressRejectsNamedLocalRemoteAndTailnetHostsOverPlainHTTP() {
        let rejectedAddresses = [
            "http://localhost:8765",
            "http://stado-dev:8765",
            "http://stado-dev.local:8765",
            "http://dashboard.example.com",
            "http://203.0.113.42",
            "http://100.96.12.34",
        ]

        for value in rejectedAddresses {
            XCTAssertThrowsError(try DashboardAddress(value), "input should be rejected: \(value)") { error in
                guard case CleanupClientError.invalidDashboardURL = error else {
                    return XCTFail("Expected invalidDashboardURL for \(value), got \(error)")
                }
            }
        }
    }

    func testCurrentReportRequestIsAnExactBodylessQuerylessGET() throws {
        let client = CleanupClient()
        let address = try DashboardAddress("https://dashboard.example/base")

        let request = client.currentReportRequest(at: address)

        XCTAssertEqual(request.url?.absoluteString, "https://dashboard.example/base/api/cleanup.json")
        XCTAssertEqual(request.httpMethod, "GET")
        XCTAssertNil(request.url?.query)
        XCTAssertNil(request.httpBody)
        XCTAssertNil(request.value(forHTTPHeaderField: "X-Stado-Action"))
    }

    func testRunCleanupRequestIsAnExactAuthorizedBodylessQuerylessPOST() throws {
        let client = CleanupClient()
        let address = try DashboardAddress("https://dashboard.example/base")

        let request = client.runCleanupRequest(at: address)

        XCTAssertEqual(request.url?.absoluteString, "https://dashboard.example/base/api/cleanup/run")
        XCTAssertEqual(request.httpMethod, "POST")
        XCTAssertNil(request.url?.query)
        XCTAssertNil(request.httpBody)
        XCTAssertEqual(request.value(forHTTPHeaderField: "X-Stado-Action"), "cleanup")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Length"), "0")
    }

    private func decodeResponse(_ json: String) throws -> CleanupResponse {
        try JSONDecoder().decode(CleanupResponse.self, from: Data(json.utf8))
    }

    private func minimalEnvelope(
        ok: Bool = true,
        service: String = "cleanup",
        outcome: String = "never_run",
        errors: String = "[]"
    ) -> String {
        """
        {
          "ok": \(ok),
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
            "errors": \(errors)
          }
        }
        """
    }
}
