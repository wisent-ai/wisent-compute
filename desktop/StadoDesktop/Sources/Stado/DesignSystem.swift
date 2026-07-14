import AppKit
import SwiftUI

enum StadoTheme {
    enum Space {
        static let xxs: CGFloat = 4
        static let xs: CGFloat = 8
        static let sm: CGFloat = 12
        static let md: CGFloat = 16
        static let lg: CGFloat = 24
        static let xl: CGFloat = 32
    }

    enum Radius {
        static let small: CGFloat = 6
        static let medium: CGFloat = 10
        static let large: CGFloat = 14
    }

    enum Layout {
        static let sidebarMinimum: CGFloat = 210
        static let sidebarIdeal: CGFloat = 230
        static let sidebarMaximum: CGFloat = 280
        static let windowMinimumWidth: CGFloat = 900
        static let windowMinimumHeight: CGFloat = 620
        static let contentMaximumWidth: CGFloat = 1_080
        static let metricMinimumWidth: CGFloat = 170
        static let progressWidth: CGFloat = 116
        static let emptyStateMinimumHeight: CGFloat = 220
        static let statusDot: CGFloat = 8
    }

    enum Motion {
        static let standard = Animation.easeOut(duration: 0.18)
    }

    static let panel = Color(nsColor: .controlBackgroundColor)
    static let raisedPanel = Color(nsColor: .windowBackgroundColor)
    static let separator = Color(nsColor: .separatorColor)
}

enum StatusTone {
    case healthy
    case neutral
    case warning
    case critical

    var color: Color {
        switch self {
        case .healthy: .green
        case .neutral: .secondary
        case .warning: .orange
        case .critical: .red
        }
    }
}

struct StadoCard<Content: View>: View {
    private let content: Content

    init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    var body: some View {
        content
            .padding(StadoTheme.Space.md)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(StadoTheme.panel, in: RoundedRectangle(cornerRadius: StadoTheme.Radius.medium))
            .overlay {
                RoundedRectangle(cornerRadius: StadoTheme.Radius.medium)
                    .stroke(StadoTheme.separator, lineWidth: 1)
            }
    }
}

struct MetricCard: View {
    let title: String
    let value: String
    let detail: String
    let symbol: String
    let tone: StatusTone

    var body: some View {
        StadoCard {
            HStack(alignment: .top, spacing: StadoTheme.Space.sm) {
                Image(systemName: symbol)
                    .font(.title3)
                    .foregroundStyle(tone.color)
                    .accessibilityHidden(true)
                VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                    Text(title)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(value)
                        .font(.title2.weight(.semibold))
                        .monospacedDigit()
                    Text(detail)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(2)
                }
            }
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(title), \(value), \(detail)")
    }
}

struct StatusPill: View {
    let label: String
    let tone: StatusTone

    var body: some View {
        HStack(spacing: StadoTheme.Space.xxs) {
            Circle()
                .fill(tone.color)
                .frame(width: StadoTheme.Layout.statusDot, height: StadoTheme.Layout.statusDot)
                .accessibilityHidden(true)
            Text(label)
                .font(.caption.weight(.medium))
        }
        .padding(.horizontal, StadoTheme.Space.xs)
        .padding(.vertical, StadoTheme.Space.xxs)
        .background(tone.color.opacity(0.12), in: Capsule())
        .accessibilityLabel(label)
    }
}

struct UnavailableNotice: View {
    let title: String
    let detail: String
    var symbol = "questionmark.circle"

    var body: some View {
        HStack(alignment: .top, spacing: StadoTheme.Space.sm) {
            Image(systemName: symbol)
                .foregroundStyle(.secondary)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: StadoTheme.Space.xxs) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: 0)
        }
        .padding(StadoTheme.Space.sm)
        .background(.quaternary.opacity(0.45), in: RoundedRectangle(cornerRadius: StadoTheme.Radius.small))
        .accessibilityElement(children: .combine)
    }
}
