import SwiftUI

struct SettingsView: View {
    @ObservedObject var store: CleanupStore
    @Environment(\.dismiss) private var dismiss
    @State private var draftURL = ""
    @State private var validationMessage: String?

    var body: some View {
        Form {
            Section("Dashboard") {
                TextField("Base URL", text: $draftURL, prompt: Text(DashboardAddress.defaultString))
                    .textFieldStyle(.roundedBorder)
                    .onSubmit(save)

                Text("Stado uses plain HTTP only for loopback IP addresses. Tailnet and other remote dashboard URLs must use HTTPS. Credentials are never requested or stored.")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                if let validationMessage {
                    Label(validationMessage, systemImage: "exclamationmark.triangle.fill")
                        .font(.caption)
                        .foregroundStyle(.red)
                }
            }

            HStack {
                Button("Use Local Default") {
                    draftURL = DashboardAddress.defaultString
                    validationMessage = nil
                }
                Spacer()
                Button("Cancel") {
                    dismiss()
                }
                Button("Save") {
                    save()
                }
                .buttonStyle(.borderedProminent)
            }
        }
        .formStyle(.grouped)
        .frame(width: 480)
        .fixedSize(horizontal: false, vertical: true)
        .onAppear {
            draftURL = store.dashboardURLString
        }
    }

    private func save() {
        do {
            try store.saveDashboardURL(draftURL)
            validationMessage = nil
            dismiss()
        } catch {
            validationMessage = (error as? LocalizedError)?.errorDescription
                ?? "Enter a valid dashboard URL."
        }
    }
}
