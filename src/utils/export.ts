import type { KafkaMessage } from "../types";

function escapeCSV(value: string): string {
  if (/[",\n]/.test(value)) {
    return '"' + value.replace(/"/g, '""') + '"';
  }
  return value;
}

function messageToExportValue(msg: KafkaMessage): string {
  if (msg.value_text !== null) return msg.value_text;
  const bytes = new Uint8Array(msg.value_raw);
  let binary = "";
  bytes.forEach((b) => (binary += String.fromCharCode(b)));
  return btoa(binary);
}

export function exportMessages(messages: KafkaMessage[], topic: string): void {
  if (messages.length === 0) return;
  const csvHeaders = ["partition", "offset", "timestamp", "key", "value", "headers"];
  const rows = messages.map((m) =>
    [
      String(m.partition),
      String(m.offset),
      m.timestamp != null ? String(m.timestamp) : "",
      m.key_text ?? "",
      messageToExportValue(m),
      m.headers.length > 0 ? JSON.stringify(m.headers) : "",
    ]
      .map(escapeCSV)
      .join(",")
  );
  const content = [csvHeaders.join(","), ...rows].join("\n");
  const isoDate = new globalThis.Date().toISOString().slice(0, 19).replace(/:/g, "-");
  const filename = topic + "_" + isoDate + ".csv";
  const blob = new Blob([content], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
