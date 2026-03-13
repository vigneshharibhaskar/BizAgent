"use client";

import { useRef, useState } from "react";
import { Upload, FileText, CheckCircle2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { uploadDataset } from "@/lib/api";

interface DatasetUploaderProps {
  onUploaded: (datasetId: string, name: string) => void;
  onError: (message: string) => void;
}

// Stable shape stored in state — all fields guaranteed non-nullable after normalization.
interface NormalizedUpload {
  dataset_id: string;
  name: string;
  row_count: number | null;
  warnings: string[];
}

export function DatasetUploader({ onUploaded, onError }: DatasetUploaderProps) {
  const [name, setName] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [lastUpload, setLastUpload] = useState<NormalizedUpload | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!file) return;

    setLoading(true);
    try {
      const res = await uploadDataset(name.trim() || file.name, file);

      // Normalize once — downstream code never touches nested/optional raw fields.
      const normalized: NormalizedUpload = {
        dataset_id: res.dataset.id,
        name: res.dataset.name || name.trim() || file.name,
        row_count: res.events_loaded ?? null,
        warnings: res.warnings ?? [],
      };

      setLastUpload(normalized);
      onUploaded(normalized.dataset_id, normalized.name);
    } catch (err) {
      onError(err instanceof Error ? err.message : "Upload failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Upload className="h-4 w-4" />
          Upload Dataset
        </CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-3">
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Dataset name (optional)
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. Q1 2024"
              className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            />
          </div>

          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              CSV file <span className="text-destructive">*</span>
            </label>
            <div
              className="flex cursor-pointer items-center gap-2 rounded-md border border-dashed border-input bg-background px-3 py-2 text-sm text-muted-foreground hover:border-ring hover:text-foreground"
              onClick={() => fileRef.current?.click()}
            >
              <FileText className="h-4 w-4 shrink-0" />
              <span className="truncate">
                {file ? file.name : "Click to select CSV…"}
              </span>
            </div>
            <input
              ref={fileRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <Button
            type="submit"
            disabled={!file || loading}
            className="w-full"
            size="sm"
          >
            {loading ? "Uploading…" : "Upload"}
          </Button>
        </form>

        {lastUpload && (
          <div className="mt-3 flex items-start gap-2 rounded-md bg-emerald-50 px-3 py-2 text-xs text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">
            <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <div>
              <p className="font-medium">{lastUpload.name}</p>
              <p className="text-emerald-600 dark:text-emerald-400">
                {lastUpload.row_count != null
                  ? `${lastUpload.row_count.toLocaleString()} rows`
                  : "— rows"}{" "}
                •{" "}
                <span className="font-mono text-[10px]">
                  {lastUpload.dataset_id
                    ? `${lastUpload.dataset_id.slice(0, 8)}…`
                    : "—"}
                </span>
              </p>
              {lastUpload.warnings.length > 0 && (
                <p className="mt-0.5 text-amber-600 dark:text-amber-400">
                  {lastUpload.warnings.length} warning
                  {lastUpload.warnings.length !== 1 ? "s" : ""}
                </p>
              )}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
