export interface AlertRow {
  id: string;
  symbol: string;
  venue: string;
  tier: string;
  price_type: string;
  direction: string;
  target_price: string;
  state: string;
  valid_from_unix_nanos: number;
  expires_unix_nanos: number;
  created_at_unix_nanos: number;
}

export interface AlertsPage {
  total: number;
  items: AlertRow[];
}

export async function fetchAlerts(q: Record<string, string>): Promise<AlertsPage> {
  const params = new URLSearchParams(
    Object.entries(q).filter(([, v]) => v !== "")
  );
  const res = await fetch(`/api/alerts?${params.toString()}`);
  if (!res.ok) throw new Error(`alerts: HTTP ${res.status}`);
  return (await res.json()) as AlertsPage;
}
