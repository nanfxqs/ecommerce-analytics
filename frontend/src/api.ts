export type Snapshot = { batch_id: string; revision: string; timezone: 'UTC'; complete: boolean; pinned?: boolean; event_bounds: { start: string | null; end: string | null }; updated_at: string };
export type HistoryResponse<T> = { snapshot: Snapshot | null; selected_range: { start: string; end: string } | null; data: T | null };
export type Overview = { counts: { view: number; addtocart: number; transaction: number }; visitors: { visitors: number }; trend: { day: string; view: number; addtocart: number; transaction: number; visitors: number }[] };
export async function historyOverview(range: [string, string] | null, signal: AbortSignal): Promise<HistoryResponse<Overview>> {
  const params = range ? '?' + new URLSearchParams({ start: range[0], end: range[1] }) : '';
  const response = await fetch('/api/history/overview' + params, { signal });
  if (!response.ok) {
    const body = await response.json();
    throw new Error(typeof body.detail === 'string' ? body.detail : '查询失败，请检查 UTC 日期范围');
  }
  return response.json();
}
