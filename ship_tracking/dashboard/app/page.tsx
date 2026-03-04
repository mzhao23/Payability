import { supabase } from '@/lib/supabase'

// ── 类型定义 ──
type DailyReport = {
  id: string
  report_date: string
  overall_risk_level: string
  executive_summary: string
  untracked_rate: number | null
  untracked_rate_delta: number | null
  avg_order_value: number | null
  avg_order_value_delta: number | null
  avg_init_to_pickup_hours: number | null
  avg_pickup_to_delivery_hours: number | null
  overdue_unpickup_count: number | null
}

type SupplierRisk = {
  id: string
  supplier_key: string
  risk_score: number
  risk_level: string
  untracked_rate: number | null
  avg_order_value: number | null
  avg_init_to_pickup_hours: number | null
  overdue_unpickup_count: number | null
  issues: string[] | string
}

// ── 数据获取 ──
async function getLatestReport(): Promise<DailyReport | null> {
  const { data } = await supabase
    .from('daily_reports')
    .select('*')
    .order('report_date', { ascending: false })
    .limit(1)
    .single()
  return data
}

async function getSupplierRisks(reportDate: string): Promise<SupplierRisk[]> {
  const { data } = await supabase
    .from('supplier_daily_risks')
    .select('*')
    .eq('report_date', reportDate)
    .order('risk_score', { ascending: false })
  return data || []
}

// ── 样式配置 ──
const RISK_CONFIG = {
  HIGH:   { text: 'text-red-700',    badge: 'bg-red-100 text-red-700 ring-red-200',    bar: 'bg-red-500',    border: 'border-red-100' },
  MEDIUM: { text: 'text-amber-700',  badge: 'bg-amber-100 text-amber-700 ring-amber-200', bar: 'bg-amber-500', border: 'border-amber-100' },
  LOW:    { text: 'text-emerald-700',badge: 'bg-emerald-100 text-emerald-700 ring-emerald-200', bar: 'bg-emerald-500', border: 'border-emerald-100' },
}

function fmt(v: number | null, unit = '', decimals = 1) {
  if (v === null || v === undefined) return '—'
  return `${unit}${v.toLocaleString(undefined, { maximumFractionDigits: decimals })}`
}

export default async function Dashboard() {
  const report = await getLatestReport()
  const suppliers = report ? await getSupplierRisks(report.report_date) : []

  if (!report) return <div className="p-20 text-center text-slate-400">Loading report...</div>

  const rc = RISK_CONFIG[report.overall_risk_level as keyof typeof RISK_CONFIG] || RISK_CONFIG.LOW

  return (
    <main className="min-h-screen pb-20 pt-10 px-6 bg-[#f8fafc]">
      <div className="max-w-7xl mx-auto space-y-10"> {/* 稍微调宽了最大宽度以容纳完整表格 */}

        {/* ── Top Bar ── */}
        <div className="flex items-end justify-between border-b border-slate-200 pb-8">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="h-2 w-8 bg-indigo-600 rounded-full" />
              <p className="text-[11px] text-slate-400 font-black uppercase tracking-[0.3em]">Payability Risk Intelligence</p>
            </div>
            <h1 className="text-4xl font-black text-slate-900 tracking-tight">Daily Risk Report</h1>
          </div>
          <div className="text-right">
            <p className="mono text-[10px] font-bold text-slate-400 uppercase mb-1">Generated At</p>
            <p className="mono text-sm font-bold text-slate-600 bg-slate-100 px-3 py-1 rounded-lg">{report.report_date}</p>
          </div>
        </div>

        {/* ── Hero Card ── */}
        <div className={`relative overflow-hidden glass-card p-10 flex items-center justify-between border-l-[6px] ${rc.border.replace('border-', 'border-l-')}`}>
          <div className="flex-1 pr-16 relative z-10">
            <div className="flex items-center gap-5 mb-5">
              <span className={`text-5xl font-black tracking-tighter ${rc.text}`}>{report.overall_risk_level}</span>
              <div className="h-8 w-[1px] bg-slate-200" />
              <span className="text-xs font-bold text-slate-400 uppercase tracking-widest">System Health Status</span>
            </div>
            <p className="text-lg text-slate-600 leading-relaxed max-w-2xl font-medium italic">"{report.executive_summary}"</p>
          </div>
          <div className="bg-slate-50 px-8 py-5 rounded-2xl border border-slate-100 text-center shadow-inner">
            <p className="text-4xl font-black text-slate-800">{suppliers.length}</p>
            <p className="text-[10px] font-bold text-slate-400 uppercase tracking-widest mt-1">Flagged Suppliers</p>
          </div>
        </div>

        {/* ── Table Section ── */}
        <div className="glass-card overflow-hidden border-none shadow-xl shadow-slate-200/40">
          <div className="px-8 py-6 bg-slate-900 flex items-center justify-between">
            <h2 className="font-bold text-white tracking-tight">Full Supplier Risk Breakdown</h2>
            <span className="mono text-[10px] font-bold text-slate-400 bg-white/10 px-3 py-1 rounded-full uppercase">Real-time Feed</span>
          </div>

          <div className="overflow-x-auto custom-scrollbar">
            <table className="w-full text-sm text-left border-collapse">
              <thead>
                <tr className="bg-slate-50 border-b border-slate-100 whitespace-nowrap">
                  {[ 
                    'Supplier Key', 'Risk Score', 'Level', 'Untracked', 'Avg Order', 'Init → Pickup', 'Overdue', 'Issues' 
                  ].map(h => (
                    <th key={h} className="text-[10px] font-black text-slate-400 uppercase tracking-[0.15em] px-6 py-5">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-50">
                {suppliers.map((s) => {
                  const sc = RISK_CONFIG[s.risk_level as keyof typeof RISK_CONFIG] || RISK_CONFIG.LOW
                  const issues = Array.isArray(s.issues) ? s.issues : JSON.parse(typeof s.issues === 'string' ? s.issues : '[]')
                  
                  return (
                    <tr key={s.id} className="hover:bg-slate-50 transition-colors group">
                      {/* Supplier Key */}
                      <td className="px-6 py-5">
                        <span className="mono text-[11px] font-bold text-slate-400 group-hover:text-indigo-600">
                          {s.supplier_key.length > 12 ? `...${s.supplier_key.slice(-8)}` : s.supplier_key}
                        </span>
                      </td>

                      {/* Risk Score */}
                      <td className="px-6 py-5">
                        <div className="flex items-center gap-3">
                          <div className="w-16 h-1.5 bg-slate-100 rounded-full overflow-hidden">
                            <div className={`h-full ${sc.bar} rounded-full`} style={{ width: `${s.risk_score * 10}%` }} />
                          </div>
                          <span className="mono text-[11px] font-bold text-slate-700">{s.risk_score}/10</span>
                        </div>
                      </td>

                      {/* Level Badge */}
                      <td className="px-6 py-5">
                        <span className={`text-[10px] font-black px-2.5 py-1 rounded-full ring-1 uppercase tracking-tighter ${sc.badge}`}>
                          {s.risk_level}
                        </span>
                      </td>

                      {/* Untracked Rate */}
                      <td className="px-6 py-5 mono text-[11px] font-bold text-slate-600">
                        {s.untracked_rate !== null ? `${(s.untracked_rate * 100).toFixed(1)}%` : '—'}
                      </td>

                      {/* Avg Order Value */}
                      <td className="px-6 py-5 mono text-[11px] font-bold text-slate-600">
                        {fmt(s.avg_order_value, '$', 0)}
                      </td>

                      {/* Init → Pickup */}
                      <td className="px-6 py-5">
                        <span className={`mono text-[11px] font-bold ${s.avg_init_to_pickup_hours && s.avg_init_to_pickup_hours > 48 ? 'text-red-500' : 'text-slate-500'}`}>
                          {fmt(s.avg_init_to_pickup_hours, '', 1)}h
                        </span>
                      </td>

                      {/* Overdue Orders */}
                      <td className="px-6 py-5">
                        <span className={`mono text-[11px] font-black ${s.overdue_unpickup_count && s.overdue_unpickup_count > 0 ? 'text-red-600' : 'text-slate-300'}`}>
                          {s.overdue_unpickup_count || 0}
                        </span>
                      </td>

                      {/* Issues Tags */}
                      <td className="px-6 py-5">
                        <div className="flex flex-wrap gap-1.5">
                          {issues.map((i: string) => (
                            <span key={i} className="text-[9px] font-bold px-1.5 py-0.5 rounded bg-slate-100 text-slate-500 border border-slate-200 uppercase tracking-tighter">
                              {i.replace(/_/g, ' ')}
                            </span>
                          ))}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </main>
  )
}