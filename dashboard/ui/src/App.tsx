import { useState, useEffect } from "react"
import axios from "axios"
import {
    BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
    PieChart, Pie, Cell, Legend
} from "recharts"

const API = "http://localhost:8001"

const COLORS = [
    "#E8C547", "#4ECDC4", "#FF6B6B", "#95E1D3",
    "#F38181", "#A8E6CF", "#FFD93D", "#6BCB77"
]

const CATEGORY_ICONS: Record<string, string> = {
    Housing: "🏠", Groceries: "🛒", Bills: "⚡", Transport: "🚇",
    "Eating Out": "🍽️", Entertainment: "🎬", Shopping: "🛍️",
    Health: "💊", Income: "💰", Other: "📦"
}

const fmt = (n: number) =>
    new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(n)

interface Overview {
    total_spent: number
    total_transactions: number
    top_category: string
    recurring_total: number
    banks_connected: number
}

interface Category {
    category: string
    total_spent: number
    transaction_count: number
    percentage: number
}

interface MonthlyTrend {
    month_label: string
    total_spent: number
    year: number
    month: number
}

interface Recurring {
    description: string
    avg_amount: number
    frequency: string
    last_seen: string
    times_seen: number
    bank_name: string
}

interface Transaction {
    transaction_date: string
    description: string
    amount: number
    category: string
    bank_name: string
    is_debit: boolean
    is_recurring: boolean
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string }) {
    return (
        <div style={{
            background: "rgba(255,255,255,0.03)",
            border: "1px solid rgba(255,255,255,0.08)",
            borderRadius: "2px",
            padding: "28px 32px",
            display: "flex",
            flexDirection: "column",
            gap: "8px",
        }}>
      <span style={{ fontSize: "11px", letterSpacing: "0.12em", color: "#666", textTransform: "uppercase" }}>
        {label}
      </span>
            <span style={{ fontSize: "32px", fontFamily: "'DM Mono', monospace", color: "#E8C547", fontWeight: 500 }}>
        {value}
      </span>
            {sub && <span style={{ fontSize: "12px", color: "#555" }}>{sub}</span>}
        </div>
    )
}

export default function App() {
    const [overview, setOverview] = useState<Overview | null>(null)
    const [categories, setCategories] = useState<Category[]>([])
    const [trends, setTrends] = useState<MonthlyTrend[]>([])
    const [recurring, setRecurring] = useState<Recurring[]>([])
    const [transactions, setTransactions] = useState<Transaction[]>([])
    const [activeTab, setActiveTab] = useState<"overview" | "transactions" | "recurring">("overview")
    const [loading, setLoading] = useState(true)

    useEffect(() => {
        Promise.all([
            axios.get(`${API}/overview`),
            axios.get(`${API}/categories`),
            axios.get(`${API}/monthly-trends`),
            axios.get(`${API}/recurring`),
            axios.get(`${API}/transactions?limit=100`),
        ]).then(([ov, cat, tr, rec, tx]) => {
            setOverview(ov.data)
            setCategories(cat.data)
            setTrends(tr.data)
            setRecurring(rec.data)
            setTransactions(tx.data)
            setLoading(false)
        })
    }, [])

    if (loading) return (
        <div style={{
            height: "100vh", display: "flex", alignItems: "center",
            justifyContent: "center", background: "#0A0A0A", color: "#E8C547",
            fontFamily: "'DM Mono', monospace", letterSpacing: "0.1em"
        }}>
            LOADING DATA...
        </div>
    )

    return (
        <div style={{
            minHeight: "100vh",
            background: "#0A0A0A",
            color: "#CCC",
            fontFamily: "'DM Sans', sans-serif",
        }}>
            {/* Google Fonts */}
            <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500&family=DM+Mono:wght@400;500&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #111; }
        ::-webkit-scrollbar-thumb { background: #333; }
        .tab-btn { background: none; border: none; cursor: pointer; transition: all 0.2s; }
        .tab-btn:hover { color: #E8C547 !important; }
        .tx-row:hover { background: rgba(232,197,71,0.04) !important; }
      `}</style>

            {/* Header */}
            <header style={{
                borderBottom: "1px solid rgba(255,255,255,0.06)",
                padding: "24px 48px",
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                position: "sticky",
                top: 0,
                background: "#0A0A0A",
                zIndex: 10,
            }}>
                <div style={{ display: "flex", alignItems: "baseline", gap: "12px" }}>
          <span style={{
              fontFamily: "'DM Mono', monospace",
              fontSize: "18px",
              color: "#E8C547",
              letterSpacing: "0.05em"
          }}>
            FINANCE.PIPELINE
          </span>
                    <span style={{ fontSize: "11px", color: "#444", letterSpacing: "0.1em" }}>
            {overview?.banks_connected} BANKS CONNECTED
          </span>
                </div>
                <div style={{ display: "flex", gap: "4px" }}>
                    {(["overview", "transactions", "recurring"] as const).map(tab => (
                        <button
                            key={tab}
                            className="tab-btn"
                            onClick={() => setActiveTab(tab)}
                            style={{
                                padding: "8px 20px",
                                fontSize: "11px",
                                letterSpacing: "0.1em",
                                textTransform: "uppercase",
                                color: activeTab === tab ? "#E8C547" : "#555",
                                borderBottom: activeTab === tab ? "1px solid #E8C547" : "1px solid transparent",
                            }}
                        >
                            {tab}
                        </button>
                    ))}
                </div>
            </header>

            <main style={{ padding: "48px", maxWidth: "1400px", margin: "0 auto" }}>

                {/* OVERVIEW TAB */}
                {activeTab === "overview" && (
                    <div style={{ display: "flex", flexDirection: "column", gap: "32px" }}>

                        {/* Stat cards */}
                        <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "16px" }}>
                            <StatCard label="Total Spent" value={fmt(overview!.total_spent)} />
                            <StatCard label="Transactions" value={String(overview!.total_transactions)} />
                            <StatCard label="Top Category" value={overview!.top_category} />
                            <StatCard label="Monthly Commitments" value={fmt(overview!.recurring_total)} sub="recurring payments" />
                        </div>

                        {/* Charts row */}
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "16px" }}>

                            {/* Monthly trend bar chart */}
                            <div style={{
                                background: "rgba(255,255,255,0.02)",
                                border: "1px solid rgba(255,255,255,0.06)",
                                borderRadius: "2px",
                                padding: "32px",
                            }}>
                                <div style={{
                                    fontSize: "11px", letterSpacing: "0.12em",
                                    color: "#666", textTransform: "uppercase", marginBottom: "24px"
                                }}>
                                    Monthly Spending
                                </div>
                                <ResponsiveContainer width="100%" height={240}>
                                    <BarChart data={trends} barSize={32}>
                                        <XAxis
                                            dataKey="month_label"
                                            tick={{ fill: "#555", fontSize: 11 }}
                                            axisLine={false}
                                            tickLine={false}
                                        />
                                        <YAxis
                                            tick={{ fill: "#555", fontSize: 11 }}
                                            axisLine={false}
                                            tickLine={false}
                                            tickFormatter={v => `£${v}`}
                                        />
                                        <Tooltip
                                            contentStyle={{
                                                background: "#111", border: "1px solid #333",
                                                borderRadius: "2px", fontSize: "12px"
                                            }}
                                            formatter={(v: number | undefined) => [fmt(v ?? 0), "Spent"]}
                                        />
                                        <Bar dataKey="total_spent" fill="#E8C547" radius={[2, 2, 0, 0]} />
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>

                            {/* Category pie chart */}
                            <div style={{
                                background: "rgba(255,255,255,0.02)",
                                border: "1px solid rgba(255,255,255,0.06)",
                                borderRadius: "2px",
                                padding: "32px",
                            }}>
                                <div style={{
                                    fontSize: "11px", letterSpacing: "0.12em",
                                    color: "#666", textTransform: "uppercase", marginBottom: "24px"
                                }}>
                                    Spending by Category
                                </div>
                                <ResponsiveContainer width="100%" height={240}>
                                    <PieChart>
                                        <Pie
                                            data={categories}
                                            dataKey="total_spent"
                                            nameKey="category"
                                            cx="50%"
                                            cy="50%"
                                            outerRadius={90}
                                            innerRadius={50}
                                        >
                                            {categories.map((_, i) => (
                                                <Cell key={i} fill={COLORS[i % COLORS.length]} />
                                            ))}
                                        </Pie>
                                        <Tooltip
                                            contentStyle={{
                                                background: "#111", border: "1px solid #333",
                                                borderRadius: "2px", fontSize: "12px"
                                            }}
                                            formatter={(v: number | undefined) => [fmt(v ?? 0), "Spent"]}
                                        />
                                        <Legend
                                            formatter={(v) => (
                                                <span style={{ color: "#888", fontSize: "11px" }}>{v}</span>
                                            )}
                                        />
                                    </PieChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        {/* Category breakdown table */}
                        <div style={{
                            background: "rgba(255,255,255,0.02)",
                            border: "1px solid rgba(255,255,255,0.06)",
                            borderRadius: "2px",
                            padding: "32px",
                        }}>
                            <div style={{
                                fontSize: "11px", letterSpacing: "0.12em",
                                color: "#666", textTransform: "uppercase", marginBottom: "24px"
                            }}>
                                Category Breakdown
                            </div>
                            <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                                {categories.map((cat, i) => (
                                    <div key={cat.category} style={{ display: "flex", alignItems: "center", gap: "16px" }}>
                    <span style={{ width: "24px", fontSize: "16px" }}>
                      {CATEGORY_ICONS[cat.category] || "📦"}
                    </span>
                                        <span style={{ width: "140px", fontSize: "13px", color: "#AAA" }}>
                      {cat.category}
                    </span>
                                        <div style={{ flex: 1, height: "4px", background: "#1A1A1A", borderRadius: "2px" }}>
                                            <div style={{
                                                width: `${cat.percentage}%`,
                                                height: "100%",
                                                background: COLORS[i % COLORS.length],
                                                borderRadius: "2px",
                                                transition: "width 0.6s ease",
                                            }} />
                                        </div>
                                        <span style={{
                                            width: "60px", textAlign: "right",
                                            fontSize: "11px", color: "#666"
                                        }}>
                      {cat.percentage}%
                    </span>
                                        <span style={{
                                            width: "100px", textAlign: "right",
                                            fontFamily: "'DM Mono', monospace",
                                            fontSize: "13px", color: "#E8C547"
                                        }}>
                      {fmt(cat.total_spent)}
                    </span>
                                        <span style={{ width: "80px", textAlign: "right", fontSize: "11px", color: "#555" }}>
                      {cat.transaction_count} txns
                    </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                )}

                {/* TRANSACTIONS TAB */}
                {activeTab === "transactions" && (
                    <div style={{
                        background: "rgba(255,255,255,0.02)",
                        border: "1px solid rgba(255,255,255,0.06)",
                        borderRadius: "2px",
                        padding: "32px",
                    }}>
                        <div style={{
                            fontSize: "11px", letterSpacing: "0.12em",
                            color: "#666", textTransform: "uppercase", marginBottom: "24px"
                        }}>
                            All Transactions — {transactions.length} records
                        </div>
                        <div style={{
                            display: "grid",
                            gridTemplateColumns: "120px 1fr 120px 140px 100px 80px",
                            gap: "0",
                            borderBottom: "1px solid rgba(255,255,255,0.06)",
                            paddingBottom: "12px",
                            marginBottom: "8px",
                        }}>
                            {["Date", "Description", "Bank", "Category", "Amount", ""].map(h => (
                                <span key={h} style={{
                                    fontSize: "10px", letterSpacing: "0.1em",
                                    color: "#444", textTransform: "uppercase"
                                }}>{h}</span>
                            ))}
                        </div>
                        {transactions.map((tx, i) => (
                            <div
                                key={i}
                                className="tx-row"
                                style={{
                                    display: "grid",
                                    gridTemplateColumns: "120px 1fr 120px 140px 100px 80px",
                                    padding: "12px 0",
                                    borderBottom: "1px solid rgba(255,255,255,0.03)",
                                    alignItems: "center",
                                    cursor: "default",
                                }}
                            >
                <span style={{ fontSize: "12px", color: "#555", fontFamily: "'DM Mono', monospace" }}>
                  {tx.transaction_date}
                </span>
                                <span style={{ fontSize: "13px", color: "#AAA" }}>
                  {tx.description}
                </span>
                                <span style={{ fontSize: "11px", color: "#555", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                  {tx.bank_name}
                </span>
                                <span style={{
                                    fontSize: "11px",
                                    color: "#666",
                                    display: "flex",
                                    alignItems: "center",
                                    gap: "6px"
                                }}>
                  {CATEGORY_ICONS[tx.category] || "📦"} {tx.category}
                </span>
                                <span style={{
                                    fontFamily: "'DM Mono', monospace",
                                    fontSize: "13px",
                                    color: tx.is_debit ? "#FF6B6B" : "#6BCB77",
                                    textAlign: "right",
                                }}>
                  {tx.is_debit ? "-" : "+"}{fmt(Math.abs(tx.amount))}
                </span>
                                <span style={{ textAlign: "right" }}>
                  {tx.is_recurring && (
                      <span style={{
                          fontSize: "9px", letterSpacing: "0.08em",
                          color: "#E8C547", border: "1px solid #E8C547",
                          borderRadius: "2px", padding: "2px 6px"
                      }}>
                      RECUR
                    </span>
                  )}
                </span>
                            </div>
                        ))}
                    </div>
                )}

                {/* RECURRING TAB */}
                {activeTab === "recurring" && (
                    <div style={{
                        background: "rgba(255,255,255,0.02)",
                        border: "1px solid rgba(255,255,255,0.06)",
                        borderRadius: "2px",
                        padding: "32px",
                    }}>
                        <div style={{
                            fontSize: "11px", letterSpacing: "0.12em",
                            color: "#666", textTransform: "uppercase", marginBottom: "8px"
                        }}>
                            Recurring Payments
                        </div>
                        <div style={{ fontSize: "13px", color: "#444", marginBottom: "32px" }}>
                            Monthly commitment: <span style={{ color: "#E8C547", fontFamily: "'DM Mono', monospace" }}>
                {fmt(overview!.recurring_total)}
              </span>
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                            {recurring.map((r, i) => (
                                <div key={i} style={{
                                    display: "flex",
                                    alignItems: "center",
                                    justifyContent: "space-between",
                                    padding: "20px 24px",
                                    background: "rgba(255,255,255,0.02)",
                                    border: "1px solid rgba(255,255,255,0.05)",
                                    borderRadius: "2px",
                                }}>
                                    <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                                        <span style={{ fontSize: "14px", color: "#BBB" }}>{r.description}</span>
                                        <span style={{ fontSize: "11px", color: "#444", textTransform: "uppercase", letterSpacing: "0.08em" }}>
                      {r.bank_name} · {r.frequency} · last seen {r.last_seen}
                    </span>
                                    </div>
                                    <span style={{
                                        fontFamily: "'DM Mono', monospace",
                                        fontSize: "20px",
                                        color: "#E8C547",
                                    }}>
                    {fmt(r.avg_amount)}
                  </span>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </main>
        </div>
    )
}