// lib/ai-report.ts
import { generateText } from "ai";
import type { FlaggedSupplier } from "./risk-engine";

export async function generateRiskReport(flaggedSuppliers: FlaggedSupplier[]) {
  if (flaggedSuppliers.length === 0) {
    return "No suppliers were flagged this period. All metrics are within normal ranges.";
  }

  // 控制 prompt 大小，避免一次塞太多
  const capped = flaggedSuppliers.slice(0, 20);

  const supplierSummaries = capped
    .map(
      (s) => `
Supplier: ${s.supplier_name} (${s.supplier_key})
Receivable: $${Number(s.prev_receivable).toLocaleString()} → $${Number(s.today_receivable).toLocaleString()} (${s.receivable_change_pct ?? "N/A"}% change) ${s.receivable_flagged ? "⚠️" : ""}
Potential Liability: $${Number(s.prev_liability).toLocaleString()} → $${Number(s.today_liability).toLocaleString()} (${s.liability_change_pct ?? "N/A"}% change) ${s.liability_flagged ? "⚠️" : ""}
Net Earning: $${Number(s.today_net_earning).toLocaleString()}
Chargeback: $${Number(s.today_chargeback).toLocaleString()}
Available Balance: $${Number(s.today_available_balance).toLocaleString()}
Outstanding Balance: $${Number(s.today_outstanding_bal).toLocaleString()}
Flag Reasons: ${s.flag_reasons.join("; ")}
`.trim()
    )
    .join("\n\n---\n\n");

  try {
    const { text } = await generateText({
      // ✅ AI Gateway：用 "provider/model" 字符串即可
      // 你可以先用 Anthropic 或 OpenAI 任意一个验证通路
      model: "anthropic/claude-sonnet-4.5",
      // model: "openai/gpt-5",

      system:
        "You are a financial risk analyst at Payability. Write a concise risk report for the risk team. " +
        "For each supplier: (1) one-sentence risk summary (2) what triggered it (3) concerning vs explainable (4) action: Monitor/Review/Escalate. " +
        "End with an overall portfolio summary. Keep tone professional and direct. Use dollar amounts and percentages.",
      prompt: `Generate a risk report for the following ${capped.length} flagged suppliers.\n\n${supplierSummaries}`,
    });

    return text;
  } catch (error: any) {
    console.error("AI Gateway LLM call failed, using fallback:", error?.message ?? error);

    return `AI service unavailable. Fallback rule-based summary:

Flagged suppliers: ${capped.length}

Manual review recommended.`;
  }
}