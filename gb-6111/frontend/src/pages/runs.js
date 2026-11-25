import { api } from "../api/workbench.js";
import { isActive } from "../constants/states.js";
import { empty, pageHeading, section } from "../components/common/panel.js";
import { status } from "../components/common/status.js";
import { table } from "../components/common/table.js";
import { poll } from "../hooks/poll.js";
import { el } from "../utils/dom.js";
import { time } from "../utils/format.js";

function ownerName(run) {
  return run.owner || run.handler || run.assignee || run.claimed_by || run.worker_id || run.executor || "";
}

function ownerLabel(run) {
  const name = ownerName(run);
  if (name) return name;
  return run.status === "queued" ? "等待领取" : "—";
}

function ownerLine(run) {
  const name = ownerName(run);
  if (!name) {
    return el("p", {class: "owner-line", "data-testid": "run-owner"},
      "当前经办人：", el("strong", {}, run.status === "queued" ? "等待领取" : "未知"));
  }
  const details = [];
  if (run.lease_active && run.lease_until) details.push(`经办期限至 ${time(run.lease_until)}`);
  if (run.lease_expired) details.push("经办期限已过，可由别人接手");
  if (run.takeovers > 0) details.push(`已接手 ${run.takeovers} 次`);
  return el("p", {class: "owner-line", "data-testid": "run-owner", "data-owner": name},
    "当前经办人：", el("strong", {}, name),
    details.length ? el("span", {class: "owner-meta"}, `（${details.join("；")}）`) : null);
}

function reportState(run) {
  const active = isActive(run.status);
  const note = run.status === "queued"
    ? "后台正在排队，页面会自动更新。"
    : run.status === "running"
      ? "复原处理中，有新进展会延长经办期限；停滞到期后可由其他执行者接手。"
      : (run.failure || run.result?.summary);
  return el("div", {class: "stack", id: "run-state"}, status(run.status), ownerLine(run),
    el("p", {}, note),
    run.status === "done" ? el("a", {class: "button", href: `#evidence/${run.id}`}, "查看业务证据") : null);
}

export async function runsPage(config, route, signal) {
  const items = await api.runs();
  const latest = route.id ? await api.run(route.id, signal) : items[0];
  const detail = latest ? reportState(latest) : empty("进入资料页，选用一个业务样本并提交执行。");
  async function refresh() {
    try {
      const next = await api.run(latest.id, signal);
      if (!signal.aborted) {
        detail.replaceChildren(...reportState(next).childNodes);
        const badge = document.querySelector(`[data-run="${next.id}"] .status`);
        if (badge) badge.replaceWith(status(next.status));
        const ownerCell = document.querySelector(`[data-run-owner="${next.id}"]`);
        if (ownerCell) ownerCell.textContent = ownerLabel(next);
      }
      return isActive(next.status);
    } catch (error) {
      if (!signal.aborted) detail.append(el("p", {role: "alert"}, error.message));
      return false;
    }
  }
  if (latest && isActive(latest.status)) poll(refresh, signal);
  return el("div", {class: "page"},
    pageHeading(config.modules[2], config.parameter_label),
    latest ? section(latest.case_title, detail) : detail,
    section("执行批次", items.length ? table(["资料", "编号", "参数", "状态", "经办人", "提交时间"],
      items.map(run => [el("a", {"data-run": run.id, href: `#runs/${run.id}`}, run.case_title), run.id,
        run.parameter, el("span", {"data-run": run.id}, status(run.status)),
        el("span", {"data-run-owner": run.id, "data-testid": `owner-${run.id}`}, ownerLabel(run)),
        time(run.created_at)]), "执行批次") :
      empty("暂无执行记录。")));
}
