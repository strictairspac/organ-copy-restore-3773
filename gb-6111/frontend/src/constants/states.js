export const States = Object.freeze({
  queued: "等待执行", running: "处理中", done: "已完成", failed: "执行失败",
});

export const Actions = Object.freeze({
  create_case: "登记资料", enqueue: "提交执行", claim: "领取批次", resume: "中断续跑",
  takeover: "接手过期批次", heartbeat: "续租经办期限",
  done: "执行完成", failed: "执行失败", review: "复核签署",
});

export const Active = Object.freeze(["queued", "running"]);

export function isActive(status) {
  return Active.includes(status);
}
