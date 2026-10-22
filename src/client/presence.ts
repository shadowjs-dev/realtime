import type { Member } from "../protocol";

/**
 * Applies one presence frame to the member table, ordered by member version so a repeated or reordered event changes
 * nothing. Returns whether the table changed.
 * @internal
 */
export function applyPresence(
  table: Map<string, Member>,
  op: "sync" | "join" | "set" | "leave",
  payload: Member | readonly Member[],
): boolean {
  if (op === "sync") {
    table.clear();
    for (const member of payload as readonly Member[]) table.set(member.id, member);
    return true;
  }
  const member = payload as Member;
  const known = table.get(member.id);
  if (op === "leave") return known !== undefined && member.v >= known.v && table.delete(member.id);
  if (known !== undefined && member.v <= known.v) return false;
  table.set(member.id, member);
  return true;
}
