const [baseline, twoOwners, budgetReset, timerPolling] = Bun.argv.slice(2);
if (![baseline, twoOwners, budgetReset, timerPolling].every(Boolean)) {
  throw new Error("Usage: bun functional-witness.ts BASELINE TWO_OWNERS BUDGET_RESET TIMER_POLLING");
}
async function invoke(binary: string, mode: string): Promise<{ code: number; output: string }> {
  const child = Bun.spawn([binary, mode, "owned tab input: wake and account"], {
    stdout: "pipe", stderr: "pipe",
  });
  let timedOut = false;
  const deadline = setTimeout(() => { timedOut = true; child.kill(); }, 15_000);
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
  ]);
  clearTimeout(deadline);
  if (timedOut) throw new Error(`${mode}: functional entry point did not complete`);
  return { code, output: stdout + stderr };
}
for (const mode of ["exclusive", "migration", "timer", "readiness"]) {
  const result = await invoke(baseline, mode);
  if (result.code !== 0 || !result.output.includes(`PASS ${mode}:`)) {
    throw new Error(`Baseline ${mode} failed: ${result.output}`);
  }
  console.log(result.output.trim());
}
for (const [binary, mode, name] of [
  [twoOwners, "exclusive", "ExclusiveOwnerDuplicateAdmission"],
  [budgetReset, "migration", "BudgetResetByMigration"],
  [timerPolling, "timer", "TimerFiresByPolling"],
]) {
  const result = await invoke(binary, mode);
  if (result.code === 0 || !result.output.includes(`FAIL ${name}`)) {
    throw new Error(`${name}: planted production defect was not detected: ${result.output}`);
  }
  console.log(`PASS planted defect detected: ${name}`);
}
