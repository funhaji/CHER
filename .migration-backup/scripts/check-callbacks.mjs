import fs from "node:fs";
import path from "node:path";

const botPath = path.resolve("lib/bot.ts");
const src = fs.readFileSync(botPath, "utf8");

const callbackLiterals = new Set();
for (const m of src.matchAll(/callback_data:\s*["']([^"'$\\]+)["']/g)) {
  const v = m[1].trim();
  if (v) callbackLiterals.add(v);
}

const handlerExact = new Set();
for (const m of src.matchAll(/data\s*===\s*["']([^"']+)["']/g)) {
  handlerExact.add(m[1]);
}

const handlerStarts = new Set();
for (const m of src.matchAll(/data\.startsWith\(\s*["']([^"']+)["']\s*\)/g)) {
  handlerStarts.add(m[1]);
}

const handlerRegex = [];
for (const m of src.matchAll(/if\s*\(\s*\/([^/]+)\/\.test\(data\)\s*\)/g)) {
  handlerRegex.push(new RegExp(m[1]));
}

const uncovered = [];
for (const cb of callbackLiterals) {
  if (handlerExact.has(cb)) continue;
  if ([...handlerStarts].some((p) => cb.startsWith(p))) continue;
  if (handlerRegex.some((r) => r.test(cb))) continue;
  uncovered.push(cb);
}

uncovered.sort();
process.stdout.write(`${uncovered.length} callback_data literals not obviously handled:\n`);
for (const v of uncovered) process.stdout.write(`- ${v}\n`);

