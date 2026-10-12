// Monospace code with light SQL highlighting and a copy button.
import { useState } from "react";
import { highlightSql } from "../lib/goldSql";

export function CopyButton({ text, label = "Copy" }: { text: string; label?: string }) {
  const [done, setDone] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setDone(true);
      window.setTimeout(() => setDone(false), 1500);
    } catch {
      setDone(false);
    }
  };
  return <button type="button" className="copy-button" onClick={copy} aria-live="polite">{done ? "Copied" : label}</button>;
}

export function CodeBlock({ code, sql, label }: { code: string; sql?: boolean; label: string }) {
  return (
    <div className="code-block">
      <div className="code-head"><span className="subtle">{label}</span><CopyButton text={code} label={`Copy`} /></div>
      <pre tabIndex={0} aria-label={label}><code>
        {sql ? highlightSql(code).map((p, i) => p.k ? <span key={i} className={`tok-${p.k}`}>{p.t}</span> : p.t) : code}
      </code></pre>
    </div>
  );
}
