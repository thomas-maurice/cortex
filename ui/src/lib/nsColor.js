// Deterministic per-namespace colors, readable in both themes. The hue comes
// from a string hash so a namespace keeps its color across sessions and views;
// saturation/lightness are fixed per theme for contrast against the background.

function hashString(s) {
  let h = 0
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0
  return h
}

export function nsHue(namespace) {
  return hashString(namespace || '') % 360
}

// vis-network node color object for a namespace.
export function nsNodeColors(namespace, dark) {
  const h = nsHue(namespace)
  return dark
    ? {
        background: `hsl(${h} 55% 32%)`,
        border: `hsl(${h} 70% 58%)`,
        highlight: { background: `hsl(${h} 55% 40%)`, border: `hsl(${h} 80% 68%)` },
      }
    : {
        background: `hsl(${h} 80% 87%)`,
        border: `hsl(${h} 60% 45%)`,
        highlight: { background: `hsl(${h} 80% 78%)`, border: `hsl(${h} 70% 38%)` },
      }
}

// Inline style for a legend chip / badge matching the node color.
export function nsChipStyle(namespace, dark) {
  const h = nsHue(namespace)
  return dark
    ? { backgroundColor: `hsl(${h} 55% 24%)`, color: `hsl(${h} 70% 78%)`, borderColor: `hsl(${h} 70% 45%)` }
    : { backgroundColor: `hsl(${h} 80% 92%)`, color: `hsl(${h} 65% 30%)`, borderColor: `hsl(${h} 60% 60%)` }
}
