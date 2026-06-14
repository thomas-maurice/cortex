import { marked } from 'marked'
import DOMPurify from 'dompurify'

// renderMarkdown turns a memory's markdown text into sanitized HTML. Memory text
// can originate from untrusted sources (MCP clients, imported conversations), so
// the marked output is always run through DOMPurify before it reaches v-html.
export function renderMarkdown(text) {
  const raw = marked.parse(text ?? '', { breaks: true, gfm: true })
  return DOMPurify.sanitize(raw)
}
