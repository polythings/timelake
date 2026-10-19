// Spec pages render spec/*.md at /spec/<file>. Links between specs stay on the site, links to the rest of the
// repository go to GitHub, and the page header shows the title and status, so the file's own are left out.
import { visit } from "unist-util-visit"

const REPO_BLOB = "https://github.com/polythings/timelake/blob/main/"

export default function remarkSpec() {
  return (tree, file) => {
    const path = (file.path ?? "").replaceAll("\\", "/")
    if (!/\/spec\/[^/]+\.md$/.test(path)) return
    const title = tree.children.findIndex((n) => n.type === "heading" && n.depth === 1)
    if (title >= 0) {
      // The title, and the status line under it (`**Done** · updated …`).
      const next = tree.children[title + 1]
      const statusLine = next?.type === "paragraph" && next.children[0]?.type === "strong"
      tree.children.splice(title, statusLine ? 2 : 1)
    }
    visit(tree, "link", (node) => {
      const url = node.url
      if (/^(https?:|mailto:|#|\/)/.test(url)) return
      if (url.startsWith("../")) {
        node.url = REPO_BLOB + url.slice(3)
        return
      }
      const spec = url.match(/^([\w-]+)\.md(#.*)?$/)
      if (spec) node.url = (spec[1] === "README" ? "/spec" : `/spec/${spec[1]}`) + (spec[2] ?? "")
    })
  }
}
