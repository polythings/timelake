// Markdown compiled by the MDX loader: the specs (spec/*.md, imported through `@spec/*`) and the changelog.
declare module "*.md" {
  import type { MDXContent } from "mdx/types"

  const Content: MDXContent
  export default Content
}
