import type { MDXComponents } from "mdx/types"
import Link from "next/link"

import { CopyButton } from "@/components/copy-button"

/** Elements in MDX pages. Styles come from `.prose-docs` (app/globals.css); internal links use next/link; code blocks get a copy button. */
const components: MDXComponents = {
  a: ({ href = "", ...props }) =>
    href.startsWith("/") ? <Link href={href} {...props} /> : <a href={href} {...props} />,
  pre: (props) => (
    <div className="group/code relative">
      <pre {...props} />
      <CopyButton />
    </div>
  ),
}

export function useMDXComponents(): MDXComponents {
  return components
}
