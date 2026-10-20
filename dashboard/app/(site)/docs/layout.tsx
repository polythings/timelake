import { DocsSidebar } from "@/components/docs/docs-sidebar"
import { OnThisPage } from "@/components/docs/on-this-page"
import { DocsPager } from "@/components/docs/pager"

export default function DocsLayout({ children }: LayoutProps<"/docs">) {
  return (
    <div className="mx-auto flex max-w-6xl gap-10 px-4 py-10">
      <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-52 shrink-0 overflow-y-auto md:block">
        <DocsSidebar />
      </aside>
      <div className="max-w-3xl min-w-0 flex-1">
        <article className="prose-docs min-w-0">
          {children}
          <DocsPager />
        </article>
      </div>
      <aside className="sticky top-20 hidden h-[calc(100svh-6rem)] w-44 shrink-0 overflow-y-auto xl:block">
        <OnThisPage />
      </aside>
    </div>
  )
}
