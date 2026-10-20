export { cn } from "cn"

/** A title as a fragment id: "TSBS IoT · After maintenance" → "tsbs-iot-after-maintenance". */
export const slugify = (title: string) =>
  title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
