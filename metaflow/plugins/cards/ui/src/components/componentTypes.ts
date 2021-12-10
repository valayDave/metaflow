import type { SvelteComponentDev } from "svelte/internal";
import Artifacts from "./artifacts.svelte";
import BarChart from "./bar-chart.svelte";
import Dag from "./dag/dag.svelte";
import Heading from "./heading.svelte";
import Image from "./image.svelte";
import LineChart from "./line-chart.svelte";
import Log from "./log.svelte";
import Subtitle from "./subtitle.svelte";
import Table from "./table.svelte";
import Text from "./text.svelte";
import Title from "./title.svelte";

// Mapping between type string in the JSON to svelte component
export const typesMap: Record<string, typeof SvelteComponentDev> = {
  artifacts: Artifacts,
  barChart: BarChart,
  dag: Dag,
  heading: Heading,
  image: Image,
  lineChart: LineChart,
  log: Log,
  subtitle: Subtitle,
  table: Table,
  text: Text,
  title: Title,
};

export const getComponent = (type: string): typeof SvelteComponentDev => {
  return typesMap[type];
};
