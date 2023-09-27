<!-- render a bar chart using chart.js, note, we do tree-shaking method, so register any components as needed -->
<script lang="ts">
  import type * as types from "../types";
  import {
    CategoryScale,
    Chart,
    BarElement,
    BarController,
    LinearScale,
    PointElement,
  } from "chart.js";
  import type { ChartConfiguration } from "chart.js";
  import { COLORS_LIST } from "../constants";
  import { onMount } from "svelte";

  Chart.register(
    BarElement,
    BarController,
    LinearScale,
    CategoryScale,
    PointElement,
  );

  export let componentData: types.BarChartComponent;
  $: ({ config, data, labels } = componentData);

  let el: HTMLCanvasElement;
  let chart: Chart;

  const createChart = () => {
    const chartConfiguration: ChartConfiguration = config || {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            backgroundColor: COLORS_LIST[2],
            borderColor: COLORS_LIST[2],
            data: data || [],
          },
        ],
      },
      options: {
        plugins: {
          legend: {
            display: false,
          },
        },
      },
    };
    chart = new Chart(el, chartConfiguration);
  }


  onMount(createChart);

  $: {
    if (chart) {
      const { data, labels,} = componentData;
      chart.data.labels = labels;
      chart.data.datasets[0].data = data || [];
      chart.update();
    }
  }
</script>

<div data-component="bar-chart">
  <canvas bind:this={el} />
</div>

<style></style>
