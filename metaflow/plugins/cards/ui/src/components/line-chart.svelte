<script lang="ts">
  import type * as types from "../types";
  import {
    CategoryScale,
    Chart,
    LinearScale,
    LineController,
    LineElement,
    PointElement,
  } from "chart.js";
  import type { ChartConfiguration } from "chart.js";
  import { COLORS_LIST } from "../constants";
  import { onMount } from "svelte";

  Chart.register(
    LineElement,
    LinearScale,
    LineController,
    CategoryScale,
    PointElement,
  );

  export let componentData: types.LineChartComponent;
  let el: HTMLCanvasElement;
  let chart: Chart;

  const createChart = () => {
    const { config, data, labels } = componentData;
    const chartConfiguration: ChartConfiguration = config || {
      type: "line",
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
  };

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

<div data-component="line-chart">
  <canvas bind:this={el} />
</div>

<style></style>
