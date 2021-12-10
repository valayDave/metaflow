<!-- Renders any type of component, including Page and Section -->
<script lang="ts">
import { SvelteComponent } from "svelte";

  import type * as types from "../types";
  import { getComponent } from "./componentTypes";
  import Page from "./page.svelte";
  import Section from "./section.svelte";

  import Title from "./title.svelte";

  export let componentData: types.CardComponent;

  // Get a lower-level component or a Page or Section
  let component: typeof SvelteComponent;
  switch(componentData?.type) {
    case "page":
      component = Page
      break;
    case "section":
      component = Section
      break;
    default:
    component = getComponent(componentData?.type);
  }
</script>

{#if component}
  <svelte:component this={component || Title} {componentData} />
{/if}
