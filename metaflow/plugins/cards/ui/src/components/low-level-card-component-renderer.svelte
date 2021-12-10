<!-- Lower level component renderer. Does not render Page or Section to avoid circular dependencies -->
<script lang="ts">
  import type * as types from "../types";
  import { getComponent } from "./componentTypes";

  import Title from "./title.svelte";

  export let componentData: types.CardComponent;

  let component = getComponent(componentData?.type);
</script>

{#if component}
  <svelte:component
    this={getComponent(componentData?.type) || Title}
    {componentData}
  />
{/if}
