<script lang="ts">
  // we are importing prism locally because its custom compiled.
  import "./prism";
  import "./global.css";
  import "./prism.css";
  import { cardData, modal } from "./store";
  import * as utils from "./utils";
  import Aside from "./components/aside.svelte";
  import ComponentRenderer from "./components/card-component-renderer.svelte";
  import Main from "./components/main.svelte";
  import Modal from "./components/modal.svelte";
  import Nav from "./components/aside-nav.svelte";
  import type * as types from "../types";

  let components: types.ComponentData[];
  $: components = ($cardData?.components || []) as types.ComponentData[];
</script>

<div class="container">
  <Aside>
    <Nav pageHierarchy={utils.getPageHierarchy($cardData?.components)} />
  </Aside>

  <Main>
    {#each components as componentData}
      <ComponentRenderer {componentData} />
    {/each}
  </Main>
</div>

{#if $modal}
  <Modal componentData={$modal} />
{/if}

<style>
  .container {
    width: 100%;
    display: flex;
    flex-direction: column;
    position: relative;
  }
</style>
