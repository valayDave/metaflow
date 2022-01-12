// load app
import App from "./App.svelte";

let app;

// wrapping in try/catch to let user know if its missing #app
try {
  const rando: string = (window as any).script_rando as string;
  const containerId: string = (window as any).containerId as string;
  const containedApp = document.querySelector(`[data-container="${containerId}"]`)?.querySelector(".card_app") as Element

  app = new App({
    target: containedApp ?? document.querySelector(".card_app") as Element,
    props: {rando},
  });
} catch (err: any) {
  throw new Error(err);
}

export default app;
