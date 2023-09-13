import * as types from "./types";
import { writable } from "svelte/store";
import type { Writable } from "svelte/store";

export const cardData: Writable<types.CardResponse | undefined> =
  writable(undefined);



const searchComponentChange = (
    components: types.CardComponent[], 
    id: string, 
    callback: (component: types.CardComponent) => types.CardComponent
  ) : types.CardComponent[] =>  {
  // what does the callback do ?
  // The callback allows changing the component in place
  const componentIndex = components.findIndex((fcomp: types.CardComponent) => {
    return id == fcomp.id;
  });
  
  if (componentIndex != -1) {
    console.log("Component found", id, componentIndex, components[componentIndex])
    const newComponent = callback(components[componentIndex]);
    components[componentIndex] = newComponent;
    console.log("new component")
    console.log(components[componentIndex])
    return components
  }
  for (const component of components) { 
    if (component.type === "section" || component.type === "page") {
      const newContents = searchComponentChange(component.contents || [], id, callback);
      if (newContents !== component.contents) {
        component.contents = newContents;
      }
    }
  }
  return components;
}


export const metaflow_card_update: (data:Record<string, types.CardComponent>) => boolean = (data) =>{
  const updateData = data
  console.log("data update called")
  console.log(data)
  cardData.update((value:types.CardResponse)=>{
    console.log("Old value")
    console.log(value)
    for (const componentId in updateData) {
      value.components = searchComponentChange(value.components || [], componentId, (_) => {
        return updateData[componentId];
      })
    }
    return value
  })
  return true
}
declare global {
  interface Window { metaflow_card_update: any; }
}

window.metaflow_card_update = metaflow_card_update || null

// Fetch the data from the window, or fallback to the example data file
export const setCardData: (cardDataId: string) => void = (cardDataId) => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    const data = JSON.parse(atob((window as any).__MF_DATA__[cardDataId])) as types.CardResponse;
    cardData.set(data);
  } catch (error) {
    // for now, we are loading an example card if there is no string
    fetch("/card-example.json")
      .then((resp) => resp.json())
      .then((data: types.CardResponse) => {
        cardData.set(data);
      })
      .catch(console.error);
  }

};

export const modal: Writable<types.CardComponent | undefined> =
  writable(undefined);
