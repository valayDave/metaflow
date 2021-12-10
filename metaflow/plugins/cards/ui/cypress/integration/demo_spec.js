/**
 * This is a sanity check to make sure the dev renders components.
 * Ideally, we should be doing component testing instead, however,
 * it appears that has a little way to go in development.
 *
 * For now, we're loading the dev page with the example card output, and
 * checking each component renders how we'd expect.
 */
describe("Provides a sanity check on the demo page", () => {
  before(() => {
    cy.visit("/");
  });

  /* ---------------------------------- title --------------------------------- */

  it("places the path", () => {
    cy.get("h2").contains("DefaultCardFlow/1635187021511332/join_static/1");
  });

  /* ----------------------------- navigation tree ---------------------------- */

  it("loads the navigation tree", () => {
    cy.get(".navList")
      .children()
      // nested nav list should have children
      .should("have.length", 2)
      .each((el) => {
        cy.get(el)
          // should have li children
          .find("ul li")
          .should("have.length.above", 0)
          .each((el) => {
            // each child should have a button inside it
            cy.get(el).find("button").should("have.length", 1);
          });
      });
  });

  /* ----------------------------- vertical table ----------------------------- */

  it("loads the vertical-table component", () => {
    cy.get('[data-component="table-vertical"]')
      .find("tr")
      .should("have.length", 6);
  });

  /* -------------------------------- artifacts ------------------------------- */

  it("loads the artifacts component", () => {
    cy.get('[data-component="artifacts"]')
      .find("table tr")
      .should("have.length", 23);
  });

  /* --------------------------------- images --------------------------------- */

  it("loads the images component", () => {
    cy.find('[data-component="image"]');
  });
});
